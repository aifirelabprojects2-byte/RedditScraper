"""Core Reddit trend scraper (Playwright).

Adapted from the standalone reddit_scrapy.py into a library that the Apify
entrypoint (src/main.py) drives. Reddit blocks browser-less access on most
networks, so a real browser is required. The feed's <shreddit-post> elements
already expose every field we need, so there are no per-post detail requests.

Production notes
----------------
* Each feed runs in its own short-lived browser **context** with a freshly
  rotated proxy exit IP, so concurrent feeds never share a session/IP. This is
  what keeps the actor from getting rate-limited or soft-blocked at scale.
* Every feed is retried with backoff on transient errors and empty (likely
  soft-blocked) responses, each retry on a new IP.
* Contexts are always torn down in a ``finally`` block, so a crash in one feed
  can never leak browser resources into the others.
"""

from __future__ import annotations

import asyncio
import re
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional
from urllib.parse import quote, urlsplit

from playwright.async_api import async_playwright, Error as PlaywrightError

DEFAULT_SUBREDDITS = ["all", "technology", "news", "business", "Python"]
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
BLOCKED_RESOURCES = {"image", "stylesheet", "font", "media"}

# Async callable that returns a fresh proxy URL (with rotated exit IP) per call.
ProxyUrlProvider = Callable[[], Awaitable[Optional[str]]]

# JS run once per page: pull every post's data out of the DOM in a single call.
EXTRACT_JS = """
() => Array.from(document.querySelectorAll('shreddit-post')).map(el => {
  const body = el.querySelector('[slot="text-body"]');
  return {
    title: el.getAttribute('post-title'),
    permalink: el.getAttribute('permalink'),
    score: el.getAttribute('score'),
    comments: el.getAttribute('comment-count'),
    subreddit: el.getAttribute('subreddit-prefixed-name'),
    author: el.getAttribute('author'),
    created: el.getAttribute('created-timestamp'),
    postType: el.getAttribute('post-type'),
    domain: el.getAttribute('domain'),
    contentHref: el.getAttribute('content-href'),
    id: el.getAttribute('id'),
    body: body ? body.innerText.trim() : ''
  };
})
"""

STOP_WORDS = {
    'the', 'a', 'an', 'and', 'or', 'but', 'is', 'are', 'was', 'were', 'to', 'of', 'in',
    'for', 'on', 'with', 'at', 'by', 'from', 'up', 'about', 'into', 'over', 'after',
    'this', 'that', 'these', 'those', 'it', 'its', 'how', 'why', 'what', 'when', 'who',
    'just', 'new', 'more', 'has', 'have', 'had', 'not', 'be', 'been', 'will', 'can',
    'you', 'your', 'my', 'me', 'our', 'we', 'they', 'them', 'their', 'he', 'she', 'his',
    'her', 'whats', 'did', 'would', 'could', 'should', 'some', 'any', 'thread', 'daily',
}


@dataclass
class ScraperConfig:
    keywords: list[str] = field(default_factory=list)
    subreddits: list[str] = field(default_factory=list)
    listing_sort: str = "hot"          # hot | new | top | rising
    search_sorts: list[str] = field(default_factory=lambda: ["hot", "relevance"])
    concurrency: int = 4               # concurrent browser contexts
    scrolls: int = 2                   # extra lazy-load scrolls per feed
    scroll_pause_ms: int = 900
    settle_ms: int = 700               # let the first batch finish rendering
    max_posts_per_feed: int = 100
    nav_timeout_ms: int = 45000
    selector_timeout_ms: int = 15000
    max_retries: int = 2               # extra attempts per feed after the first
    retry_backoff_ms: int = 1500       # base backoff, grows linearly per attempt
    headless: bool = True
    min_score: int = 0
    top_keywords_count: int = 15
    top_trends_count: int = 25
    proxy_url: Optional[str] = None    # static fallback when no provider is given

    def resolve(self) -> "ScraperConfig":
        if not self.subreddits and not self.keywords:
            self.subreddits = list(DEFAULT_SUBREDDITS)
        return self


def parse_num(val: Any) -> int:
    """Parse score/comment counts; tolerates plain ints and K/M suffixes."""
    if val is None:
        return 0
    s = str(val).lower().strip().replace(',', '')
    if not s:
        return 0
    try:
        if s.endswith('k'):
            return int(float(s[:-1]) * 1_000)
        if s.endswith('m'):
            return int(float(s[:-1]) * 1_000_000)
        return int(float(s))
    except ValueError:
        return 0


def playwright_proxy(url: Optional[str]) -> Optional[dict]:
    """Convert a credentialed proxy URL into Playwright's proxy settings.

    Apify proxy URLs embed credentials (``http://user:pass@host:port``), but
    Playwright expects ``server`` without credentials and ``username`` /
    ``password`` as separate keys — otherwise authentication silently fails.
    """
    if not url:
        return None
    parts = urlsplit(url)
    if not parts.hostname:
        # Not a parseable URL (e.g. the "per-context" launch placeholder); pass through.
        return {"server": url}
    server = f"{parts.scheme or 'http'}://{parts.hostname}"
    if parts.port:
        server += f":{parts.port}"
    proxy: dict[str, str] = {"server": server}
    if parts.username:
        proxy["username"] = parts.username
    if parts.password:
        proxy["password"] = parts.password
    return proxy


def normalize_post(raw: dict) -> Optional[dict]:
    title = (raw.get('title') or '').strip()
    permalink = raw.get('permalink') or ''
    if not title or not permalink:
        return None

    author = raw.get('author') or '[deleted]'
    author = author if author.startswith('u/') else f'u/{author}'

    href = raw.get('contentHref') or ''
    media_url = None
    low = href.lower()
    if href and (any(h in href for h in ('i.redd.it', 'v.redd.it', 'preview.redd.it'))
                 or low.endswith(('.png', '.jpg', '.jpeg', '.gif', '.webp'))):
        media_url = href

    url = permalink if permalink.startswith('http') else 'https://www.reddit.com' + permalink
    url = url.split('?')[0]

    return {
        'id': raw.get('id') or url,
        'title': title,
        'subreddit': raw.get('subreddit') or 'r/general',
        'author': author,
        'created_at': raw.get('created') or '',
        'post_type': raw.get('postType') or 'link',
        'domain': raw.get('domain') or 'reddit.com',
        'score': parse_num(raw.get('score')),
        'comments': parse_num(raw.get('comments')),
        'url': url,
        'post_content': (raw.get('body') or '').strip(),
        'media_url': media_url,
    }


class BrowserScraper:
    def __init__(
        self,
        config: ScraperConfig,
        logger=None,
        proxy_url_provider: Optional[ProxyUrlProvider] = None,
    ):
        self.config = config
        self.keywords = [k.lower() for k in config.keywords]
        self._sem = asyncio.Semaphore(config.concurrency)
        # Accept any logger with info/warning/error; default to a no-op shim.
        self.log = logger or _NullLogger()
        self._proxy_provider = proxy_url_provider
        self._stopped = False

    def request_stop(self) -> None:
        """Cooperative shutdown: in-flight feeds finish, pending ones bail fast."""
        self._stopped = True

    @property
    def _use_proxy(self) -> bool:
        return self._proxy_provider is not None or bool(self.config.proxy_url)

    def _feeds(self) -> list[tuple[str, str]]:
        cfg = self.config
        feeds: list[tuple[str, str]] = []
        for sub in cfg.subreddits:
            feeds.append((f"https://www.reddit.com/r/{sub}/{cfg.listing_sort}/",
                          f"r/{sub}/{cfg.listing_sort}"))
        for kw in cfg.keywords:
            for sort in cfg.search_sorts:
                feeds.append((f"https://www.reddit.com/search/?q={quote(kw)}&sort={sort}",
                              f"search:{kw}:{sort}"))
        return feeds

    @staticmethod
    async def _block_assets(route, request) -> None:
        if request.resource_type in BLOCKED_RESOURCES:
            await route.abort()
        else:
            await route.continue_()

    async def _next_proxy(self) -> Optional[str]:
        if self._proxy_provider is not None:
            return await self._proxy_provider()
        return self.config.proxy_url

    async def _new_context(self, browser):
        """A fresh, isolated context — new session + rotated exit IP per feed."""
        proxy = playwright_proxy(await self._next_proxy()) if self._use_proxy else None
        context = await browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1366, "height": 900},
            locale="en-US",
            proxy=proxy,
        )
        await context.route("**/*", self._block_assets)
        return context

    async def _attempt_feed(self, browser, url: str, label: str) -> list[dict]:
        """One scrape attempt in a dedicated context. Raises on failure/empty."""
        cfg = self.config
        context = await self._new_context(browser)
        try:
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=cfg.nav_timeout_ms)
            try:
                await page.wait_for_selector("shreddit-post", timeout=cfg.selector_timeout_ms)
            except PlaywrightError:
                # No posts usually means a soft block — worth retrying on a new IP.
                raise _EmptyFeed(label)

            await page.wait_for_timeout(cfg.settle_ms)
            prev_count = 0
            for _ in range(cfg.scrolls):
                count = await page.eval_on_selector_all("shreddit-post", "els => els.length")
                # Stop early once we have enough, or when scrolling stops loading more.
                if count >= cfg.max_posts_per_feed or count == prev_count:
                    break
                prev_count = count
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(cfg.scroll_pause_ms)

            raw_items = await page.evaluate(EXTRACT_JS)
        finally:
            # Tearing down the context closes its pages too; never leak resources.
            await context.close()

        posts = [p for p in (normalize_post(r) for r in raw_items) if p]
        return posts[: cfg.max_posts_per_feed]

    async def _scrape_feed(self, browser, url: str, label: str) -> list[dict]:
        cfg = self.config
        async with self._sem:
            if self._stopped:
                return []
            last_err = "unknown error"
            attempts = cfg.max_retries + 1
            for attempt in range(1, attempts + 1):
                try:
                    posts = await self._attempt_feed(browser, url, label)
                    self.log.info(f"feed {label:<30} -> {len(posts):3d} posts (attempt {attempt})")
                    return posts
                except _EmptyFeed:
                    last_err = "no posts rendered (blocked or empty)"
                except PlaywrightError as exc:
                    last_err = str(exc).splitlines()[0]
                except Exception as exc:  # defensive: never let one feed kill the run
                    last_err = f"unexpected: {exc}"

                if attempt < attempts and not self._stopped:
                    backoff = cfg.retry_backoff_ms * attempt / 1000
                    self.log.warning(
                        f"feed {label:<30} -> attempt {attempt} failed ({last_err}); "
                        f"retrying on a new IP in {backoff:.1f}s"
                    )
                    await asyncio.sleep(backoff)
                else:
                    break

            self.log.error(f"feed {label:<30} -> gave up after {attempt} attempt(s): {last_err}")
            return []

    async def run(self) -> dict:
        feeds = self._feeds()
        self.log.info(
            f"Scraping {len(feeds)} feed(s) with a headless browser "
            f"(concurrency={self.config.concurrency}, retries={self.config.max_retries}, "
            f"proxy={'on' if self._use_proxy else 'off'})."
        )

        launch_args = ["--disable-blink-features=AutomationControlled"]
        # Chromium needs a browser-level proxy set for per-context proxies to take
        # effect; the placeholder is overridden by each context's real proxy.
        launch_proxy = {"server": "per-context"} if self._use_proxy else None

        results: list = []
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=self.config.headless,
                args=launch_args,
                proxy=launch_proxy,
            )
            try:
                results = await asyncio.gather(
                    *(self._scrape_feed(browser, url, label) for url, label in feeds),
                    return_exceptions=True,
                )
            finally:
                await browser.close()

        raw_posts: list[dict] = []
        for (url, label), result in zip(feeds, results):
            if isinstance(result, Exception):
                self.log.error(f"Feed {label} failed: {result}")
            else:
                raw_posts.extend(result)

        return self._build_report(raw_posts)

    def _score_and_match(self, post: dict) -> dict:
        searchable = f"{post['title']} {post['post_content']}".lower()
        matched, boost = [], 0.0
        for kw in self.keywords:
            if kw in searchable:
                matched.append(kw)
                boost += 50.0

        score, comments = post["score"], post["comments"]
        trend_score = (score * 1.5) + (comments * 3.0) + boost

        parts = [f"Trending discussion in {post['subreddit']} titled '{post['title']}'."]
        body = post["post_content"]
        if body:
            parts.append(f"Content: {body[:200]}..." if len(body) > 200 else f"Content: {body}")
        elif post["media_url"]:
            parts.append(f"Media Attachment: {post['media_url']}")
        if matched:
            parts.append(f"Matched topics: {', '.join(matched)}.")
        parts.append(f"Recorded engagement: {score:,} upvotes and {comments:,} community comments.")

        post["matched_keywords"] = matched
        post["trend_score"] = round(trend_score, 2)
        post["content_snippet"] = " ".join(parts)
        return post

    def _build_report(self, raw_posts: list[dict]) -> dict:
        unique: dict[str, dict] = {}
        for post in raw_posts:
            scored = self._score_and_match(post)
            if scored["score"] < self.config.min_score:
                continue
            key = scored["id"] or scored["url"]
            if key not in unique:
                unique[key] = scored
            else:
                prev = unique[key]
                prev["matched_keywords"] = sorted(
                    set(prev["matched_keywords"]) | set(scored["matched_keywords"]))
                if scored["trend_score"] > prev["trend_score"]:
                    scored["matched_keywords"] = prev["matched_keywords"]
                    unique[key] = scored

        sorted_posts = sorted(unique.values(), key=lambda p: p["trend_score"], reverse=True)

        words: list[str] = []
        for post in sorted_posts:
            cleaned = re.sub(r"[^\w\s]", "", post["title"].lower())
            words.extend(w for w in cleaned.split() if len(w) > 2 and w not in STOP_WORDS)
        top_keywords = [w for w, _ in Counter(words).most_common(self.config.top_keywords_count)]

        if not sorted_posts:
            self.log.warning("No posts scraped.")

        return {
            "target_keywords_queried": self.config.keywords,
            "subreddits_queried": self.config.subreddits,
            "total_posts_analyzed": len(sorted_posts),
            "top_trending_keywords": top_keywords,
            "top_viral_trends": sorted_posts[: self.config.top_trends_count],
            "all_posts": sorted_posts,
        }


class _EmptyFeed(Exception):
    """Raised when a feed rendered no posts (treated as a retryable soft block)."""


class _NullLogger:
    def info(self, *_a, **_k): ...
    def warning(self, *_a, **_k): ...
    def error(self, *_a, **_k): ...
