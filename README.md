# Reddit Trend Scraper (Apify Actor)

Scrapes Reddit subreddit feeds and keyword searches with a real browser
(Playwright + Chromium), ranks posts by a trend score, and returns them.

Reddit blocks browser-less access (plain HTTP / `.json` / TLS-impersonation) on
most networks, so a headless browser is required. The feed's `<shreddit-post>`
elements already expose title, score, comments, author, created time, post type,
domain and media href — so there are no slow per-post detail requests. Images,
CSS, fonts and media are aborted at the network layer for speed.

## Built for scale

* **Per-feed IP rotation** — each feed browses in its own isolated browser
  context with a freshly rotated Apify Proxy exit IP, so concurrent feeds never
  share a session or IP. This is what keeps the actor from being rate-limited or
  soft-blocked when scraping many feeds at once.
* **Retries with backoff** — every feed is retried (`maxRetries`) on transient
  errors and empty (likely soft-blocked) responses, each retry on a new IP.
* **Memory-aware concurrency** — concurrency is automatically capped to what the
  actor's allocated memory can run, so more Chromium contexts can't OOM the run.
* **Resilient cleanup** — contexts are always torn down, and a crash in one feed
  is isolated and can never take down the rest of the run.
* **Graceful shutdown** — abort/migration events stop new work cleanly while
  in-flight feeds finish.

## Input

All fields are optional. If both `keywords` and `subreddits` are empty, a default
set of feeds is scraped (`all`, `technology`, `news`, `business`, `Python`).

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `keywords` | string[] | `[]` | Keywords to search across Reddit. |
| `subreddits` | string[] | `[]` | Subreddit names (no `r/` prefix). |
| `listingSort` | string | `hot` | `hot` \| `new` \| `top` \| `rising`. |
| `searchSorts` | string[] | `["hot","relevance"]` | Sort orders per keyword search. |
| `concurrency` | int | `4` | Browser pages scraped at once. |
| `scrolls` | int | `2` | Extra lazy-load scrolls per feed (0 = first page). |
| `maxPostsPerFeed` | int | `100` | Cap on posts per feed. |
| `minScore` | int | `0` | Drop posts below this upvote score. |
| `maxRetries` | int | `2` | Extra attempts per feed (each on a fresh proxy IP). |
| `topTrends` | int | `25` | Top posts kept in the summary. |
| `proxyConfiguration` | object | Apify residential | Proxy used for browsing. |

Example input:

```json
{
  "keywords": ["open ai", "llm"],
  "subreddits": ["technology", "Python"],
  "listingSort": "hot",
  "scrolls": 3
}
```

## Output

- **Dataset** — one item per scraped post, each with `id`, `title`, `subreddit`,
  `author`, `created_at`, `post_type`, `domain`, `score`, `comments`, `url`,
  `post_content`, `media_url`, `matched_keywords`, `trend_score` and a
  human-readable `content_snippet`.
- **Key-value store `OUTPUT`** — a ranked summary: queried keywords/subreddits,
  `total_posts_analyzed`, `top_trending_keywords` and `top_viral_trends`.

## Run locally

```bash
pip install -r requirements.txt   # apify 3.x + pydantic
pip install playwright            # provided by the base image in production
playwright install chromium
apify run            # with the Apify CLI, reads storage/key_value_stores/default/INPUT.json
```

Or push to the platform with `apify push`.
