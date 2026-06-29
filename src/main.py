"""Apify Actor entrypoint for the Reddit trend scraper.

Reads the run input, drives the Playwright-based BrowserScraper, pushes every
scraped post to the default dataset, and stores a ranked summary in the
key-value store under OUTPUT.

Production behaviour
--------------------
* Each feed browses through a freshly rotated Apify Proxy exit IP in its own
  isolated browser context, so concurrent feeds never share a session.
* Concurrency is capped to what the actor's allocated memory can safely run,
  to avoid out-of-memory crashes from too many concurrent Chromium contexts.
* Aborts/migrations are handled cooperatively so the run shuts down cleanly.
"""

from __future__ import annotations

from apify import Actor

try:  # Event enum location is stable in SDK v2, but stay resilient across versions.
    from apify import Event
except ImportError:  # pragma: no cover
    Event = None

from .scraper import BrowserScraper, ScraperConfig

# Rough memory budget per concurrent Chromium context (page + renderer), plus a
# baseline reserve for the Python process and the shared browser instance.
MB_BASELINE_RESERVE = 350
MB_PER_CONTEXT = 250


def _as_list(value) -> list[str]:
    """Accept either a list (stringList editor) or a comma-separated string."""
    if value is None:
        return []
    if isinstance(value, str):
        return [item.strip() for item in value.split(",") if item.strip()]
    return [str(item).strip() for item in value if str(item).strip()]


def _as_int(value, default: int, *, minimum: int | None = None) -> int:
    """Coerce input to int without crashing on bad data; clamp to a floor."""
    try:
        result = int(value)
    except (TypeError, ValueError):
        result = default
    if minimum is not None:
        result = max(minimum, result)
    return result


def _memory_safe_concurrency(requested: int) -> int:
    """Cap concurrency so concurrent Chromium contexts fit in the actor's RAM."""
    try:
        memory_mb = Actor.get_env().get("memory_mbytes")
    except Exception:
        memory_mb = None
    if not memory_mb:
        return requested
    affordable = max(1, (int(memory_mb) - MB_BASELINE_RESERVE) // MB_PER_CONTEXT)
    safe = min(requested, affordable)
    if safe < requested:
        Actor.log.warning(
            f"Capping concurrency {requested} -> {safe} to fit {memory_mb} MB of actor memory."
        )
    return safe


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}

        # Resolve an Apify proxy configuration for the browser, if configured.
        proxy_config = await Actor.create_proxy_configuration(
            actor_proxy_input=actor_input.get("proxyConfiguration"),
        )
        proxy_provider = None
        if proxy_config:
            async def proxy_provider() -> str:
                # A fresh URL each call rotates the Apify Proxy exit IP, so every
                # browser context (and every retry) browses from a different IP.
                return await proxy_config.new_url()

            Actor.log.info("Apify Proxy enabled with per-context IP rotation.")
        else:
            Actor.log.warning("No proxy configured — Reddit may rate-limit or block at scale.")

        concurrency = _memory_safe_concurrency(
            _as_int(actor_input.get("concurrency", 4), 4, minimum=1)
        )

        config = ScraperConfig(
            keywords=_as_list(actor_input.get("keywords")),
            subreddits=_as_list(actor_input.get("subreddits")),
            listing_sort=actor_input.get("listingSort", "hot"),
            search_sorts=_as_list(actor_input.get("searchSorts")) or ["hot", "relevance"],
            concurrency=concurrency,
            scrolls=_as_int(actor_input.get("scrolls", 2), 2, minimum=0),
            max_posts_per_feed=_as_int(actor_input.get("maxPostsPerFeed", 100), 100, minimum=1),
            min_score=_as_int(actor_input.get("minScore", 0), 0),
            max_retries=_as_int(actor_input.get("maxRetries", 2), 2, minimum=0),
            top_trends_count=_as_int(actor_input.get("topTrends", 25), 25, minimum=1),
            headless=True,
        ).resolve()

        target = config.keywords or [f"r/{s}" for s in config.subreddits]
        Actor.log.info(f"Starting Reddit scraper for: {', '.join(target) or 'default feeds'}")
        await Actor.set_status_message("Scraping Reddit feeds...")

        scraper = BrowserScraper(config, logger=Actor.log, proxy_url_provider=proxy_provider)

        # Cooperative shutdown: stop launching new work if the actor is aborted/migrated.
        def _on_shutdown(*_args) -> None:
            Actor.log.warning("Shutdown event received — finishing in-flight feeds and stopping.")
            scraper.request_stop()

        if Event is not None:
            for event_name in ("ABORTING", "MIGRATING"):
                member = getattr(Event, event_name, None)
                if member is not None:
                    try:
                        Actor.on(member, _on_shutdown)
                    except Exception:
                        # Shutdown handling stays best-effort across SDK versions.
                        pass

        report = await scraper.run()

        all_posts = report.pop("all_posts", [])

        # Push every post as a dataset item so downstream tools can consume them.
        # push_data chunks large lists internally to respect Apify's request limits.
        if all_posts:
            await Actor.push_data(all_posts)

        # Store the ranked summary (without the full post list) in OUTPUT.
        await Actor.set_value("OUTPUT", report)

        Actor.log.info(
            f"Done. Analyzed {report['total_posts_analyzed']} posts; "
            f"pushed {len(all_posts)} to the dataset."
        )
        await Actor.set_status_message(
            f"Done. {report['total_posts_analyzed']} posts scraped."
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
