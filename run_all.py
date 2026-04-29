#!/usr/bin/env python3
"""
Combined Data Fetch Runner (Manus Scheduled Task)

Runs once daily. On each run, all tasks execute sequentially:
  1. fetch_news.py          — RSS news aggregation
  2. fetch_sensortower.py   — Sensor Tower app rankings
  3. fetch_producthunt_top.py — Product Hunt top products
  4. fetch_github_trending.py — GitHub trending repos
  5. fetch_x_posts.py       — X.com AI/tech posts (25-hour lookback)
  6. fetch_podcasts.py       — Tech podcast episodes (yesterday's)
  7. fetch_openrouter_ranking.py — OpenRouter AI app rankings
"""

import os
import sys
import logging
import traceback
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("run_all")

SCRIPT_DIR = Path("/home/ubuntu/manus-data-fetch")

# Environment variables for API keys
SENSORTOWER_API_KEY = os.environ.get("SENSORTOWER_API_KEY", "")
PH_API_KEY = os.environ.get("PH_API_KEY", "")
PH_API_SECRET = os.environ.get("PH_API_SECRET", "")


def _setup():
    """Ensure script directory is on sys.path and is the cwd."""
    os.chdir(str(SCRIPT_DIR))
    if str(SCRIPT_DIR) not in sys.path:
        sys.path.insert(0, str(SCRIPT_DIR))


def run_news():
    """Run the news fetcher."""
    log.info("=" * 60)
    log.info("TASK 1: News Fetcher")
    log.info("=" * 60)
    try:
        _setup()
        import fetch_news
        fetch_news.main()
        log.info("News fetcher completed successfully.")
    except Exception as e:
        log.error(f"News fetcher failed: {e}")
        traceback.print_exc()


def run_sensortower():
    """Run the Sensor Tower fetcher."""
    log.info("=" * 60)
    log.info("TASK 2: Sensor Tower Fetcher")
    log.info("=" * 60)
    if not SENSORTOWER_API_KEY:
        log.warning("SENSORTOWER_API_KEY not set, skipping Sensor Tower fetch.")
        return
    try:
        _setup()
        import fetch_sensortower
        fetch_sensortower.main()
        log.info("Sensor Tower fetcher completed successfully.")
    except Exception as e:
        log.error(f"Sensor Tower fetcher failed: {e}")
        traceback.print_exc()


def run_producthunt():
    """Run the Product Hunt fetcher."""
    log.info("=" * 60)
    log.info("TASK 3: Product Hunt Fetcher")
    log.info("=" * 60)
    if not PH_API_KEY or not PH_API_SECRET:
        log.warning("PH_API_KEY/PH_API_SECRET not set, skipping Product Hunt fetch.")
        return
    try:
        _setup()
        import fetch_producthunt_top
        fetch_producthunt_top.main()
        log.info("Product Hunt fetcher completed successfully.")
    except Exception as e:
        log.error(f"Product Hunt fetcher failed: {e}")
        traceback.print_exc()


def run_github_trending():
    """Run the GitHub trending repos fetcher."""
    log.info("=" * 60)
    log.info("TASK 4: GitHub Trending Fetcher")
    log.info("=" * 60)
    try:
        _setup()
        import fetch_github_trending
        fetch_github_trending.main()
        log.info("GitHub trending fetcher completed successfully.")
    except Exception as e:
        log.error(f"GitHub trending fetcher failed: {e}")
        traceback.print_exc()


def run_x_posts():
    """Run the X.com posts fetcher."""
    log.info("=" * 60)
    log.info("TASK 5: X.com Posts Fetcher")
    log.info("=" * 60)
    try:
        _setup()
        import fetch_x_posts
        fetch_x_posts.main()
        log.info("X.com posts fetcher completed successfully.")
    except Exception as e:
        log.error(f"X.com posts fetcher failed: {e}")
        traceback.print_exc()


def run_podcasts():
    """Run the podcast monitor."""
    log.info("=" * 60)
    log.info("TASK 6: Podcast Monitor")
    log.info("=" * 60)
    try:
        _setup()
        import fetch_podcasts
        fetch_podcasts.main()
        log.info("Podcast monitor completed successfully.")
    except Exception as e:
        log.error(f"Podcast monitor failed: {e}")
        traceback.print_exc()


def run_openrouter_ranking():
    """Run the OpenRouter global ranking fetcher."""
    log.info("=" * 60)
    log.info("TASK 7: OpenRouter Ranking Fetcher")
    log.info("=" * 60)
    try:
        _setup()
        import fetch_openrouter_ranking
        fetch_openrouter_ranking.main()
        log.info("OpenRouter ranking fetcher completed successfully.")
    except Exception as e:
        log.error(f"OpenRouter ranking fetcher failed: {e}")
        traceback.print_exc()


def main():
    log.info("=" * 60)
    log.info(f"Combined Data Fetch Runner — {datetime.now(timezone.utc).isoformat()}")
    log.info("=" * 60)

    run_news()
    run_sensortower()
    run_producthunt()
    run_github_trending()
    run_x_posts()
    run_podcasts()
    run_openrouter_ranking()

    log.info("\n" + "=" * 60)
    log.info("All tasks finished.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
