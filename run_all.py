#!/usr/bin/env python3
"""
Combined Data Fetch Runner (Manus Scheduled Task)

Runs every hour. On each run:
  1. Always: fetch_news.py (RSS news aggregation)
  2. Always: fetch_x_posts.py (X.com AI/tech posts)
  3. Daily (first run of the day):
     - fetch_sensortower.py (app rankings)
     - fetch_producthunt_top.py (top products)
     - fetch_github_trending.py (trending repos)
     - fetch_podcasts.py (tech podcast episodes)

The "daily" tasks are gated by checking whether the current UTC date
has already been processed (tracked via a local marker file).
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

# Marker file to track which UTC date has already run daily tasks
# Stored OUTSIDE the repo directory so git pull / clone won't erase it
SCRIPT_DIR = Path("/home/ubuntu/manus-data-fetch")
DAILY_MARKER = Path("/home/ubuntu/.daily_marker")

# Environment variables for API keys
SENSORTOWER_API_KEY = os.environ.get("SENSORTOWER_API_KEY", "")
PH_API_KEY = os.environ.get("PH_API_KEY", "")
PH_API_SECRET = os.environ.get("PH_API_SECRET", "")


def _today_utc():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _daily_already_ran():
    """Check if daily tasks already ran today."""
    if DAILY_MARKER.exists():
        marker_date = DAILY_MARKER.read_text().strip()
        return marker_date == _today_utc()
    return False


def _mark_daily_done():
    """Mark daily tasks as done for today."""
    DAILY_MARKER.write_text(_today_utc())


def run_news():
    """Run the news fetcher."""
    log.info("=" * 60)
    log.info("TASK 1: News Fetcher")
    log.info("=" * 60)
    try:
        # Change to script directory so imports work
        os.chdir(str(SCRIPT_DIR))
        sys.path.insert(0, str(SCRIPT_DIR))

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
        os.chdir(str(SCRIPT_DIR))
        sys.path.insert(0, str(SCRIPT_DIR))

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
        os.chdir(str(SCRIPT_DIR))
        sys.path.insert(0, str(SCRIPT_DIR))

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
        os.chdir(str(SCRIPT_DIR))
        sys.path.insert(0, str(SCRIPT_DIR))

        import fetch_github_trending
        fetch_github_trending.main()
        log.info("GitHub trending fetcher completed successfully.")
    except Exception as e:
        log.error(f"GitHub trending fetcher failed: {e}")
        traceback.print_exc()


def run_x_posts():
    """Run the X.com posts fetcher."""
    log.info("=" * 60)
    log.info("TASK 6: X.com Posts Fetcher")
    log.info("=" * 60)
    try:
        os.chdir(str(SCRIPT_DIR))
        sys.path.insert(0, str(SCRIPT_DIR))

        import fetch_x_posts
        fetch_x_posts.main()
        log.info("X.com posts fetcher completed successfully.")
    except Exception as e:
        log.error(f"X.com posts fetcher failed: {e}")
        traceback.print_exc()


def run_podcasts():
    """Run the podcast monitor."""
    log.info("=" * 60)
    log.info("TASK 5: Podcast Monitor")
    log.info("=" * 60)
    try:
        os.chdir(str(SCRIPT_DIR))
        sys.path.insert(0, str(SCRIPT_DIR))

        import fetch_podcasts
        fetch_podcasts.main()
        log.info("Podcast monitor completed successfully.")
    except Exception as e:
        log.error(f"Podcast monitor failed: {e}")
        traceback.print_exc()


def main():
    log.info("=" * 60)
    log.info(f"Combined Data Fetch Runner — {datetime.now(timezone.utc).isoformat()}")
    log.info("=" * 60)

    # Task 1: Always run news fetcher
    run_news()

    # Task 6: Always run X.com posts fetcher
    run_x_posts()

    # Tasks 2, 3, 4, 5: Run daily tasks only once per UTC day
    if _daily_already_ran():
        log.info("\nDaily tasks (Sensor Tower + Product Hunt + GitHub Trending + Podcasts) already ran today. Skipping.")
    else:
        log.info("\nRunning daily tasks (first run of the day)...")
        run_sensortower()
        run_producthunt()
        run_github_trending()
        run_podcasts()
        _mark_daily_done()
        log.info("Daily tasks completed and marked as done.")

    log.info("\n" + "=" * 60)
    log.info("All tasks finished.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
