#!/usr/bin/env python3
"""
GitHub Trending Repositories Fetcher (Manus version)

Scrapes the GitHub Trending page for today's trending repositories and stores
them as native Google Sheets in Google Drive (Latest + Cumulative).

Fields captured:
  rank, repo_full_name, description, language, total_stars, forks,
  stars_today, url, fetched_date

Runs daily (gated by run_all.py).
"""

import logging
import re
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup

import drive_storage as ds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────

TRENDING_URL = "https://github.com/trending"
BASE_FILENAME = "github_trending"

HEADERS_ROW = [
    "rank",
    "repo_full_name",
    "description",
    "language",
    "total_stars",
    "forks",
    "stars_today",
    "url",
    "fetched_date",
]

DEDUP_KEYS = ["repo_full_name", "fetched_date"]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)


# ─── Scraper ────────────────────────────────────────────────────────────────

def _parse_number(text: str) -> int:
    """Parse a number string like '29,037' or '3,542' into an integer."""
    if not text:
        return 0
    cleaned = text.strip().replace(",", "")
    try:
        return int(cleaned)
    except ValueError:
        return 0


def fetch_trending() -> list[dict]:
    """Scrape GitHub trending page and return a list of repo dicts."""
    log.info(f"Fetching GitHub trending page: {TRENDING_URL}")

    resp = requests.get(TRENDING_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    articles = soup.select("article.Box-row")

    if not articles:
        log.warning("No trending repos found on page. The HTML structure may have changed.")
        return []

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    rows = []

    for rank, article in enumerate(articles, start=1):
        # Repo name (e.g., "666ghj / MiroFish")
        h2 = article.select_one("h2 a")
        if not h2:
            continue
        repo_full_name = h2.get_text(strip=True).replace(" ", "").replace("\n", "")
        url = f"https://github.com/{repo_full_name}"

        # Description
        p = article.select_one("p")
        description = p.get_text(strip=True) if p else ""

        # Language
        lang_span = article.select_one("span[itemprop='programmingLanguage']")
        language = lang_span.get_text(strip=True) if lang_span else ""

        # Stars and forks — look for links with star/fork icons
        star_link = article.select_one("a[href$='/stargazers']")
        fork_link = article.select_one("a[href$='/forks']")
        total_stars = _parse_number(star_link.get_text(strip=True)) if star_link else 0
        forks = _parse_number(fork_link.get_text(strip=True)) if fork_link else 0

        # Stars today — text like "3,257 stars today"
        stars_today = 0
        stars_today_span = article.select_one("span.d-inline-block.float-sm-right")
        if stars_today_span:
            match = re.search(r"([\d,]+)\s+stars?\s+today", stars_today_span.get_text())
            if match:
                stars_today = _parse_number(match.group(1))

        rows.append({
            "rank": rank,
            "repo_full_name": repo_full_name,
            "description": description,
            "language": language,
            "total_stars": total_stars,
            "forks": forks,
            "stars_today": stars_today,
            "url": url,
            "fetched_date": today_str,
        })

    log.info(f"  Found {len(rows)} trending repositories")
    return rows


# ─── Main ───────────────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("GitHub Trending Repositories Fetcher (Manus version)")
    log.info("=" * 60)

    rows = fetch_trending()

    if not rows:
        log.warning("No repos fetched. Exiting.")
        return

    # Print summary
    for r in rows:
        log.info(
            f"  {r['rank']:>2}. {r['repo_full_name']:<45} "
            f"★ {r['total_stars']:>7,}  ⑂ {r['forks']:>6,}  "
            f"+{r['stars_today']:>5,} today  [{r['language']}]"
        )

    # Save to Google Drive
    log.info("\nSaving to Google Drive...")
    count = ds.save_latest_and_cumulative(
        BASE_FILENAME,
        rows,
        HEADERS_ROW,
        DEDUP_KEYS,
    )

    log.info("=" * 60)
    log.info(f"Done! {count} trending repos saved to Google Drive.")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
