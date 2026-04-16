#!/usr/bin/env python3
"""
OpenRouter Global App Ranking Fetcher (Manus version)
=====================================================
Scrapes the OpenRouter Apps page for the daily global ranking of AI apps
and agents, then stores the data as native Google Sheets in Google Drive
(Latest + Cumulative).

Fields captured:
  rank, app_name, description, categories, total_tokens, total_requests,
  origin_url, openrouter_url, icon_url, fetched_date

Runs daily (gated by run_all.py).
"""

import json
import logging
import re
from datetime import datetime, timezone

import requests

import drive_storage as ds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Configuration ────────────────────────────────────────────────────────────

APPS_URL = "https://openrouter.ai/apps"

BASE_FILENAME = "openrouter_ranking"

HEADERS_ROW = [
    "rank",
    "app_name",
    "description",
    "categories",
    "total_tokens",
    "total_tokens_display",
    "total_requests",
    "origin_url",
    "openrouter_url",
    "icon_url",
    "fetched_date",
]

DEDUP_KEYS = ["app_name", "fetched_date"]

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# How many top apps to save (the page returns up to 200)
TOP_N = 50


# ── Helpers ──────────────────────────────────────────────────────────────────

def _format_tokens(tokens: int) -> str:
    """Format token count into a human-readable string (e.g., '377.1B')."""
    if tokens >= 1_000_000_000_000:
        return f"{tokens / 1_000_000_000_000:.1f}T"
    if tokens >= 1_000_000_000:
        return f"{tokens / 1_000_000_000:.1f}B"
    if tokens >= 1_000_000:
        return f"{tokens / 1_000_000:.1f}M"
    if tokens >= 1_000:
        return f"{tokens / 1_000:.1f}K"
    return str(tokens)


def _get_favicon_url(origin_url: str) -> str:
    """Generate a Google favicon proxy URL for an origin URL."""
    if not origin_url:
        return ""
    return (
        f"https://t0.gstatic.com/faviconV2?client=SOCIAL&type=FAVICON"
        f"&fallback_opts=TYPE,SIZE,URL&url={origin_url}&size=256"
    )


# ── Fetch & Parse ────────────────────────────────────────────────────────────

def fetch_ranking() -> list[dict]:
    """Fetch the OpenRouter apps page and extract the global daily ranking
    from the embedded Next.js RSC payload."""

    log.info(f"  Fetching {APPS_URL} ...")
    resp = requests.get(APPS_URL, headers={"User-Agent": USER_AGENT}, timeout=30)
    resp.raise_for_status()
    html = resp.text
    log.info(f"  Received {len(html):,} bytes")

    # Extract RSC (React Server Components) payloads
    payloads = re.findall(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', html)
    log.info(f"  Found {len(payloads)} RSC payloads")

    # Find the payload containing "rankingMap" with "day" data
    ranking_data = None
    for payload in payloads:
        if 'rankingMap' not in payload:
            continue

        # Decode JS string escapes (the regex captures content inside
        # a JS string literal, so quotes appear as \" in the match)
        decoded = payload.replace('\\"', '"').replace('\\/', '/').replace('\\n', '\n')

        # Find and extract the "day" array
        day_idx = decoded.find('"day":[')
        if day_idx < 0:
            continue

        arr_start = decoded.find('[', day_idx)
        depth = 0
        end = arr_start
        for i in range(arr_start, len(decoded)):
            if decoded[i] == '[':
                depth += 1
            elif decoded[i] == ']':
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break

        day_str = decoded[arr_start:end]
        try:
            ranking_data = json.loads(day_str)
            log.info(f"  Parsed {len(ranking_data)} apps from rankingMap.day")
            break
        except json.JSONDecodeError as e:
            log.warning(f"  Failed to parse day array: {e}")
            continue

    if not ranking_data:
        log.error("  Could not find ranking data in the page")
        return []

    return ranking_data


def build_rows(ranking_data: list[dict], top_n: int = TOP_N) -> list[dict]:
    """Convert raw ranking data into rows for Google Sheets."""
    fetched_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Sort by rank
    ranking_data.sort(key=lambda x: x.get("rank", 9999))

    rows = []
    for entry in ranking_data[:top_n]:
        app = entry.get("app", {})
        total_tokens = int(entry.get("total_tokens", 0))
        total_requests = int(entry.get("total_requests", 0))
        origin_url = app.get("origin_url", "") or ""
        slug = app.get("slug", "")
        openrouter_url = f"https://openrouter.ai/apps/{slug}" if slug else ""
        categories = ", ".join(app.get("categories", []))

        rows.append({
            "rank": entry.get("rank", ""),
            "app_name": app.get("title", ""),
            "description": app.get("description", ""),
            "categories": categories,
            "total_tokens": total_tokens,
            "total_tokens_display": _format_tokens(total_tokens),
            "total_requests": total_requests,
            "origin_url": origin_url,
            "openrouter_url": openrouter_url,
            "icon_url": _get_favicon_url(origin_url),
            "fetched_date": fetched_date,
        })

    return rows


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print(f"\nOpenRouter Global Ranking Fetcher")
    print(f"{'=' * 50}")

    ranking_data = fetch_ranking()
    if not ranking_data:
        print("  No ranking data found. Exiting.")
        return

    rows = build_rows(ranking_data)
    print(f"  Built {len(rows)} rows (top {TOP_N})")

    # Preview top 10
    print(f"\n  Top 10:")
    for row in rows[:10]:
        print(
            f"    {row['rank']:3d}. {row['app_name'][:35]:<35s} "
            f"{row['total_tokens_display']:>8s} tokens  [{row['categories']}]"
        )

    # Save to Google Drive
    print(f"\n  Saving to Google Drive...")
    saved = ds.save_latest_and_cumulative(
        base_filename=BASE_FILENAME,
        rows=rows,
        headers=HEADERS_ROW,
        dedup_keys=DEDUP_KEYS,
    )
    print(f"  Saved {saved} rows to Google Drive.")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
