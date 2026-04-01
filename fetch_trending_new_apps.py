#!/usr/bin/env python3
"""
=============================================================================
SensorTower Trending New Apps Discovery Pipeline
=============================================================================
Discovers rapidly rising new mobile apps using SensorTower's Unified API.

Pipeline steps:
  1. Create a custom fields filter (release date ≤3 months, category exclusions)
  2. Fetch top trending unified apps sorted by 7-day growth rate
  3. Enrich each app with name, publisher, icon, and metadata
  4. Fetch app descriptions from SensorTower app detail endpoints
  5. Classify ALL apps via LLM to exclude games, finance, government,
     utility, and religion apps
  6. Apply exclusions, rank, and save to Google Drive (Latest + Cumulative)

Output:
  - trending_new_apps_2026_latest  (Google Sheet in Latest folder)
  - trending_new_apps_2026         (Google Sheet in Cumulative folder)
=============================================================================
"""

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

# ─── Local imports ─────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from drive_storage import save_latest_and_cumulative
from llm_client import call_llm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Configuration ─────────────────────────────────────────────────────────

SENSORTOWER_AUTH_TOKEN = os.environ.get(
    "SENSORTOWER_API_KEY", "ST0_xZUxAWTy_dtrh7ck4HmVXPo"
)
SENSORTOWER_BASE_URL = "https://api.sensortower.com"

LIMIT = 100  # Max apps to fetch from SensorTower
REGIONS = "WW"  # WW = worldwide

# Categories to EXCLUDE via Primary Category (server-side pre-filter)
EXCLUDE_PRIMARY_CATEGORIES = ["Games", "Finance", "Utilities", "Tools"]

# App IQ Category values to EXCLUDE (server-side pre-filter)
EXCLUDE_APPIQ_CATEGORIES = [
    "Law & Government",
    "Government Agencies & Organizations",
    "Government Benefits Management Apps",
    "Law & Legal Agencies",
    "Religion & Spirituality",
]

# App IQ boolean tags to EXCLUDE (server-side pre-filter)
EXCLUDE_APPIQ_TAGS = {
    "App IQ - Religion - Christianity": "True",
    "App IQ - Religion - Islam": "True",
}

# LLM batch size
LLM_BATCH_SIZE = 10

# Google Sheet output
BASE_FILENAME = "trending_new_apps"
AGGREGATED_FILENAME = "trending_new_apps_aggregated"
DEDUP_KEYS = ["fetch_date", "unified_app_id"]
AGGREGATED_HEADERS = [
    "fetch_date",
    "category",
    "app_count",
    "total_weekly_downloads",
    "total_dau_30d",
    "avg_growth_rate_pct",
    "example_apps",
]
AGGREGATED_DEDUP_KEYS = ["fetch_date", "category"]

HEADERS_OUT = [
    "fetch_date",
    "rank",
    "app_name",
    "publisher_name",
    "icon_url",
    "app_category",
    "primary_category",
    "app_iq_category",
    "earliest_release_date",
    "released_days_ago",
    "weekly_downloads",
    "download_delta",
    "growth_rate_pct",
    "weekly_revenue_cents",
    "all_time_downloads_ww",
    "current_us_rating",
    "free",
    "in_app_purchases",
    "last_30_days_dau_ww",
    "predominant_age_ww",
    "genders_ww",
    "most_popular_country",
    "app_description",
    "ios_app_id",
    "android_app_id",
    "unified_app_id",
]

# ─── API Helpers ───────────────────────────────────────────────────────────

API_HEADERS = {
    "Authorization": f"Bearer {SENSORTOWER_AUTH_TOKEN}",
    "Content-Type": "application/json",
    "Accept": "application/json",
}


def st_post(path, payload, retries=3):
    """POST to SensorTower API and return JSON."""
    url = f"{SENSORTOWER_BASE_URL}{path}"
    for attempt in range(retries):
        try:
            resp = requests.post(url, headers=API_HEADERS, json=payload, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"  st_post {path} attempt {attempt+1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    return None


def st_get(path, params=None, retries=3, timeout=120):
    """GET from SensorTower API and return JSON."""
    url = f"{SENSORTOWER_BASE_URL}{path}"
    if params is None:
        params = {}
    params["auth_token"] = SENSORTOWER_AUTH_TOKEN
    for attempt in range(retries):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            log.warning(f"  st_get {path} attempt {attempt+1}/{retries}: {e}")
            if attempt < retries - 1:
                time.sleep(3 * (attempt + 1))
    return None


def clean_html(text):
    """Strip HTML tags from description text."""
    if not text:
        return ""
    return re.sub(r"<[^>]+>", " ", text).strip()


# ─── STEP 1: Create Custom Fields Filter ──────────────────────────────────

def create_filter():
    """Build and submit a server-side custom fields filter."""
    log.info("=" * 60)
    log.info("STEP 1: Creating SensorTower custom fields filter")
    log.info("=" * 60)

    custom_fields = [
        # Include: released within last 3 months
        {
            "exclude": False,
            "global": True,
            "name": "Released Days Ago (WW)",
            "values": ["~ 1 week", "~ 2 weeks", "~ 1 month", "~ 3 months"],
        },
        # Include: not a game
        {
            "exclude": False,
            "global": True,
            "name": "Is a Game",
            "values": ["false"],
        },
        # Exclude: primary categories
        {
            "exclude": True,
            "global": True,
            "name": "Primary Category",
            "values": EXCLUDE_PRIMARY_CATEGORIES,
        },
        # Exclude: App IQ subcategories
        {
            "exclude": True,
            "global": True,
            "name": "App IQ Category",
            "values": EXCLUDE_APPIQ_CATEGORIES,
        },
    ]

    # Exclude: App IQ boolean tags
    for tag_name, tag_value in EXCLUDE_APPIQ_TAGS.items():
        custom_fields.append({
            "exclude": True,
            "global": True,
            "name": tag_name,
            "values": [tag_value],
        })

    result = st_post("/v1/custom_fields_filter", {"custom_fields": custom_fields})
    if not result or "custom_fields_filter_id" not in result:
        log.error("  Failed to create filter!")
        return None

    filter_id = result["custom_fields_filter_id"]
    log.info(f"  Filter ID: {filter_id}")
    log.info(f"  Exclusions: {EXCLUDE_PRIMARY_CATEGORIES}")
    return filter_id


# ─── STEP 2: Fetch Trending Unified Apps ──────────────────────────────────

def fetch_trending_apps(filter_id):
    """Fetch top trending unified (hybrid) apps by 7-day growth."""
    log.info("=" * 60)
    log.info("STEP 2: Fetching trending unified apps")
    log.info("=" * 60)

    # Use the most recent Monday as the reference date
    today = datetime.now(timezone.utc)
    days_since_monday = today.weekday()
    last_monday = today - timedelta(days=days_since_monday)
    date_str = last_monday.strftime("%Y-%m-%d")

    params = {
        "comparison_attribute": "transformed_delta",
        "time_range": "week",
        "measure": "units",
        "category": 0,
        "date": date_str,
        "regions": REGIONS,
        "limit": LIMIT,
        "device_type": "total",
        "custom_fields_filter_id": filter_id,
        "custom_tags_mode": "include_unified_apps",
    }

    log.info(f"  Date: {date_str}  |  Regions: {REGIONS}  |  Limit: {LIMIT}")
    data = st_get(
        "/v1/unified/sales_report_estimates_comparison_attributes",
        params,
        timeout=180,
    )

    if not data or (isinstance(data, dict) and "errors" in data):
        log.error(f"  API error: {json.dumps(data, indent=2) if data else 'No response'}")
        return []

    log.info(f"  Received {len(data)} unified apps")
    return data


# ─── STEP 3: Enrich Apps with Names & Metadata ───────────────────────────

def search_entity(app_id):
    """Look up a unified app by ID."""
    results = st_get(
        "/v1/unified/search_entities",
        {"term": app_id, "entity_type": "app"},
        timeout=30,
    )
    if isinstance(results, list) and results:
        for r in results:
            if r.get("app_id") == app_id or r.get("id") == app_id:
                return r
        return results[0]
    return None


def enrich_apps(raw_apps):
    """Resolve unified IDs to app names, publishers, and metadata."""
    log.info("=" * 60)
    log.info("STEP 3: Enriching apps with metadata")
    log.info("=" * 60)

    enriched = []
    for i, app in enumerate(raw_apps):
        app_id = app.get("app_id", "")
        entities = app.get("entities", [])
        custom_tags = entities[0].get("custom_tags", {}) if entities else {}

        # Metrics
        units_absolute = app.get("units_absolute", 0)
        units_delta = app.get("units_delta", 0)
        units_transformed_delta = app.get("units_transformed_delta", 0)
        revenue_absolute = app.get("revenue_absolute", 0)

        # Look up name & publisher
        try:
            info = search_entity(app_id)
            time.sleep(0.3)
        except Exception as e:
            log.warning(f"  [{i+1}] ERROR {app_id}: {e}")
            info = None

        app_name = "Unknown"
        publisher_name = "Unknown"
        icon_url = ""
        ios_app_id = ""
        android_app_id = ""

        if info:
            app_name = info.get("name") or info.get("humanized_name") or "Unknown"
            publisher_name = info.get("publisher_name", "Unknown")
            icon_url = info.get("icon_url", "")
            ios_apps = info.get("ios_apps", [])
            android_apps = info.get("android_apps", [])
            if ios_apps:
                ios_app_id = ios_apps[0].get("app_id", "")
            if android_apps:
                android_app_id = android_apps[0].get("app_id", "")

        enriched.append({
            "unified_app_id": app_id,
            "app_name": app_name,
            "publisher_name": publisher_name,
            "icon_url": icon_url,
            "ios_app_id": str(ios_app_id),
            "android_app_id": str(android_app_id),
            "primary_category": custom_tags.get("Primary Category", "N/A"),
            "app_iq_category": custom_tags.get("App IQ Category", "N/A"),
            "earliest_release_date": custom_tags.get("Earliest Release Date", "N/A"),
            "released_days_ago": custom_tags.get("Released Days Ago (WW)", "N/A"),
            "weekly_downloads": units_absolute,
            "download_delta": units_delta,
            "growth_rate": units_transformed_delta,
            "weekly_revenue_cents": revenue_absolute,
            "all_time_downloads_ww": custom_tags.get("All Time Downloads (WW)", "N/A"),
            "current_us_rating": custom_tags.get("Current US Rating", "N/A"),
            "free": custom_tags.get("Free", "N/A"),
            "in_app_purchases": custom_tags.get("In-App Purchases", "N/A"),
            "last_30_days_dau_ww": custom_tags.get("Last 30 Days DAU (WW)", "N/A"),
            "predominant_age_ww": custom_tags.get("Predominant Age (Last Quarter, WW)", "N/A"),
            "genders_ww": custom_tags.get("Genders (Last Quarter, WW)", "N/A"),
            "most_popular_country": custom_tags.get("Most Popular Country by Downloads", "N/A"),
            "app_description": "",  # Filled in Step 4
        })

        tag = f"OK {app_name}" if app_name != "Unknown" else f"? {app_id}"
        log.info(f"  [{i+1}/{len(raw_apps)}] {tag} ({enriched[-1]['primary_category']})")

    log.info(f"  Enriched {len(enriched)} apps")
    return enriched


# ─── STEP 4: Fetch App Descriptions ──────────────────────────────────────

def fetch_app_description(ios_app_id, android_app_id):
    """Fetch the app description from SensorTower's app detail endpoint.
    Tries iOS first (richer descriptions), falls back to Android."""
    desc = ""

    if ios_app_id:
        try:
            data = st_get(f"/v1/ios/apps/{ios_app_id}", timeout=30)
            if isinstance(data, dict) and "description" in data:
                d = data["description"]
                if isinstance(d, dict):
                    desc = d.get("full_description", "") or d.get("short_description", "")
                elif isinstance(d, str):
                    desc = d
            if desc:
                return clean_html(desc)
        except Exception:
            pass

    if android_app_id:
        try:
            data = st_get(f"/v1/android/apps/{android_app_id}", timeout=30)
            if isinstance(data, dict) and "description" in data:
                d = data["description"]
                if isinstance(d, dict):
                    desc = d.get("full_description", "") or d.get("short_description", "")
                elif isinstance(d, str):
                    desc = d
            if desc:
                return clean_html(desc)
        except Exception:
            pass

    return ""


def fetch_descriptions(enriched_apps):
    """Fetch app descriptions for all enriched apps."""
    log.info("=" * 60)
    log.info("STEP 4: Fetching app descriptions")
    log.info("=" * 60)

    success = 0
    for i, app in enumerate(enriched_apps):
        ios_id = app.get("ios_app_id", "")
        android_id = app.get("android_app_id", "")

        desc = fetch_app_description(ios_id, android_id)
        app["app_description"] = desc
        time.sleep(0.3)

        status = f"OK ({len(desc)} chars)" if desc else "- no description"
        log.info(f"  [{i+1}/{len(enriched_apps)}] {app['app_name'][:40]}: {status}")
        if desc:
            success += 1

    log.info(f"  Fetched descriptions for {success}/{len(enriched_apps)} apps")
    return enriched_apps


# ─── STEP 5: Classify ALL Apps via LLM ───────────────────────────────────

def classify_apps_with_llm(enriched_apps):
    """Use LLM to classify every app: exclude unwanted categories AND assign
    a descriptive app_category label using Google Search grounding."""
    log.info("=" * 60)
    log.info("STEP 5: Classifying ALL apps via LLM (exclude + categorize)")
    log.info("=" * 60)

    exclusions = {}
    total = len(enriched_apps)

    for batch_start in range(0, total, LLM_BATCH_SIZE):
        batch_end = min(batch_start + LLM_BATCH_SIZE, total)
        batch = enriched_apps[batch_start:batch_end]

        apps_text = ""
        for idx, app in enumerate(batch):
            app_num = batch_start + idx + 1
            desc_snippet = (
                app["app_description"][:500]
                if app["app_description"]
                else "No description available"
            )
            apps_text += (
                f"\n--- App #{app_num} ---\n"
                f"Name: {app['app_name']}\n"
                f"Store Category: {app['primary_category']}\n"
                f"Publisher: {app['publisher_name']}\n"
                f"Description: {desc_snippet}\n"
            )

        prompt = (
            "You are a mobile app classifier. For each app below, do TWO things:\n\n"
            "**Task A — Exclude check:** Determine if the app belongs to ANY of these "
            "5 excluded categories:\n"
            "1. **Games** — actual playable games, game mods, game skins, game companion apps\n"
            "2. **Finance** — banking, investing, crypto, stock trading, insurance, loans, money transfer\n"
            "3. **Government** — government services, police, military, tax filing, civic services\n"
            "4. **Utility** — pure tools like keyboards, calculators, VPNs, caller IDs, "
            "file managers, battery savers, flashlights, QR scanners, PDF viewers\n"
            "5. **Religion** — prayer apps, scripture readers, worship aids, faith-based social\n\n"
            "**Task B — Category label:** Assign a concise, descriptive app category label "
            "that captures what the app actually does. Use specific labels like:\n"
            "  AI Assistant, AI Image Generator, AI Video Creator, Social Networking, "
            "  Short Video, Photo Editor, Video Editor, Dating, E-commerce, Food Delivery, "
            "  Fitness Tracker, Mental Health, Language Learning, Music Streaming, "
            "  Podcast Player, News Aggregator, Task Manager, Note Taking, "
            "  Ride Hailing, Travel Booking, Real Estate, Job Search, Parenting, "
            "  Pet Care, Fashion, Beauty, Sports, Education, Health, Meditation, etc.\n"
            "  Use Google Search if needed to understand what the app does.\n\n"
            "For EACH app, respond with a JSON object on a single line:\n"
            '{"app_num": <number>, "exclude": true/false, '
            '"exclude_reason": "<which excluded category or none>", '
            '"app_category": "<descriptive category label>"}\n\n'
            "Rules:\n"
            "- Only exclude if the app's PRIMARY purpose clearly fits an excluded category\n"
            "- If unsure or borderline, lean toward KEEPING the app\n"
            "- DO NOT exclude: sports fan apps, AI chat apps, parking apps, "
            "gamified education, video/photo editors, earning/rewards apps\n\n"
            "Return ONLY the JSON lines, one per app, no other text.\n\n"
            f"APPS TO CLASSIFY:\n{apps_text}"
        )

        # Use search grounding so Gemini can look up unfamiliar apps
        response_text = call_llm(prompt, max_tokens=3000, use_search=True)

        if response_text:
            for line in response_text.split("\n"):
                line = line.strip()
                if not line or not line.startswith("{"):
                    continue
                try:
                    result = json.loads(line)
                    app_num = result.get("app_num", 0)
                    should_exclude = result.get("exclude", False)
                    exclude_reason = result.get("exclude_reason", "none")
                    app_category = result.get("app_category", "Other")

                    if app_num < 1 or app_num > total:
                        continue

                    app = enriched_apps[app_num - 1]
                    app["app_category"] = app_category

                    if should_exclude:
                        exclusions[app["app_name"]] = f"{exclude_reason}"
                        log.info(f"  [EXCLUDE] #{app_num} {app['app_name']}: {exclude_reason} (cat: {app_category})")
                    else:
                        log.info(f"  [KEEP]    #{app_num} {app['app_name']} -> {app_category}")
                except json.JSONDecodeError:
                    continue
        else:
            log.warning(f"  LLM batch {batch_start+1}-{batch_end} returned empty")
            # Set default category for apps in failed batches
            for idx in range(batch_start, batch_end):
                if idx < total and not enriched_apps[idx].get("app_category"):
                    enriched_apps[idx]["app_category"] = enriched_apps[idx].get("primary_category", "Other")

        log.info(f"  --- Batch {batch_start+1}-{batch_end} done ---")
        time.sleep(1)

    # Ensure all apps have a category (fallback for any missed)
    for app in enriched_apps:
        if not app.get("app_category"):
            app["app_category"] = app.get("primary_category", "Other")

    log.info(f"  {len(exclusions)} apps marked for exclusion out of {total}")
    return exclusions


# ─── STEP 6: Apply Exclusions, Rank, and Save ────────────────────────────

def save_results(enriched_apps, exclusions):
    """Filter, rank, and save to Google Drive as native Google Sheets."""
    log.info("=" * 60)
    log.info("STEP 6: Applying exclusions and saving to Google Drive")
    log.info("=" * 60)

    fetch_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Separate clean vs excluded
    clean_apps = []
    for app in enriched_apps:
        name = app["app_name"]
        if name not in exclusions:
            clean_apps.append(app)

    # Sort by growth rate descending
    clean_apps.sort(key=lambda x: x.get("growth_rate", 0), reverse=True)

    log.info(
        f"  Original: {len(enriched_apps)}  |  "
        f"Excluded: {len(exclusions)}  |  "
        f"Final: {len(clean_apps)}"
    )

    if not clean_apps:
        log.warning("  No apps remaining after exclusions!")
        return 0

    # Build rows for Google Sheets
    rows = []
    for i, app in enumerate(clean_apps):
        rows.append({
            "fetch_date": fetch_date,
            "rank": i + 1,
            "app_name": app["app_name"],
            "publisher_name": app["publisher_name"],
            "icon_url": app["icon_url"],
            "app_category": app.get("app_category", app.get("primary_category", "Other")),
            "primary_category": app["primary_category"],
            "app_iq_category": app["app_iq_category"],
            "earliest_release_date": app["earliest_release_date"],
            "released_days_ago": app["released_days_ago"],
            "weekly_downloads": app["weekly_downloads"],
            "download_delta": app["download_delta"],
            "growth_rate_pct": round(app["growth_rate"] * 100, 2),
            "weekly_revenue_cents": app["weekly_revenue_cents"],
            "all_time_downloads_ww": app["all_time_downloads_ww"],
            "current_us_rating": app["current_us_rating"],
            "free": app["free"],
            "in_app_purchases": app["in_app_purchases"],
            "last_30_days_dau_ww": app["last_30_days_dau_ww"],
            "predominant_age_ww": app["predominant_age_ww"],
            "genders_ww": app["genders_ww"],
            "most_popular_country": app["most_popular_country"],
            "app_description": app["app_description"][:1000],  # Truncate for Sheets
            "ios_app_id": app["ios_app_id"],
            "android_app_id": app["android_app_id"],
            "unified_app_id": app["unified_app_id"],
        })

    # Save to Google Drive
    saved = save_latest_and_cumulative(BASE_FILENAME, rows, HEADERS_OUT, DEDUP_KEYS)
    log.info(f"  Saved {saved} rows to Google Drive")

    # Console summary
    log.info(f"\n{'Rank':<5} {'App Name':<40} {'Category':<22} {'Growth':<10} {'Downloads':<12}")
    log.info(f"{'-'*5} {'-'*40} {'-'*22} {'-'*10} {'-'*12}")
    for row in rows[:20]:
        log.info(
            f"{row['rank']:<5} {row['app_name'][:39]:<40} "
            f"{row['app_category'][:21]:<22} "
            f"{row['growth_rate_pct']:.0f}%{'':<6} "
            f"{row['weekly_downloads']:<12,}"
        )
    if len(rows) > 20:
        log.info(f"  ... and {len(rows) - 20} more apps")

    # ── STEP 7: Build aggregated category view ──
    log.info("=" * 60)
    log.info("STEP 7: Building aggregated category view")
    log.info("=" * 60)

    category_data = {}
    for row in rows:
        cat = row.get("app_category", "Other")
        if cat not in category_data:
            category_data[cat] = {
                "app_count": 0,
                "total_weekly_downloads": 0,
                "total_dau_30d": 0,
                "growth_rates": [],
                "example_apps": [],
            }
        entry = category_data[cat]
        entry["app_count"] += 1

        # Sum downloads
        try:
            entry["total_weekly_downloads"] += int(float(str(row.get("weekly_downloads", 0))))
        except (ValueError, TypeError):
            pass

        # Sum DAU
        try:
            dau_val = str(row.get("last_30_days_dau_ww", "0")).replace(",", "").strip()
            if dau_val and dau_val != "N/A":
                entry["total_dau_30d"] += int(float(dau_val))
        except (ValueError, TypeError):
            pass

        # Collect growth rates for averaging
        try:
            entry["growth_rates"].append(float(row.get("growth_rate_pct", 0)))
        except (ValueError, TypeError):
            pass

        # Collect example apps (up to 5 per category)
        if len(entry["example_apps"]) < 5:
            entry["example_apps"].append({
                "name": row.get("app_name", ""),
                "downloads_7d": row.get("weekly_downloads", 0),
                "growth_pct": row.get("growth_rate_pct", 0),
            })

    # Build aggregated rows
    agg_rows = []
    for cat, data in sorted(category_data.items(), key=lambda x: x[1]["total_weekly_downloads"], reverse=True):
        avg_growth = (
            round(sum(data["growth_rates"]) / len(data["growth_rates"]), 2)
            if data["growth_rates"]
            else 0
        )
        agg_rows.append({
            "fetch_date": fetch_date,
            "category": cat,
            "app_count": data["app_count"],
            "total_weekly_downloads": data["total_weekly_downloads"],
            "total_dau_30d": data["total_dau_30d"],
            "avg_growth_rate_pct": avg_growth,
            "example_apps": json.dumps(data["example_apps"], ensure_ascii=False),
        })

    # Save aggregated view
    if agg_rows:
        saved_agg = save_latest_and_cumulative(
            AGGREGATED_FILENAME, agg_rows, AGGREGATED_HEADERS, AGGREGATED_DEDUP_KEYS
        )
        log.info(f"  Saved {saved_agg} aggregated category rows")

        # Log aggregated summary
        log.info(f"\n{'Category':<30} {'Apps':<6} {'Downloads':<14} {'DAU 30d':<14} {'Avg Growth'}")
        log.info(f"{'-'*30} {'-'*6} {'-'*14} {'-'*14} {'-'*10}")
        for row in agg_rows:
            log.info(
                f"{row['category'][:29]:<30} "
                f"{row['app_count']:<6} "
                f"{row['total_weekly_downloads']:<14,} "
                f"{row['total_dau_30d']:<14,} "
                f"{row['avg_growth_rate_pct']:.1f}%"
            )

    return len(rows)


# ─── MAIN ─────────────────────────────────────────────────────────────────

def main():
    start_time = time.time()
    log.info("=" * 60)
    log.info("  SensorTower Trending New Apps Discovery Pipeline")
    log.info(f"  Run time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
    log.info("=" * 60)

    # Step 1: Create server-side filter
    filter_id = create_filter()
    if not filter_id:
        log.error("[ABORT] Failed to create filter.")
        return
    time.sleep(0.5)

    # Step 2: Fetch trending apps
    raw_apps = fetch_trending_apps(filter_id)
    if not raw_apps:
        log.error("[ABORT] No apps returned from SensorTower.")
        return

    # Step 3: Enrich with names & metadata
    enriched = enrich_apps(raw_apps)

    # Step 4: Fetch app descriptions
    enriched = fetch_descriptions(enriched)

    # Step 5: Classify ALL apps via LLM
    exclusions = classify_apps_with_llm(enriched)

    # Step 6: Save results to Google Drive
    save_results(enriched, exclusions)

    elapsed = time.time() - start_time
    log.info(f"\n[DONE] Pipeline completed in {elapsed:.1f}s")


if __name__ == "__main__":
    main()
