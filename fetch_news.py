#!/usr/bin/env python3
"""
Daily News Fetcher (Manus version)
Fetches news from RSS feeds (36kr, TechCrunch, Techmeme), filters by category,
translates Chinese content, and stores results in Google Drive via gws CLI.

Adapted from the original GitHub Actions version:
- Uses llm_client.call_llm() instead of direct Gemini API calls
- Uses drive_storage.py (gws CLI) instead of Google OAuth
"""

import os
import re
import json
import time
import logging
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

import feedparser
import requests
from bs4 import BeautifulSoup

from llm_client import call_llm

# ─── Configuration ───────────────────────────────────────────────────────────

RSS_FEEDS = [
    {"name": "36kr", "url": "https://36kr.com/feed", "language": "zh"},
    {"name": "TechCrunch", "url": "https://techcrunch.com/feed/", "language": "en"},
    {"name": "Techmeme", "url": "https://www.techmeme.com/feed.xml", "language": "en"},
]

ALLOWED_CATEGORIES = ["New Product", "New Feature", "New VC Investment"]

EXCLUDED_TOPICS = [
    "energy", "solar", "wind power", "nuclear", "oil", "gas pipeline",
    "battery storage", "renewable energy", "fossil fuel",
    "hardware", "chip fabrication", "semiconductor manufacturing",
    "processor launch", "GPU release", "motherboard", "chip design",
    "CPU", "RAM", "SSD", "hard drive", "display panel", "sensor",
    "wearable device", "smart watch", "VR headset", "AR glasses",
    "robot hardware", "drone hardware", "3D printer", "IoT device",
    "smartphone launch", "laptop launch", "tablet launch",
    "finance", "stock market", "banking", "interest rate", "federal reserve",
    "treasury", "bond market", "forex", "cryptocurrency price",
    "IPO", "earnings report", "quarterly results",
    "npm package", "python package", "ruby gem", "crate release",
    "library release", "framework release", "SDK release",
    "programming language update", "compiler update", "runtime update",
    "git tool", "CI/CD", "devops tool", "testing framework",
    "code editor", "IDE plugin", "linter", "formatter",
    "database release", "web framework", "CSS framework",
]

HARD_EXCLUDE_TITLE_KEYWORDS = [
    "chip startup", "chip maker", "chipmaker", "chip company", "chip business",
    "chip design", "chip fab", "chip plant", "chip factory",
    "semiconductor", "wafer", "foundry", "TSMC", "ASML",
    "AI chip", "ai chip", "custom chip", "custom silicon",
    "GPU", "TPU", "NPU", "processor", "CPU",
    "Nvidia", "NVIDIA", "nvidia", "AMD", "Intel",
    "robot", "robotics", "humanoid", "quadruped", "robotic",
    "eVTOL", "evtol", "EVTOL", "flying car", "flying taxi",
    "drone", "satellite", "spacecraft", "rocket",
    "electric vehicle", "EV battery", "self-driving car", "autonomous vehicle",
    "Tesla", "Rivian", "Lucid Motors",
    "solar panel", "wind turbine", "nuclear reactor", "power plant",
    "biotech", "pharmaceutical", "drug trial", "clinical trial",
]

ALLOWED_FINANCE_KEYWORDS = [
    "venture capital", "vc", "funding round", "series a", "series b", "series c",
    "seed round", "raised", "investment round", "startup funding", "angel investor",
    "accelerator", "incubator",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


# ─── RSS Fetching ────────────────────────────────────────────────────────────

def fetch_rss(feed_config: dict) -> list[dict]:
    """Fetch and parse an RSS feed, returning entries from the last 24 hours."""
    url = feed_config["url"]
    name = feed_config["name"]
    language = feed_config["language"]

    log.info(f"Fetching RSS feed: {name} ({url})")

    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsFetcher/1.0)"}
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
        feed = feedparser.parse(resp.content)
    except Exception as e:
        log.error(f"Failed to fetch {name}: {e}")
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    entries = []

    for entry in feed.entries:
        pub_date = parse_entry_date(entry)
        if pub_date is None:
            pub_date = datetime.now(timezone.utc)

        if pub_date < cutoff:
            continue

        title = entry.get("title", "").strip()
        link = entry.get("link", "").strip()
        summary = entry.get("summary", "") or entry.get("description", "")

        if summary:
            soup = BeautifulSoup(summary, "html.parser")
            summary = soup.get_text(separator=" ", strip=True)

        image = extract_image_from_entry(entry, summary)

        entries.append({
            "title": title,
            "url": link,
            "content": summary[:3000],
            "datetime": pub_date,
            "source": name,
            "language": language,
            "image": image,
        })

    log.info(f"  Found {len(entries)} entries from last 24h in {name}")
    return entries


def parse_entry_date(entry) -> Optional[datetime]:
    """Parse the publication date from a feed entry."""
    for attr in ["published_parsed", "updated_parsed"]:
        parsed = getattr(entry, attr, None)
        if parsed:
            try:
                dt = datetime(*parsed[:6], tzinfo=timezone.utc)
                return dt
            except Exception:
                pass

    for attr in ["published", "updated", "dc_date"]:
        raw = entry.get(attr)
        if raw:
            try:
                from email.utils import parsedate_to_datetime
                dt = parsedate_to_datetime(raw)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except Exception:
                pass

    return None


def extract_image_from_entry(entry, summary: str) -> Optional[str]:
    """Try to extract a main image URL from the feed entry."""
    media_content = entry.get("media_content", [])
    if media_content:
        for media in media_content:
            url = media.get("url", "")
            if url and any(ext in url.lower() for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]):
                return url

    media_thumbnail = entry.get("media_thumbnail", [])
    if media_thumbnail:
        for thumb in media_thumbnail:
            url = thumb.get("url", "")
            if url:
                return url

    enclosures = entry.get("enclosures", [])
    for enc in enclosures:
        if enc.get("type", "").startswith("image/"):
            return enc.get("href", "") or enc.get("url", "")

    raw_summary = entry.get("summary", "") or entry.get("description", "")
    if raw_summary:
        soup = BeautifulSoup(raw_summary, "html.parser")
        img = soup.find("img")
        if img and img.get("src"):
            return img["src"]

    content_encoded = entry.get("content", [])
    if content_encoded:
        for c in content_encoded:
            val = c.get("value", "")
            if val:
                soup = BeautifulSoup(val, "html.parser")
                img = soup.find("img")
                if img and img.get("src"):
                    return img["src"]

    return None


# ─── Translation ─────────────────────────────────────────────────────────────

def _contains_chinese(text: str) -> bool:
    return bool(re.search(r'[\u4e00-\u9fff]', text))


def _extract_json_from_text(text: str) -> dict | None:
    if not text:
        return None

    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
        cleaned = re.sub(r'\s*```$', '', cleaned)
        cleaned = cleaned.strip()

    try:
        obj = json.loads(cleaned)
        if isinstance(obj, dict) and "title" in obj:
            return obj
    except json.JSONDecodeError:
        pass

    title_match = re.search(r'"title"\s*:\s*"((?:[^"\\]|\\.)*)"', cleaned)
    if not title_match:
        title_match_partial = re.search(r'"title"\s*:\s*"((?:[^"\\]|\\.)*)', cleaned)
        if title_match_partial:
            title_val = title_match_partial.group(1)
            if title_val.endswith("\\"):
                title_val = title_val[:-1]
            return {"title": title_val, "content": ""}
        return None

    title_val = title_match.group(1)
    content_val = ""
    content_match = re.search(r'"content"\s*:\s*"((?:[^"\\]|\\.)*)', cleaned)
    if content_match:
        content_val = content_match.group(1)
        if content_val.endswith("\\"):
            content_val = content_val[:-1]
    return {"title": title_val, "content": content_val}


def translate_to_english(title: str, content: str) -> tuple[str, str]:
    """Translate Chinese title and content to English using LLM."""
    truncated_content = content[:800]

    prompt = f"""Translate the following Chinese news title and content into English.
Return ONLY a JSON object with keys "title" and "content". No markdown, no code blocks, no explanation.

Title: {title}

Content: {truncated_content}"""

    system = 'You are a professional translator. Translate Chinese to English accurately. Return a single JSON object only: {"title": "...", "content": "..."}. Keep it concise.'

    for attempt in range(2):
        result = call_llm(prompt, system, max_tokens=3000, use_search=False)

        if not result:
            log.warning(f"Translation attempt {attempt + 1} returned empty, retrying...")
            time.sleep(1)
            continue

        data = _extract_json_from_text(result)
        if data:
            translated_title = data.get("title", title)
            translated_content = data.get("content", content)

            if not _contains_chinese(translated_title):
                return translated_title, translated_content
            else:
                log.warning(f"Translation attempt {attempt + 1} still contains Chinese in title, retrying...")
                time.sleep(1)
                continue
        else:
            log.warning(f"Translation attempt {attempt + 1} failed to parse: {result[:150]}")
            time.sleep(1)
            continue

    # Fallback: title-only translation
    log.info(f"    Falling back to title-only translation...")
    title_prompt = f"Translate this Chinese headline to English. Return ONLY the English translation, nothing else.\n\n{title}"
    title_result = call_llm(title_prompt, "", max_tokens=500, use_search=False)

    if title_result and not _contains_chinese(title_result.strip()):
        translated_title = title_result.strip().strip('"').strip()
        if truncated_content.strip():
            content_prompt = f"Translate this Chinese text to English. Return ONLY the English translation, nothing else.\n\n{truncated_content}"
            content_result = call_llm(content_prompt, "", max_tokens=2000, use_search=False)
            if content_result and not _contains_chinese(content_result.strip()[:100]):
                return translated_title, content_result.strip()
        return translated_title, content

    log.error(f"All translation attempts failed for: {title[:50]}")
    return title, content


# ─── Categorization & Filtering ──────────────────────────────────────────────

CATEGORIZATION_CHUNK_SIZE = 15


def _pre_filter_by_keywords(entries: list[dict]) -> list[dict]:
    passed = []
    for entry in entries:
        title_lower = entry['title'].lower()
        excluded = False
        for keyword in HARD_EXCLUDE_TITLE_KEYWORDS:
            if keyword.lower() in title_lower:
                log.info(f"  PRE-FILTER EXCLUDED: \"{entry['title'][:80]}\" (matched: '{keyword}')")
                excluded = True
                break
        if not excluded:
            passed.append(entry)
    return passed


def categorize_and_filter(entries: list[dict]) -> list[dict]:
    if not entries:
        return []

    log.info(f"  Running keyword pre-filter on {len(entries)} articles...")
    entries = _pre_filter_by_keywords(entries)
    log.info(f"  {len(entries)} articles passed pre-filter")

    if not entries:
        return []

    filtered = []
    total = len(entries)

    for chunk_start in range(0, total, CATEGORIZATION_CHUNK_SIZE):
        chunk = entries[chunk_start:chunk_start + CATEGORIZATION_CHUNK_SIZE]
        chunk_num = chunk_start // CATEGORIZATION_CHUNK_SIZE + 1
        total_chunks = (total + CATEGORIZATION_CHUNK_SIZE - 1) // CATEGORIZATION_CHUNK_SIZE
        log.info(f"  Categorizing chunk {chunk_num}/{total_chunks} ({len(chunk)} articles)...")

        chunk_results = _categorize_batch(chunk, chunk_start)
        filtered.extend(chunk_results)

        if chunk_start + CATEGORIZATION_CHUNK_SIZE < total:
            time.sleep(1)

    return filtered


def _categorize_batch(chunk: list[dict], global_offset: int) -> list[dict]:
    entries_text = ""
    for idx, entry in enumerate(chunk):
        entries_text += f"\n{idx + 1}. Title: {entry['title']}\n   URL: {entry.get('url', '')}\n   Source: {entry.get('source', '')}\n   Content: {entry['content'][:500]}\n"

    prompt = f"""You are a strict news categorization expert. Analyze ALL of the following articles and categorize each one.

IMPORTANT: Use Google Search to look up article URLs when the title/content is ambiguous. Understanding the full context is critical for accurate categorization.

CATEGORIES (choose exactly one per article):

1. "New Product" — The article announces a NEWLY LAUNCHED SOFTWARE product, app, website, GitHub project, software tool, platform, or AI model.
   - ONLY software products count. Physical/hardware products do NOT count.
   - An article about a company raising money is NOT a new product (it's VC investment).

2. "New Feature" — The article announces a NEW FEATURE added to an EXISTING product/app/platform/service.

3. "New VC Investment" — The article announces a new venture capital investment, funding round, or acquisition of a SOFTWARE/TECH startup.
   - EXCLUDE if the company receiving investment makes HARDWARE.

4. "EXCLUDE" — The article does NOT fit any of the above three categories.

When in doubt, ALWAYS choose EXCLUDE.

Articles:
{entries_text}

Respond with ONLY a JSON array of objects, each with "index" (1-based), "category", and "reason" (brief 1-sentence explanation).
Example: [{{"index": 1, "category": "New Product", "reason": "Announces launch of a new AI tool"}}]
No other text."""

    system = "You are a news categorization expert. Categorize tech news articles strictly. Use Google Search to verify article context when needed. Be strict: if an article does not clearly fit New Product, New Feature, or New VC Investment, mark it as EXCLUDE."

    result = call_llm(prompt, system, max_tokens=4000, use_search=True)

    if not result:
        log.error("Empty LLM response for batch categorization")
        return []

    categories = _parse_categorization_response(result, len(chunk))

    filtered = []
    for cat_info in categories:
        idx = cat_info.get("index", 0) - 1
        category = cat_info.get("category", "EXCLUDE")
        reason = cat_info.get("reason", "")

        if 0 <= idx < len(chunk):
            global_idx = global_offset + idx
            if category in ALLOWED_CATEGORIES:
                entry = chunk[idx].copy()
                entry["category"] = category
                filtered.append(entry)
                log.info(f"  [{global_idx+1}] {chunk[idx]['title'][:60]}... -> {category} ({reason})")
            else:
                log.info(f"  [{global_idx+1}] {chunk[idx]['title'][:60]}... -> EXCLUDED ({reason})")

    return filtered


def _parse_categorization_response(result: str, expected_count: int) -> list[dict]:
    try:
        cleaned = result.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)
        categories = json.loads(cleaned)
        if isinstance(categories, list):
            return categories
    except json.JSONDecodeError:
        pass

    match = re.search(r'\[\s*\{.*?\}\s*\]', result, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    categories = []
    for m in re.finditer(r'\{[^{}]*"index"\s*:\s*(\d+)[^{}]*"category"\s*:\s*"([^"]+)"[^{}]*\}', result):
        try:
            idx = int(m.group(1))
            cat = m.group(2)
            categories.append({"index": idx, "category": cat, "reason": ""})
        except (ValueError, IndexError):
            continue

    if categories:
        return categories

    log.warning(f"Failed to parse categorization response, excluding all")
    return []


# ─── LLM-based Deduplication ─────────────────────────────────────────────────

def deduplicate_articles(entries: list[dict]) -> list[dict]:
    if len(entries) <= 1:
        return entries

    articles_text = ""
    for idx, entry in enumerate(entries):
        articles_text += f"\n{idx + 1}. [{entry.get('source', '')}] {entry['title']}"

    prompt = f"""Analyze the following list of news articles and identify groups of articles that cover THE SAME topic or event.

Two articles are duplicates if they report on the same specific event, announcement, or story.

Articles:
{articles_text}

Respond with ONLY a JSON array of duplicate groups. Each group is an array of article indices (1-based).
If there are no duplicates, respond with an empty array: []

Example response: [[1, 5], [3, 7, 9]]
Only include groups with 2+ articles.
Respond with ONLY the JSON array, no other text."""

    system = "You are a news deduplication expert. Identify articles that cover the exact same news event or announcement."

    result = call_llm(prompt, system, max_tokens=1000, use_search=False)

    if not result:
        log.warning("Empty LLM response for deduplication, keeping all articles")
        return entries

    try:
        cleaned = result.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
            cleaned = re.sub(r'\s*```$', '', cleaned)
        groups = json.loads(cleaned)
        if not isinstance(groups, list):
            groups = []
    except json.JSONDecodeError:
        match = re.search(r'\[\s*\[.*?\]\s*\]', result, re.DOTALL)
        if match:
            try:
                groups = json.loads(match.group(0))
            except json.JSONDecodeError:
                groups = []
        else:
            groups = []

    if not groups:
        log.info("  No duplicate articles found")
        return entries

    indices_to_remove = set()
    for group in groups:
        if not isinstance(group, list) or len(group) < 2:
            continue
        valid_indices = [i - 1 for i in group if isinstance(i, int) and 1 <= i <= len(entries)]
        if len(valid_indices) < 2:
            continue
        best_idx = max(valid_indices, key=lambda i: len(entries[i].get('content', '')))
        for idx in valid_indices:
            if idx != best_idx:
                indices_to_remove.add(idx)
                log.info(f"  Removing duplicate [{idx+1}] \"{entries[idx]['title'][:60]}...\"")

    deduplicated = [e for i, e in enumerate(entries) if i not in indices_to_remove]
    log.info(f"  Removed {len(indices_to_remove)} duplicate(s), {len(deduplicated)} articles remaining")
    return deduplicated


# ─── Image Extraction from URL ───────────────────────────────────────────────

def try_extract_image_from_url(url: str) -> Optional[str]:
    if not url:
        return None
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; NewsFetcher/1.0)"}
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code != 200:
            return None
        soup = BeautifulSoup(resp.text, "html.parser")
        og_image = soup.find("meta", property="og:image")
        if og_image and og_image.get("content"):
            return og_image["content"]
        tw_image = soup.find("meta", attrs={"name": "twitter:image"})
        if tw_image and tw_image.get("content"):
            return tw_image["content"]
        for img in soup.find_all("img"):
            src = img.get("src", "")
            width = img.get("width", "")
            if src and (not width or int(width or 0) > 200):
                if any(ext in src.lower() for ext in [".jpg", ".jpeg", ".png", ".webp"]):
                    return src
    except Exception as e:
        log.debug(f"Failed to extract image from {url}: {e}")
    return None


# ─── Batch Summarization ────────────────────────────────────────────────────

SUMMARIZATION_CHUNK_SIZE = 10


def summarize_articles(entries: list[dict]) -> list[dict]:
    if not entries:
        return entries

    total = len(entries)
    log.info(f"  Summarizing {total} articles...")

    for chunk_start in range(0, total, SUMMARIZATION_CHUNK_SIZE):
        chunk = entries[chunk_start:chunk_start + SUMMARIZATION_CHUNK_SIZE]
        chunk_num = chunk_start // SUMMARIZATION_CHUNK_SIZE + 1
        total_chunks = (total + SUMMARIZATION_CHUNK_SIZE - 1) // SUMMARIZATION_CHUNK_SIZE
        log.info(f"  Summarizing chunk {chunk_num}/{total_chunks} ({len(chunk)} articles)...")
        _summarize_batch(chunk)
        if chunk_start + SUMMARIZATION_CHUNK_SIZE < total:
            time.sleep(1)

    return entries


def _summarize_batch(chunk: list[dict]) -> None:
    entries_text = ""
    for idx, entry in enumerate(chunk):
        content = entry['content']
        first_para = content.split('\n\n')[0].split('\n')[0].strip()
        if len(first_para) < 30:
            first_para = content[:300]
        else:
            first_para = first_para[:300]
        entries_text += f"\n{idx + 1}. Title: {entry['title']}\n   First paragraph: {first_para}\n"

    prompt = f"""For each article below, write EXACTLY ONE sentence summarizing the key point.

RULES:
- Use ONLY the first paragraph provided. Do NOT add any information beyond what is in the first paragraph.
- Output must be EXACTLY 1 sentence per article.
- Keep it under 150 characters if possible.
- Do NOT start with "The article..." or "This article...".

Articles:
{entries_text}

Respond with ONLY a JSON array of objects, each with "index" (1-based) and "summary" (exactly 1 sentence).
No other text, no markdown code blocks."""

    system = "You are a professional news editor. Write exactly ONE sentence per article. Be specific with names and numbers. Return valid JSON only."

    result = call_llm(prompt, system, max_tokens=4000, use_search=False)

    if not result:
        log.warning("Empty LLM response for summarization, keeping raw content")
        return

    summaries = _parse_summarization_response(result, len(chunk))

    for item in summaries:
        idx = item.get("index", 0) - 1
        summary = item.get("summary", "")
        if 0 <= idx < len(chunk) and summary:
            chunk[idx]["content"] = summary
            log.info(f"    [{idx+1}] Summarized: {chunk[idx]['title'][:50]}...")


def _parse_summarization_response(result: str, expected_count: int) -> list[dict]:
    cleaned = result.strip()
    if cleaned.startswith("```"):
        cleaned = re.sub(r'^```(?:json)?\s*', '', cleaned)
        cleaned = re.sub(r'\s*```$', '', cleaned)
        cleaned = cleaned.strip()

    try:
        summaries = json.loads(cleaned)
        if isinstance(summaries, list):
            return summaries
    except json.JSONDecodeError:
        pass

    match = re.search(r'\[\s*\{.*?\}\s*\]', result, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            pass

    summaries = []
    for m in re.finditer(r'"index"\s*:\s*(\d+)\s*,\s*"summary"\s*:\s*"((?:[^"\\]|\\.)*)"', result):
        try:
            summaries.append({"index": int(m.group(1)), "summary": m.group(2)})
        except (ValueError, IndexError):
            continue

    if summaries:
        return summaries

    log.warning("Failed to parse summarization response")
    return []


# ─── Google Drive XLSX Storage ────────────────────────────────────────────────

NEWS_CSV = "news_raw.xlsx"
NEWS_HEADERS = [
    "url", "date_of_news", "datetime_of_news", "source", "category",
    "title", "news_content", "main_picture",
]


def store_entries(entries: list[dict]) -> int:
    if not entries:
        return 0

    from drive_storage import save_latest_and_cumulative

    sgt = timezone(timedelta(hours=8))
    crawl_now = datetime.now(sgt)
    crawl_date = crawl_now.strftime("%Y-%m-%d")
    crawl_datetime = crawl_now.isoformat()

    rows = []
    for entry in entries:
        rows.append({
            "url": entry["url"],
            "date_of_news": crawl_date,
            "datetime_of_news": crawl_datetime,
            "source": entry["source"],
            "category": entry["category"],
            "title": entry["title"][:500],
            "news_content": entry["content"][:5000],
            "main_picture": entry.get("image") or "",
        })

    inserted = save_latest_and_cumulative(
        NEWS_CSV, rows, NEWS_HEADERS, dedup_keys=["url"]
    )
    return inserted


# ─── Main Pipeline ───────────────────────────────────────────────────────────

def main():
    log.info("=" * 60)
    log.info("Starting daily news fetch pipeline (Manus version)")
    log.info("=" * 60)

    # Step 1: Fetch all RSS feeds
    all_entries = []
    for feed_config in RSS_FEEDS:
        entries = fetch_rss(feed_config)
        all_entries.extend(entries)

    log.info(f"\nTotal entries fetched: {len(all_entries)}")

    if not all_entries:
        log.info("No entries found. Exiting.")
        return

    # Step 2: Translate Chinese entries (36kr)
    for entry in all_entries:
        if entry["language"] == "zh":
            log.info(f"  Translating: {entry['title'][:50]}...")
            entry["title"], entry["content"] = translate_to_english(
                entry["title"], entry["content"]
            )
            time.sleep(0.5)

    # Step 3: Try to extract images for entries without one
    for entry in all_entries:
        if not entry.get("image"):
            img = try_extract_image_from_url(entry["url"])
            if img:
                entry["image"] = img

    # Step 4: Categorize and filter (batch processing)
    log.info("\nCategorizing and filtering entries (batch)...")
    filtered_entries = categorize_and_filter(all_entries)
    log.info(f"Entries after filtering: {len(filtered_entries)}")

    # Step 4.5: LLM-based deduplication
    log.info("\nDeduplicating articles...")
    filtered_entries = deduplicate_articles(filtered_entries)
    log.info(f"Entries after deduplication: {len(filtered_entries)}")

    cat_counts = {}
    for entry in filtered_entries:
        cat = entry.get("category", "Unknown")
        cat_counts[cat] = cat_counts.get(cat, 0) + 1
    for cat, count in sorted(cat_counts.items()):
        log.info(f"  {cat}: {count}")

    # Step 5: Summarize articles
    log.info("\nSummarizing articles...")
    summarize_articles(filtered_entries)

    # Step 6: Store in Google Drive
    log.info("\nStoring entries in Google Drive...")
    inserted = store_entries(filtered_entries)
    log.info(f"\nPipeline complete. Inserted {inserted} new entries.")

    # Summary
    log.info("\n" + "=" * 60)
    log.info("SUMMARY")
    log.info(f"  Total fetched:    {len(all_entries)}")
    log.info(f"  After filtering:  {len(filtered_entries)}")
    log.info(f"  New inserted:     {inserted}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
