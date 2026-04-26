#!/usr/bin/env python3
"""
Podcast Monitor (Manus version)
================================
Fetches RSS feeds from a curated list of English and Chinese tech/startup
podcasts, filters for episodes released on a target date (default: yesterday
in Singapore timezone), scrapes episode pages for richer text, summarises
each episode using an LLM, filters for tech-related content, and saves the
results to Google Drive as native Google Sheets.

Adapted from the original podcast_monitor.py:
- Uses llm_client.call_llm() instead of direct OpenAI calls
- Uses drive_storage.save_latest_and_cumulative() for Google Drive output
- Adds podcast image/artwork extraction from RSS feeds
- Uses podcast URL (link) as the primary key for upsert deduplication
"""

import datetime
import html
import json
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import feedparser
import pytz
import requests
from bs4 import BeautifulSoup

from llm_client import call_llm
from drive_storage import save_latest_and_cumulative

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

TIMEZONE = "Asia/Singapore"

PODCASTS = [
    # ---- English Podcasts ----
    {"name": "This Week in Startups",       "rss": "https://anchor.fm/s/7c624c84/podcast/rss"},
    {"name": "The Startup Ideas Podcast",   "rss": "https://rss.flightcast.com/ordbkg8yojpehffas7vr7qpc.xml"},
    {"name": "TechCrunch Equity",           "rss": "https://techcrunch.com/podcasts/equity/feed/"},
    {"name": "Acquired",                    "rss": "https://acquired.libsyn.com/rss"},
    {"name": "The All-In Podcast",          "rss": "https://rss.libsyn.com/shows/254861/destinations/1928300.xml"},
    {"name": "Product Hunt Radio",          "rss": "https://feeds.simplecast.com/iCV67fGr"},
    {"name": "Indie Hackers",               "rss": "https://feeds.transistor.fm/the-indie-hackers-podcast"},
    {"name": "The SaaS Podcast",            "rss": "https://feeds.megaphone.fm/VMP2403579036"},
    {"name": "Launched",                    "rss": "https://feeds.transistor.fm/launched"},
    {"name": "Hard Fork",                   "rss": "https://feeds.simplecast.com/6HKOhNgS"},
    {"name": "My First Million",            "rss": "https://feeds.megaphone.fm/HS2300184645"},
    {"name": "The Twenty Minute VC",        "rss": "https://thetwentyminutevc.libsyn.com/rss"},
    {"name": "Lenny's Podcast",             "rss": "https://api.substack.com/feed/podcast/10845.rss"},

    # ---- YouTube Channels ----
    {"name": "Y Combinator",                "rss": "https://www.youtube.com/feeds/videos.xml?channel_id=UCcefcZRL2oaA_uBNeo5UOWg"},
    {"name": "乱翻书 (Luan Fan Shu)",       "rss": "https://www.youtube.com/feeds/videos.xml?channel_id=UC0nqbhVSVHP9KVkHHBn58sw"},

    # ---- Chinese Podcasts ----
    {"name": "硅谷101 (Silicon Valley 101)", "rss": "https://feeds.fireside.fm/sv101/rss"},
    {"name": "What's Next｜科技早知道",       "rss": "https://feeds.fireside.fm/guiguzaozhidao/rss"},
    {"name": "OnBoard!",                     "rss": "https://feed.xyzfm.space/xxg7ryklkkft"},
    {"name": "疯投圈 (Crazy Capital)",        "rss": "https://crazy.capital/feed"},
    {"name": "商业就是这样",                   "rss": "https://feeds.fireside.fm/thatisbiz/rss"},
    {"name": "张小珺商业访谈录",               "rss": "https://rsshub.bestblogs.dev/xiaoyuzhou/podcast/626b46ea9cbbf0451cf5a962"},
    {"name": "科技乱炖",                      "rss": "https://feeds.daopub.com/ld.xml"},
    {"name": "乱翻书",                        "rss": "https://feed.xyzfm.space/yxuruh3f9mc4"},
    {"name": "少数派播客 (SSPAI)",             "rss": "https://sspai.typlog.io/feed/audio.xml"},
    {"name": "枫言枫语",                      "rss": "https://justinyan.me/feed/podcast"},
    {"name": "创业内幕 (Startup Insider)",     "rss": "https://www.ximalaya.com/album/20119986.xml"},
    {"name": "奇想驿 by 产品沉思录",           "rss": "https://feed.xyzfm.space/4wq8y3ymmc7p"},
]

MAX_WORKERS = 10
MAX_SCRAPE_LENGTH = 15000

REQUEST_TIMEOUT = 30
REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

# Google Sheet headers for podcast data
HEADERS = [
    "fetch_date", "podcast_name", "episode_title", "episode_title_original",
    "summary", "link", "audio_url", "pub_date", "podcast_image_url",
    "episode_image_url", "is_tech_related",
]

DEDUP_KEYS = ["link"]  # Primary key: podcast episode URL


# ---------------------------------------------------------------------------
# Helper Functions – Text Extraction
# ---------------------------------------------------------------------------

def strip_html(raw_html: str) -> str:
    """Remove HTML tags, unescape entities, and collapse whitespace."""
    if not raw_html:
        return ""
    text = re.sub(r"<[^>]+>", " ", raw_html)
    text = html.unescape(text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def get_full_rss_text(entry) -> str:
    """Extract ALL available text from an RSS feed entry."""
    candidates = []
    content_list = getattr(entry, "content", None)
    if content_list and isinstance(content_list, list):
        for item in content_list:
            candidates.append(strip_html(item.get("value", "")))
    for field in ("summary", "description"):
        val = getattr(entry, field, None)
        if val:
            candidates.append(strip_html(val))
    if candidates:
        return max(candidates, key=len)
    return ""


# ---------------------------------------------------------------------------
# Helper Functions – Image Extraction
# ---------------------------------------------------------------------------

def get_podcast_image(feed) -> str:
    """Extract the podcast-level artwork/image URL from the feed."""
    # Try feed.feed.image (standard RSS)
    feed_data = getattr(feed, "feed", None)
    if feed_data:
        # itunes:image
        itunes_image = getattr(feed_data, "image", None)
        if itunes_image:
            if isinstance(itunes_image, dict):
                url = itunes_image.get("href", "") or itunes_image.get("url", "")
                if url:
                    return url
            elif isinstance(itunes_image, str):
                return itunes_image

        # Check for itunes image in raw form
        for attr_name in ("itunes_image", "image"):
            img = getattr(feed_data, attr_name, None)
            if img:
                if isinstance(img, dict):
                    url = img.get("href", "") or img.get("url", "")
                    if url:
                        return url
                elif isinstance(img, str) and img.startswith("http"):
                    return img

    return ""


def get_episode_image(entry) -> str:
    """Extract the episode-level image URL from a feed entry."""
    # itunes:image on the entry
    itunes_image = getattr(entry, "image", None)
    if itunes_image:
        if isinstance(itunes_image, dict):
            url = itunes_image.get("href", "") or itunes_image.get("url", "")
            if url:
                return url
        elif isinstance(itunes_image, str) and itunes_image.startswith("http"):
            return itunes_image

    # Check media:thumbnail or media:content
    media_thumbnail = getattr(entry, "media_thumbnail", None)
    if media_thumbnail and isinstance(media_thumbnail, list):
        for thumb in media_thumbnail:
            url = thumb.get("url", "")
            if url:
                return url

    media_content = getattr(entry, "media_content", None)
    if media_content and isinstance(media_content, list):
        for mc in media_content:
            if "image" in mc.get("type", ""):
                url = mc.get("url", "")
                if url:
                    return url

    # Try to extract from content HTML
    content_list = getattr(entry, "content", None)
    if content_list and isinstance(content_list, list):
        for item in content_list:
            val = item.get("value", "")
            if val:
                match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', val)
                if match:
                    return match.group(1)

    # Try summary/description HTML
    for field in ("summary", "description"):
        val = getattr(entry, field, None)
        if val:
            match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', val)
            if match:
                return match.group(1)

    return ""


# ---------------------------------------------------------------------------
# Helper Functions – RSS Parsing
# ---------------------------------------------------------------------------

def get_publish_date(entry) -> datetime.datetime | None:
    """Extract the publish date from a feed entry as a UTC datetime."""
    for attr in ("published_parsed", "updated_parsed"):
        parsed = getattr(entry, attr, None)
        if parsed:
            return datetime.datetime(*parsed[:6], tzinfo=pytz.utc)
    return None


def get_audio_url(entry) -> str:
    """Extract the first audio enclosure URL from a feed entry."""
    for enc in getattr(entry, "enclosures", []):
        if "audio" in enc.get("type", "") or enc.get("href", "").endswith(
            (".mp3", ".m4a")
        ):
            return enc.get("href", "")
    for link in getattr(entry, "links", []):
        if "audio" in link.get("type", ""):
            return link.get("href", "")
    return ""


def fetch_and_filter(
    podcast: dict, target_date: datetime.date, tz: pytz.BaseTzInfo
) -> list[dict]:
    """Fetch an RSS feed and return episodes published on *target_date*."""
    name = podcast["name"]
    rss_url = podcast["rss"]
    episodes = []

    try:
        feed = feedparser.parse(rss_url)
        if feed.bozo and not feed.entries:
            print(f"  [WARN] Could not parse '{name}': {feed.bozo_exception}")
            return episodes

        # Get podcast-level image
        podcast_image = get_podcast_image(feed)

        for entry in feed.entries:
            pub_utc = get_publish_date(entry)
            if pub_utc is None:
                continue

            pub_local = pub_utc.astimezone(tz)
            if pub_local.date() != target_date:
                continue

            title = entry.get("title", "No title")
            full_rss_text = get_full_rss_text(entry)
            link = entry.get("link", "").strip()
            # Fallback: if link is empty, try to construct one from the GUID/ID,
            # or use the audio URL as the link
            if not link:
                entry_id = entry.get("id", "").strip()
                if entry_id and entry_id.startswith("http"):
                    link = entry_id
            if not link:
                # Use audio enclosure URL as last resort
                for enc_link in entry.get("links", []):
                    href = enc_link.get("href", "")
                    if href and enc_link.get("rel") == "enclosure":
                        link = href
                        break
            if not link:
                link = get_audio_url(entry)  # final fallback
            audio_url = get_audio_url(entry)
            episode_image = get_episode_image(entry)

            episodes.append(
                {
                    "podcast_name": name,
                    "episode_title": title,
                    "rss_description": full_rss_text,
                    "scraped_text": "",
                    "link": link,
                    "audio_url": audio_url,
                    "pub_date": pub_local.strftime("%Y-%m-%d %H:%M %Z"),
                    "podcast_image_url": podcast_image,
                    "episode_image_url": episode_image,
                    "summary": "",
                    "is_tech_related": True,
                }
            )

        status = (
            f"{len(episodes)} episode(s)" if episodes else "no episodes"
        )
        print(f"  [OK]   {name}: {status}")

    except Exception as exc:
        print(f"  [ERR]  {name}: {exc}")

    return episodes


# ---------------------------------------------------------------------------
# Step 1 – Scrape Episode Webpage for Transcript / Show Notes
# ---------------------------------------------------------------------------

def scrape_episode_page(url: str) -> str:
    """Fetch the episode webpage and extract the main text content."""
    if not url:
        return ""

    try:
        resp = requests.get(
            url, headers=REQUEST_HEADERS, timeout=REQUEST_TIMEOUT
        )
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"

        soup = BeautifulSoup(resp.text, "html.parser")

        for tag in soup(["script", "style", "nav", "header", "footer",
                         "noscript", "iframe", "svg"]):
            tag.decompose()

        main = (
            soup.find("article")
            or soup.find("main")
            or soup.find("div", class_=re.compile(
                r"(content|episode|show.?notes|description|post|entry)",
                re.I
            ))
            or soup.find("body")
        )

        if main:
            text = main.get_text(separator="\n", strip=True)
        else:
            text = soup.get_text(separator="\n", strip=True)

        text = re.sub(r"\n{3,}", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)

        if len(text) > MAX_SCRAPE_LENGTH:
            text = text[:MAX_SCRAPE_LENGTH] + "\n...[truncated]"

        return text

    except Exception as exc:
        print(f"    Scrape failed for {url}: {exc}")
        return ""


def enrich_with_scraped_text(episodes: list[dict]) -> None:
    """Scrape each episode's webpage in parallel to get richer text."""
    print(f"\n  Scraping {len(episodes)} episode page(s)...\n")

    with ThreadPoolExecutor(max_workers=5) as pool:
        future_map = {
            pool.submit(scrape_episode_page, ep["link"]): ep
            for ep in episodes
        }
        for future in as_completed(future_map):
            ep = future_map[future]
            scraped = future.result()
            ep["scraped_text"] = scraped
            chars = len(scraped)
            print(
                f"    [{ep['podcast_name']}] Scraped {chars} chars "
                f"from episode page"
            )


# ---------------------------------------------------------------------------
# Step 1.5 – Translate Non-English Episode Titles
# ---------------------------------------------------------------------------

def _is_non_english(text: str) -> bool:
    """Heuristic check: if >30% of characters are non-ASCII, treat as non-English."""
    if not text:
        return False
    non_ascii = sum(1 for c in text if ord(c) > 127)
    return non_ascii / len(text) > 0.3


def translate_titles(episodes: list[dict]) -> None:
    """Translate non-English episode titles to English using the LLM.

    Stores the original title in 'episode_title_original' and overwrites
    'episode_title' with the English translation.
    """
    to_translate = [ep for ep in episodes if _is_non_english(ep["episode_title"])]
    if not to_translate:
        print("  No non-English titles to translate.")
        return

    print(f"  Translating {len(to_translate)} non-English title(s)...")

    # Batch translate for efficiency
    titles_text = "\n".join(
        f"[{i}] {ep['episode_title']}" for i, ep in enumerate(to_translate)
    )

    prompt = f"""Translate each of the following podcast episode titles to English.
Keep the translation concise and faithful to the original meaning.
Respond with one translation per line in the format: [index] translated title

Titles:
{titles_text}

Translations:"""

    result = call_llm(prompt, system="You are a professional translator. Translate to English.",
                      max_tokens=1000, use_search=False)

    if not result:
        print("  Translation failed, keeping original titles.")
        return

    # Parse translations
    translations = {}
    for line in result.strip().splitlines():
        line = line.strip()
        match = re.match(r"\[?(\d+)\]?\s*(.+)", line)
        if match:
            idx = int(match.group(1))
            translated = match.group(2).strip()
            translations[idx] = translated

    for i, ep in enumerate(to_translate):
        if i in translations:
            ep["episode_title_original"] = ep["episode_title"]
            ep["episode_title"] = translations[i]
            print(f"    Translated: {ep['episode_title_original'][:40]}... -> {ep['episode_title'][:60]}")
        else:
            ep["episode_title_original"] = ep["episode_title"]
            print(f"    No translation for: {ep['episode_title'][:50]}")


# ---------------------------------------------------------------------------
# Step 2 – LLM Summarisation
# ---------------------------------------------------------------------------

def get_best_text(episode: dict) -> str:
    """Return the richest text available for an episode."""
    scraped = episode.get("scraped_text", "")
    rss = episode.get("rss_description", "")

    if scraped and rss:
        if len(scraped) > len(rss) * 1.5:
            return scraped
        else:
            return f"{rss}\n\n---\nAdditional details from episode page:\n{scraped}"
    return scraped or rss


def summarise_episode(episode: dict) -> dict:
    """Use the LLM to generate a concise summary of the episode."""
    podcast = episode["podcast_name"]
    title = episode["episode_title"]

    content = get_best_text(episode)
    if not content:
        episode["summary"] = "No content available for summarisation."
        return episode

    if len(content) > 12000:
        content = content[:12000] + "\n...[truncated]"

    print(f"  [{podcast}] Summarising ({len(content)} chars of text)...")

    prompt = f"""You are a podcast analyst. Summarise the following podcast episode
in 3-5 sentences. Focus on the key topics discussed, notable guests, and any
actionable insights or announcements. Write in English even if the content is
in another language.

Podcast: {podcast}
Episode title: {title}

Episode text content:
{content}

Summary:"""

    system = "You are a podcast analyst. Write concise, informative summaries in English."

    result = call_llm(prompt, system=system, max_tokens=500, use_search=False)
    if result:
        episode["summary"] = result
        print(f"  [{podcast}] Summary generated.")
    else:
        fallback = episode.get("rss_description", "")
        episode["summary"] = fallback[:500] if fallback else ""
        print(f"  [{podcast}] Summarisation failed, using fallback.")

    return episode


# ---------------------------------------------------------------------------
# Step 3 – Tech-Relevance Filtering
# ---------------------------------------------------------------------------

def filter_tech_episodes(episodes: list[dict]) -> list[dict]:
    """Use an LLM to determine which episodes are related to technology."""
    if not episodes:
        return episodes

    print(f"\n{'=' * 50}")
    print(f"  Filtering for tech-related episodes...")
    print(f"{'=' * 50}\n")

    episode_summaries = []
    for i, ep in enumerate(episodes):
        text = ep.get("summary") or ep.get("rss_description") or ep.get("episode_title", "")
        episode_summaries.append(
            f"[{i}] Podcast: {ep['podcast_name']}\n"
            f"    Title: {ep['episode_title']}\n"
            f"    Summary: {text}"
        )

    joined = "\n\n".join(episode_summaries)

    prompt = f"""You are a content classifier. Below is a list of podcast episodes
with their summaries. For each episode, determine whether it is related to
technology. Technology-related topics include (but are not limited to):
software, apps, AI, startups, SaaS, programming, hardware, internet,
cybersecurity, cloud computing, venture capital in tech, product development,
and tech industry news.

Episodes that are NOT tech-related include those primarily about: politics
(without a tech angle), sports, cooking, pure finance/economics without a
tech connection, entertainment/celebrity news, maritime shipping insurance,
military conflicts, etc.

For each episode, respond with its index number and either "TECH" or
"NOT_TECH". Respond ONLY with the classifications, one per line, in the
format:  [index] TECH  or  [index] NOT_TECH

Episodes:
{joined}

Classifications:"""

    result = call_llm(prompt, max_tokens=200, use_search=False)
    if not result:
        print("  Tech-filtering failed, keeping all episodes.")
        return episodes

    print(f"  LLM classification result:\n{result}\n")

    not_tech_indices = set()
    for line in result.splitlines():
        line = line.strip()
        match = re.match(r"\[?(\d+)\]?\s*(NOT_TECH|TECH)", line)
        if match:
            idx = int(match.group(1))
            label = match.group(2)
            if label == "NOT_TECH":
                not_tech_indices.add(idx)

    filtered = []
    for i, ep in enumerate(episodes):
        if i in not_tech_indices:
            ep["is_tech_related"] = False
            print(f"  [REMOVED] {ep['podcast_name']}: {ep['episode_title']}")
        else:
            ep["is_tech_related"] = True
            filtered.append(ep)

    print(f"\n  Kept {len(filtered)} of {len(episodes)} episodes.")
    return filtered


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    tz = pytz.timezone(TIMEZONE)
    now_local = datetime.datetime.now(tz)
    target_date = (now_local - datetime.timedelta(days=1)).date()

    print(f"\nPodcast Monitor (Manus version)")
    print(f"{'=' * 50}")
    print(f"  Timezone       : {TIMEZONE}")
    print(f"  Current time   : {now_local.strftime('%Y-%m-%d %H:%M %Z')}")
    print(f"  Target date    : {target_date}")
    print(f"  Podcasts       : {len(PODCASTS)}")
    print(f"{'=' * 50}")

    # ------------------------------------------------------------------
    # Phase 1: Fetch RSS feeds and filter by date
    # ------------------------------------------------------------------
    print(f"\n[Phase 1] Fetching RSS feeds...\n")

    all_episodes: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(fetch_and_filter, p, target_date, tz): p
            for p in PODCASTS
        }
        for future in as_completed(futures):
            all_episodes.extend(future.result())

    all_episodes.sort(key=lambda e: e["podcast_name"])

    print(f"\n  Episodes found: {len(all_episodes)}")

    if not all_episodes:
        print("\n  No episodes were published on this date. Exiting.")
        return

    # ------------------------------------------------------------------
    # Phase 2: Scrape episode webpages for richer text content
    # ------------------------------------------------------------------
    print(f"\n[Phase 2] Scraping episode webpages for transcripts/show notes...")
    enrich_with_scraped_text(all_episodes)

    # ------------------------------------------------------------------
    # Phase 2.5: Translate non-English episode titles
    # ------------------------------------------------------------------
    print(f"\n[Phase 2.5] Translating non-English episode titles...")
    translate_titles(all_episodes)

    # ------------------------------------------------------------------
    # Phase 3: Summarise each episode using LLM
    # ------------------------------------------------------------------
    print(f"\n[Phase 3] Generating summaries...\n")
    for ep in all_episodes:
        summarise_episode(ep)

    # ------------------------------------------------------------------
    # Phase 4: Filter out non-tech episodes
    # ------------------------------------------------------------------
    print(f"\n[Phase 4] Filtering for tech-related content...")
    tech_episodes = filter_tech_episodes(all_episodes)

    # ------------------------------------------------------------------
    # Phase 5: Save to Google Drive
    # ------------------------------------------------------------------
    print(f"\n[Phase 5] Saving to Google Drive...")

    now_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    # Build rows for Google Sheet (only tech-related episodes)
    rows = []
    for ep in tech_episodes:
        rows.append({
            "fetch_date": now_str,
            "podcast_name": ep["podcast_name"],
            "episode_title": ep["episode_title"],
            "episode_title_original": ep.get("episode_title_original", ep["episode_title"]),
            "summary": ep.get("summary", ""),
            "link": ep.get("link", ""),
            "audio_url": ep.get("audio_url", ""),
            "pub_date": ep.get("pub_date", ""),
            "podcast_image_url": ep.get("podcast_image_url", ""),
            "episode_image_url": ep.get("episode_image_url", ""),
            "is_tech_related": "TRUE",
        })

    if rows:
        saved = save_latest_and_cumulative(
            base_filename="podcast_episodes",
            rows=rows,
            headers=HEADERS,
            dedup_keys=DEDUP_KEYS,
        )
        print(f"  Saved {saved} podcast episodes to Google Drive.")
    else:
        print("  No tech-related episodes to save.")

    # ------------------------------------------------------------------
    # Summary
    # ------------------------------------------------------------------
    print(f"\n{'=' * 50}")
    print(f"SUMMARY")
    print(f"  Total episodes found: {len(all_episodes)}")
    print(f"  Tech-related episodes: {len(tech_episodes)}")
    print(f"  Saved to Google Drive: {len(rows)}")
    print(f"{'=' * 50}")


if __name__ == "__main__":
    main()
