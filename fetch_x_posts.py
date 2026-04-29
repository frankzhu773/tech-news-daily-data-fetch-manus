#!/usr/bin/env python3
"""
X.com (Twitter) Posts Fetcher — Manus Scheduled Task
=====================================================
Fetches the latest tweets from a curated list of AI/tech accounts on X.com,
filters them for technology relevance using an LLM, summarizes each qualifying
tweet, and saves them as native Google Sheets in the Latest and Cumulative
folders on Google Drive.

Accounts tracked:
  @sama, @demishassabis, @ylecun, @karpathy, @AndrewYNg,
  @drfeifei, @rowancheung, @benthompson, @huggingface,
  @BytePlusGlobal, @GoogleAI, @Google, @AnthropicAI, @GeminiApp

Output files:
  Latest/posts_2026_latest       — overwritten each run
  Cumulative/posts_2026          — upserted with tweet link as dedup key
"""

import sys
import os
import json
import re
import logging
import time as _time
from datetime import datetime, timezone, timedelta

sys.path.append('/opt/.manus/.sandbox-runtime')
from data_api import ApiClient

from llm_client import call_llm
from drive_storage import save_latest_and_cumulative

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("fetch_x_posts")

# ── Configuration ──────────────────────────────────────────────────────────

ACCOUNTS = [
    {"username": "sama",           "display": "Sam Altman",        "role": "CEO of OpenAI"},
    {"username": "demishassabis",  "display": "Demis Hassabis",    "role": "CEO of Google DeepMind"},
    {"username": "ylecun",         "display": "Yann LeCun",        "role": "Chief AI Scientist at Meta"},
    {"username": "karpathy",       "display": "Andrej Karpathy",   "role": "AI Educator, ex-Tesla/OpenAI"},
    {"username": "AndrewYNg",      "display": "Andrew Ng",         "role": "Founder of DeepLearning.AI"},
    {"username": "drfeifei",       "display": "Dr. Fei-Fei Li",    "role": "Stanford Professor, AI Pioneer"},
    {"username": "rowancheung",    "display": "Rowan Cheung",      "role": "Founder of The Rundown AI"},
    {"username": "benthompson",    "display": "Ben Thompson",      "role": "Founder of Stratechery"},
    {"username": "huggingface",    "display": "Hugging Face",      "role": "Open-Source AI Community"},
    {"username": "BytePlusGlobal", "display": "BytePlus",          "role": "ByteDance's Enterprise Tech Platform"},
    {"username": "GoogleAI",        "display": "Google AI",         "role": "Google's AI Research & Products"},
    {"username": "Google",          "display": "Google",            "role": "Google"},
    {"username": "AnthropicAI",     "display": "Anthropic",         "role": "AI Safety Company, Makers of Claude"},
    {"username": "GeminiApp",       "display": "Gemini",            "role": "Google's AI Assistant"},
]

# Google Sheet headers
HEADERS = [
    "fetch_date",
    "source",
    "author",
    "author_username",
    "author_role",
    "tweet_type",
    "full_text",
    "full_text_original",
    "quoted_text",
    "quoted_text_original",
    "summary",
    "link",
    "images",
    "created_at",
    "is_tech_related",
    "profile_photo_url",
]

DEDUP_KEYS = ["link"]  # Primary key: tweet URL

# How many hours back to look for tweets
LOOKBACK_HOURS = float(os.environ.get("X_LOOKBACK_HOURS", "25"))

# ── Twitter API helpers ───────────────────────────────────────────────────

api_client = ApiClient()


# Cache for profile photo URLs (username -> url)
_profile_photo_cache = {}


def get_user_id_and_photo(username: str) -> tuple[str, str]:
    """Resolve a username to its rest_id and profile photo URL via the Twitter profile API."""
    resp = api_client.call_api(
        'Twitter/get_user_profile_by_username',
        query={'username': username}
    )
    user_result = (resp.get('result', {})
                      .get('data', {})
                      .get('user', {})
                      .get('result', {}))
    rest_id = user_result.get('rest_id', '')

    # Extract profile photo from avatar.image_url
    # Replace '_normal' with '_400x400' for higher resolution
    photo_url = user_result.get('avatar', {}).get('image_url', '')
    if photo_url:
        photo_url = photo_url.replace('_normal.', '_400x400.')
    _profile_photo_cache[username] = photo_url

    return rest_id, photo_url


def fetch_user_tweets(user_id: str, count: int = 20):
    """Fetch recent tweets for a user_id."""
    return api_client.call_api(
        'Twitter/get_user_tweets',
        query={'user': user_id, 'count': str(count)}
    )


def parse_tweets(response: dict) -> list[dict]:
    """Extract structured tweet data from the raw API response."""
    tweets = []
    timeline = response.get('result', {}).get('timeline', {})
    instructions = timeline.get('instructions', [])

    for instruction in instructions:
        for entry in instruction.get('entries', []):
            entry_id = entry.get('entryId', '')
            if not (entry_id.startswith('tweet-') or entry_id.startswith('profile-conversation')):
                continue

            content = entry.get('content', {})
            if 'itemContent' in content:
                items = [content['itemContent']]
            elif 'items' in content:
                items = [
                    item.get('item', {}).get('itemContent', {})
                    for item in content.get('items', [])
                ]
            else:
                continue

            for item_content in items:
                result = item_content.get('tweet_results', {}).get('result', {})
                if result.get('__typename') == 'TweetWithVisibilityResults':
                    result = result.get('tweet', {})

                legacy = result.get('legacy', {})
                if not legacy:
                    continue

                tweet_id = legacy.get('id_str', result.get('rest_id', ''))
                full_text = legacy.get('full_text', '')
                created_at = legacy.get('created_at', '')

                # Tweet type
                is_retweet = bool(legacy.get('retweeted_status_result'))
                is_reply = bool(legacy.get('in_reply_to_status_id_str'))
                is_quote = bool(legacy.get('is_quote_status'))
                tweet_type = ('Retweet' if is_retweet else
                              'Reply' if is_reply else
                              'Quote' if is_quote else 'Original')

                # Parse timestamp
                try:
                    dt = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')
                except Exception:
                    dt = None

                # Extract images
                images = []
                extended = legacy.get('extended_entities', legacy.get('entities', {}))
                for media in extended.get('media', []):
                    if media.get('type') in ('photo', 'animated_gif'):
                        images.append(media.get('media_url_https', ''))

                # Get quoted tweet text if available
                quoted_text = ''
                if is_quote:
                    qt = legacy.get('quoted_status_result', result.get('quoted_status_result', {}))
                    qt_result = qt.get('result', {})
                    if qt_result.get('__typename') == 'TweetWithVisibilityResults':
                        qt_result = qt_result.get('tweet', {})
                    qt_legacy = qt_result.get('legacy', {})
                    quoted_text = qt_legacy.get('full_text', '')

                # Get retweeted text
                rt_text = ''
                if is_retweet:
                    rt = legacy.get('retweeted_status_result', {})
                    rt_result = rt.get('result', {})
                    if rt_result.get('__typename') == 'TweetWithVisibilityResults':
                        rt_result = rt_result.get('tweet', {})
                    rt_legacy = rt_result.get('legacy', {})
                    rt_text = rt_legacy.get('full_text', '')
                    # Also grab images from the retweeted tweet
                    rt_ext = rt_legacy.get('extended_entities', rt_legacy.get('entities', {}))
                    for media in rt_ext.get('media', []):
                        if media.get('type') in ('photo', 'animated_gif'):
                            images.append(media.get('media_url_https', ''))

                tweets.append({
                    'tweet_id': tweet_id,
                    'full_text': full_text,
                    'quoted_text': quoted_text,
                    'rt_text': rt_text,
                    'created_at': dt,
                    'type': tweet_type,
                    'images': images,
                })

    return tweets


# ── LLM-based filtering and summarization ─────────────────────────────────

def is_tech_related(text: str, author: str) -> bool:
    """Use the LLM to determine if a tweet is related to technology."""
    prompt = f"Author: {author}\nTweet:\n{text}"
    system = (
        "You are a strict technology relevance classifier. "
        "Given a tweet, determine if it is related to technology. "
        "RELEVANT topics include: AI, machine learning, software, apps, "
        "programming, coding, startups, tech products, tech companies, "
        "tech industry news, scientific research (CS, engineering, robotics), "
        "hardware, semiconductors, cloud computing, data science, "
        "open source, tech business strategy, and tech policy/regulation.\n\n"
        "NOT RELEVANT topics include: personal life updates, food, sports, "
        "politics (unless directly about tech regulation), humor/memes "
        "unrelated to tech, philosophy unrelated to AI, social commentary "
        "unrelated to technology, and general opinions not about tech.\n\n"
        "Respond with ONLY one word: YES or NO."
    )
    result = call_llm(prompt, system=system, max_tokens=5, use_search=False)
    if result:
        return result.strip().upper().startswith("YES")
    # On error, include the tweet (fail open)
    return True


def summarize_tweet(text: str, author: str) -> str:
    """Use the LLM to produce a concise one-line summary of a tweet."""
    prompt = f"Author: {author}\nTweet:\n{text}"
    system = (
        "You are a concise news summarizer. Given a tweet, produce a "
        "single-sentence summary (max 30 words) that captures the key point. "
        "Do not include the author name. Do not use quotes. "
        "If the tweet is very short or just an emoji, return it as-is."
    )
    result = call_llm(prompt, system=system, max_tokens=80, use_search=False)
    if result:
        return result.strip()
    # Fallback: truncate the original text
    clean = text.replace('\n', ' ').strip()
    return clean[:120] + ('...' if len(clean) > 120 else '')


# ── Translation ────────────────────────────────────────────────────────────

def _is_non_english(text: str) -> bool:
    """Heuristic check: if >30% of characters are non-ASCII, treat as non-English."""
    if not text:
        return False
    non_ascii = sum(1 for c in text if ord(c) > 127)
    return non_ascii / len(text) > 0.3


def translate_text(text: str) -> str:
    """Translate non-English text to English using the LLM."""
    if not text or not _is_non_english(text):
        return text

    prompt = f"""Translate the following text to English. Keep the translation faithful
to the original meaning. Only output the translation, nothing else.

Text:
{text}

Translation:"""

    result = call_llm(
        prompt,
        system="You are a professional translator. Translate to English faithfully and concisely.",
        max_tokens=500,
        use_search=False,
    )
    return result.strip() if result else text


def translate_tweets(results: list) -> list:
    """Translate non-English full_text and quoted_text in tweet results.

    Stores originals in separate fields and overwrites with English translations.
    Each result is a tuple of (acct, tweet, summary).
    """
    to_translate = []
    for i, (acct, tweet, summary) in enumerate(results):
        needs = _is_non_english(tweet['full_text']) or _is_non_english(tweet.get('quoted_text', ''))
        if needs:
            to_translate.append(i)

    if not to_translate:
        print("  No non-English tweets to translate.")
        return results

    print(f"  Translating {len(to_translate)} non-English tweet(s)...")

    for idx in to_translate:
        acct, tweet, summary = results[idx]

        # Translate full_text
        if _is_non_english(tweet['full_text']):
            tweet['full_text_original'] = tweet['full_text']
            translated = translate_text(tweet['full_text'])
            tweet['full_text'] = translated
            print(f"    [@{acct['username']}] Translated full_text: {tweet['full_text_original'][:40]}... -> {translated[:50]}")

        # Translate quoted_text
        if tweet.get('quoted_text') and _is_non_english(tweet['quoted_text']):
            tweet['quoted_text_original'] = tweet['quoted_text']
            translated = translate_text(tweet['quoted_text'])
            tweet['quoted_text'] = translated
            print(f"    [@{acct['username']}] Translated quoted_text")

    return results


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    now_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
    cutoff_str = cutoff.strftime('%Y-%m-%d %H:%M UTC')
    fetch_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')

    print(f"\nX.com Posts Fetcher (Manus version)")
    print(f"{'=' * 58}")
    print(f"  Time window : {cutoff_str} -> {now_str}")
    print(f"  Accounts    : {len(ACCOUNTS)}")
    print(f"  Lookback    : {LOOKBACK_HOURS} hours")
    print(f"{'=' * 58}\n")

    all_results = []    # list of (account_info, tweet_data, summary)
    total_fetched = 0
    total_filtered = 0

    for acct in ACCOUNTS:
        username = acct['username']
        display = acct['display']
        print(f"  Fetching @{username} ({display})...", end=" ", flush=True)

        try:
            user_id, profile_photo = get_user_id_and_photo(username)
            if not user_id:
                print("Could not resolve user ID")
                continue

            response = fetch_user_tweets(user_id, count=20)
            tweets = parse_tweets(response)

            # Filter to the time window
            recent = [t for t in tweets if t['created_at'] and t['created_at'] >= cutoff]
            total_fetched += len(recent)
            print(f"found {len(recent)} tweet(s) in window", end="", flush=True)

            kept = 0
            for tweet in recent:
                # Build the full context for classification and summarization
                context = tweet['full_text']
                if tweet['quoted_text']:
                    context += f"\n[Quoted tweet]: {tweet['quoted_text']}"
                if tweet['rt_text']:
                    context = tweet['rt_text']

                # LLM Tech Relevance Filter
                if not is_tech_related(context, display):
                    total_filtered += 1
                    continue

                summary = summarize_tweet(context, display)
                all_results.append((acct, tweet, summary))
                kept += 1

            suffix = f" ({len(recent) - kept} filtered out)" if len(recent) - kept > 0 else ""
            print(f" -> {kept} tech-related{suffix}")

        except Exception as e:
            print(f"\n    Error: {e}")

    # Sort all tweets by time (newest first)
    all_results.sort(
        key=lambda x: x[1]['created_at'] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )

    print(f"\n{'=' * 58}")
    print(f"  Total tweets in window: {total_fetched}")
    print(f"  Tech-related (kept):    {len(all_results)}")
    print(f"  Filtered out:           {total_filtered}")
    print(f"{'=' * 58}")

    if not all_results:
        print("\n  No tech-related tweets found. Nothing to save.")
        return

    # Translate non-English tweets
    print(f"\n  Translating non-English tweets...")
    all_results = translate_tweets(all_results)

    # Build rows for Google Sheet
    rows = []
    for acct, tweet, summary in all_results:
        tweet_url = f"https://x.com/{acct['username']}/status/{tweet['tweet_id']}"
        created_str = tweet['created_at'].strftime('%Y-%m-%d %H:%M UTC') if tweet['created_at'] else ''
        images_str = " | ".join(tweet['images']) if tweet['images'] else ""

        rows.append({
            "fetch_date": fetch_date,
            "source": "x",
            "author": acct['display'],
            "author_username": f"@{acct['username']}",
            "author_role": acct['role'],
            "tweet_type": tweet['type'],
            "full_text": tweet['full_text'],
            "full_text_original": tweet.get('full_text_original', tweet['full_text']),
            "quoted_text": tweet.get('quoted_text', ''),
            "quoted_text_original": tweet.get('quoted_text_original', tweet.get('quoted_text', '')),
            "summary": summary,
            "link": tweet_url,
            "images": images_str,
            "created_at": created_str,
            "is_tech_related": "TRUE",
            "profile_photo_url": _profile_photo_cache.get(acct['username'], ''),
        })

    # Save to Google Drive
    print(f"\n  Saving {len(rows)} posts to Google Drive...")
    saved = save_latest_and_cumulative(
        base_filename="posts",
        rows=rows,
        headers=HEADERS,
        dedup_keys=DEDUP_KEYS,
    )
    print(f"  Saved {saved} posts to Google Drive.")


if __name__ == "__main__":
    main()
