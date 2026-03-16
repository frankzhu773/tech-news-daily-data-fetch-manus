# Tech News Daily Data Fetch (Manus Edition)

Automated data fetching pipeline that runs as a Manus scheduled task. Adapted from the original [GitHub Actions version](https://github.com/frankzhu773/tech-news-daily-data-fetch).

## Overview

This pipeline collects tech news and app market data from multiple sources, processes it with AI, and stores results as native Google Sheets in Google Drive.

## Scripts

| Script | Description | Frequency |
|:---|:---|:---|
| `run_all.py` | Combined runner that orchestrates all tasks | Every hour |
| `fetch_news.py` | Fetches RSS news from 36kr, TechCrunch, Techmeme; translates, categorizes, and summarizes | Every hour |
| `fetch_sensortower.py` | Fetches app download/growth/advertiser rankings from Sensor Tower API | Once per day |
| `fetch_producthunt_top.py` | Fetches top 15 Product Hunt products | Once per day |
| `drive_storage.py` | Google Drive storage utility using `gws` CLI for native Google Sheets | Shared module |
| `llm_client.py` | LLM client using Gemini 2.5 Flash via OpenAI-compatible API | Shared module |

## Key Changes from Original

1. **LLM**: Replaced direct Gemini API with `gemini-2.5-flash` via OpenAI-compatible API, including search grounding support.
2. **Storage**: Replaced Google OAuth-based Python SDK with `gws` CLI, outputting **native Google Sheets** instead of `.xlsx` files.
3. **Scheduling**: Consolidated 3 GitHub Actions workflows into a single hourly Manus scheduled task with daily gating logic.
4. **GitHub Pages**: RSS feed generation and deployment removed (out of scope).

## Google Drive Structure

```
ROOT_FOLDER/
  2026/
    Latest/          (overwritten each run)
      news_raw_2026_latest
      download_rank_7d_2026_latest
      download_percent_rank_7d_2026_latest
      download_delta_rank_7d_2026_latest
      advertiser_rank_7d_2026_latest
      product_hunt_top_product_2026_latest
    Cumulative/      (appended with deduplication)
      news_raw_2026
      download_rank_7d_2026
      ...
```

## Environment Variables

| Variable | Description |
|:---|:---|
| `SENSORTOWER_API_KEY` | Sensor Tower API authentication key |
| `PH_API_KEY` | Product Hunt API client ID |
| `PH_API_SECRET` | Product Hunt API client secret |
| `OPENAI_API_KEY` | Pre-configured in Manus environment for Gemini 2.5 Flash |

## Dependencies

- Python 3.11+
- `openai` — LLM client
- `feedparser` — RSS feed parsing
- `openpyxl` — (legacy, may be removed)
- `requests` — HTTP client
- `gws` CLI — Google Workspace operations (pre-installed in Manus)

## Running Manually

```bash
cd /home/ubuntu/manus-data-fetch
SENSORTOWER_API_KEY="..." PH_API_KEY="..." PH_API_SECRET="..." python3 run_all.py
```

Or run individual scripts:

```bash
python3 fetch_news.py
python3 fetch_sensortower.py
python3 fetch_producthunt_top.py
```
