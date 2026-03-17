"""
Google Drive Native Sheets Storage Utility (gws CLI version)

Stores data as native Google Sheets in Google Drive using the gws CLI tool.
Data is organized into Latest (overwritten) and Cumulative (upserted) folders.

Folder structure:
  ROOT_FOLDER_ID/
    2026/
      Latest/
        news_raw_2026_latest          (Google Sheet)
        download_rank_7d_2026_latest  (Google Sheet)
        ...
      Cumulative/
        news_raw_2026                 (Google Sheet)
        download_rank_7d_2026         (Google Sheet)
        ...
"""

import os
import json
import logging
import subprocess
import time as _time
from datetime import datetime, timezone

log = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────

ROOT_FOLDER_ID = "1hzvd_SkU3z2oP-op9LtYn3Q50Op7qY_P"
SHEET_MIME = "application/vnd.google-apps.spreadsheet"
FOLDER_MIME = "application/vnd.google-apps.folder"

# Maximum cells per values.update request (Sheets API limit is 10M but we
# chunk to avoid overly large CLI args).  50 rows * 20 cols = 1000 per chunk.
CHUNK_ROWS = 200


# ─── gws CLI helpers ────────────────────────────────────────────────────────

def _run_gws(args, input_data=None, retries=3):
    """Run a gws command and return parsed JSON output with retry logic."""
    cmd = ["gws"] + args
    for attempt in range(retries):
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=120,
                input=input_data,
            )
            if result.returncode != 0:
                log.warning(
                    f"gws command failed (attempt {attempt+1}/{retries}): "
                    f"{' '.join(cmd)}\nstderr: {result.stderr[:500]}"
                )
                if attempt < retries - 1:
                    _time.sleep(2 * (attempt + 1))
                    continue
                log.error(f"gws command failed after {retries} attempts: {' '.join(cmd)}")
                return None
            if result.stdout.strip():
                return json.loads(result.stdout)
            return {}
        except subprocess.TimeoutExpired:
            log.warning(f"gws command timed out (attempt {attempt+1}/{retries}): {' '.join(cmd)}")
            if attempt < retries - 1:
                _time.sleep(2 * (attempt + 1))
                continue
            log.error(f"gws command timed out after {retries} attempts")
            return None
        except json.JSONDecodeError as e:
            log.error(f"Failed to parse gws output: {e}\nOutput: {result.stdout[:500]}")
            return None
        except Exception as e:
            log.warning(f"gws command error (attempt {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                _time.sleep(2 * (attempt + 1))
                continue
            log.error(f"gws command error after {retries} attempts: {e}")
            return None
    return None


# ─── Folder helpers ─────────────────────────────────────────────────────────

def _find_folder(parent_id, folder_name):
    """Find a folder by name under parent_id. Returns folder ID or None."""
    q = (
        f"name = '{folder_name}' and '{parent_id}' in parents "
        f"and mimeType = '{FOLDER_MIME}' and trashed = false"
    )
    result = _run_gws([
        "drive", "files", "list",
        "--params", json.dumps({"q": q, "fields": "files(id,name)", "pageSize": "1"}),
    ])
    if result and result.get("files"):
        return result["files"][0]["id"]
    return None


def _create_folder(parent_id, folder_name):
    """Create a folder under parent_id. Returns the new folder ID."""
    result = _run_gws([
        "drive", "files", "create",
        "--json", json.dumps({
            "name": folder_name,
            "mimeType": FOLDER_MIME,
            "parents": [parent_id],
        }),
    ])
    if result and result.get("id"):
        log.info(f"  Created folder: {folder_name}")
        return result["id"]
    log.error(f"  Failed to create folder: {folder_name}")
    return None


# In-memory cache for folder IDs to avoid repeated lookups
_folder_cache = {}


def ensure_subfolder(parent_id, folder_name):
    """Find or create a subfolder under parent_id. Returns the folder ID. Cached."""
    cache_key = f"{parent_id}/{folder_name}"
    if cache_key in _folder_cache:
        return _folder_cache[cache_key]
    folder_id = _find_folder(parent_id, folder_name)
    if folder_id:
        _folder_cache[cache_key] = folder_id
        return folder_id
    folder_id = _create_folder(parent_id, folder_name)
    if folder_id:
        _folder_cache[cache_key] = folder_id
    return folder_id


# ─── Folder management ──────────────────────────────────────────────────────

def _get_year_folder(year=None):
    """Get (or create) the year subfolder under root. Returns folder ID."""
    if year is None:
        year = datetime.now(timezone.utc).year
    return ensure_subfolder(ROOT_FOLDER_ID, str(year))


def get_latest_folder(year=None):
    """Get (or create) the Latest folder: root/{year}/Latest."""
    year_folder = _get_year_folder(year)
    return ensure_subfolder(year_folder, "Latest")


def get_cumulative_folder(year=None):
    """Get (or create) the Cumulative folder: root/{year}/Cumulative."""
    year_folder = _get_year_folder(year)
    return ensure_subfolder(year_folder, "Cumulative")


def _latest_filename(base_name, year=None):
    """Generate Latest filename: news_raw -> news_raw_2026_latest"""
    if year is None:
        year = datetime.now(timezone.utc).year
    base = base_name.removesuffix(".xlsx").removesuffix(".csv")
    return f"{base}_{year}_latest"


def _cumulative_filename(base_name, year=None):
    """Generate Cumulative filename: news_raw -> news_raw_2026"""
    if year is None:
        year = datetime.now(timezone.utc).year
    base = base_name.removesuffix(".xlsx").removesuffix(".csv")
    return f"{base}_{year}"


# ─── Google Sheet file operations ───────────────────────────────────────────

def _find_sheet_in_folder(name, folder_id):
    """Find a Google Sheet by name in a folder. Returns spreadsheet ID or None."""
    q = (
        f"name = '{name}' and '{folder_id}' in parents "
        f"and mimeType = '{SHEET_MIME}' and trashed = false"
    )
    result = _run_gws([
        "drive", "files", "list",
        "--params", json.dumps({"q": q, "fields": "files(id,name)", "pageSize": "1"}),
    ])
    if result and result.get("files"):
        return result["files"][0]["id"]
    return None


def _create_sheet_in_folder(name, folder_id):
    """Create a new empty Google Sheet in a folder. Returns spreadsheet ID."""
    result = _run_gws([
        "drive", "files", "create",
        "--json", json.dumps({
            "name": name,
            "mimeType": SHEET_MIME,
            "parents": [folder_id],
        }),
    ])
    if result and result.get("id"):
        return result["id"]
    log.error(f"  Failed to create Google Sheet: {name}")
    return None


def _sanitize_value(v):
    """Convert a value to a JSON-safe string for Google Sheets."""
    if v is None:
        return ""
    if isinstance(v, (int, float, bool)):
        return v
    return str(v)


def _write_rows_to_sheet(spreadsheet_id, rows, headers):
    """Clear and write all rows (header + data) to Sheet1 of a Google Sheet.

    Uses values.clear then values.update for a full overwrite.
    Data is written in chunks to avoid overly large CLI arguments.
    """
    # Step 1: Clear existing data
    _run_gws([
        "sheets", "spreadsheets", "values", "clear",
        "--params", json.dumps({
            "spreadsheetId": spreadsheet_id,
            "range": "Sheet1",
        }),
    ])

    # Step 2: Build all rows as a 2D array
    all_values = [list(headers)]
    for row in rows:
        all_values.append([_sanitize_value(row.get(h, "")) for h in headers])

    # Step 3: Write in chunks to avoid CLI arg size limits
    offset = 0
    while offset < len(all_values):
        chunk = all_values[offset : offset + CHUNK_ROWS]
        start_row = offset + 1  # 1-indexed
        end_row = start_row + len(chunk) - 1
        num_cols = len(headers)
        # Convert column number to letter (A-Z, AA-AZ, etc.)
        col_letter = _col_letter(num_cols)
        range_str = f"Sheet1!A{start_row}:{col_letter}{end_row}"

        result = _run_gws([
            "sheets", "spreadsheets", "values", "update",
            "--params", json.dumps({
                "spreadsheetId": spreadsheet_id,
                "range": range_str,
                "valueInputOption": "RAW",
            }),
            "--json", json.dumps({"values": chunk}),
        ])
        if not result:
            log.error(f"  Failed to write chunk at row {start_row}")
        offset += CHUNK_ROWS


def _col_letter(n):
    """Convert 1-based column number to Excel-style letter (1->A, 26->Z, 27->AA)."""
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result


def _read_sheet(spreadsheet_id):
    """Read all data from Sheet1 of a Google Sheet. Returns list of dicts."""
    result = _run_gws([
        "sheets", "+read",
        "--spreadsheet", spreadsheet_id,
        "--range", "Sheet1",
    ])
    if not result or "values" not in result:
        return []

    values = result["values"]
    if len(values) < 2:
        return []

    headers = values[0]
    rows = []
    for row_vals in values[1:]:
        # Pad row to match header length
        padded = row_vals + [""] * (len(headers) - len(row_vals))
        rows.append({h: v for h, v in zip(headers, padded)})
    return rows


def _write_to_folder(name, rows, headers, folder_id):
    """Write rows to a native Google Sheet in the specified folder (create or overwrite)."""
    spreadsheet_id = _find_sheet_in_folder(name, folder_id)

    if not spreadsheet_id:
        spreadsheet_id = _create_sheet_in_folder(name, folder_id)
        if not spreadsheet_id:
            log.error(f"  Failed to create {name}")
            return

    _write_rows_to_sheet(spreadsheet_id, rows, headers)
    log.info(f"  Wrote {len(rows)} rows to {name}")


# ─── High-level storage functions ────────────────────────────────────────────

def save_latest_and_cumulative(base_filename, rows, headers, dedup_keys):
    """Two-step save: overwrite Latest, then upsert into Cumulative.

    Step 1 — Latest: Overwrite the sheet with only the new rows.
    Step 2 — Cumulative: Read existing data, merge with new rows
             deduplicating by dedup_keys (new rows win on conflict).
    """
    if not rows:
        log.info(f"  No rows to save for {base_filename}")
        return 0

    latest_name = _latest_filename(base_filename)
    cumulative_name = _cumulative_filename(base_filename)

    # Step 1: Overwrite Latest
    latest_folder = get_latest_folder()
    _write_to_folder(latest_name, rows, headers, latest_folder)
    log.info(f"  [Latest] Wrote {len(rows)} rows to {latest_name}")

    # Step 2: Upsert into Cumulative
    cumulative_folder = get_cumulative_folder()
    sheet_id = _find_sheet_in_folder(cumulative_name, cumulative_folder)

    if sheet_id:
        existing = _read_sheet(sheet_id)

        def _make_key(row):
            return tuple(str(row.get(k, "")) for k in dedup_keys)

        new_keys = {_make_key(r) for r in rows}
        filtered = [r for r in existing if _make_key(r) not in new_keys]
        replaced = len(existing) - len(filtered)
        all_rows = filtered + rows
        if replaced:
            log.info(f"  [Cumulative] Replacing {replaced} existing rows")
    else:
        all_rows = rows

    # Final dedup safety check: remove any remaining duplicates by dedup_keys
    def _make_key(row):
        return tuple(str(row.get(k, "")) for k in dedup_keys)

    seen_keys = set()
    deduped_rows = []
    for row in all_rows:
        key = _make_key(row)
        if key not in seen_keys:
            seen_keys.add(key)
            deduped_rows.append(row)
    if len(deduped_rows) < len(all_rows):
        log.warning(f"  [Cumulative] Removed {len(all_rows) - len(deduped_rows)} duplicate rows in safety check")
    all_rows = deduped_rows

    _write_to_folder(cumulative_name, all_rows, headers, cumulative_folder)
    log.info(f"  [Cumulative] Saved {len(rows)} new rows to {cumulative_name} (total: {len(all_rows)})")
    return len(rows)


# ─── Read helpers ─────────────────────────────────────────────────────────────

def read_latest(base_filename, year=None):
    """Read a Google Sheet from the Latest folder. Returns list of dicts."""
    folder_id = get_latest_folder(year)
    name = _latest_filename(base_filename, year)

    sheet_id = _find_sheet_in_folder(name, folder_id)
    if not sheet_id:
        log.info(f"  {name} not found in Latest")
        return []

    rows = _read_sheet(sheet_id)
    log.info(f"  Read {len(rows)} rows from Latest/{name}")
    return rows


def read_cumulative(base_filename, year=None):
    """Read a Google Sheet from the Cumulative folder. Returns list of dicts."""
    folder_id = get_cumulative_folder(year)
    name = _cumulative_filename(base_filename, year)

    sheet_id = _find_sheet_in_folder(name, folder_id)
    if not sheet_id:
        log.info(f"  {name} not found in Cumulative")
        return []

    rows = _read_sheet(sheet_id)
    log.info(f"  Read {len(rows)} rows from Cumulative/{name}")
    return rows


# ─── Legacy helpers (root folder) ────────────────────────────────────────────

def find_file(filename):
    """Find a file by name in the root folder. Returns file ID or None."""
    return _find_sheet_in_folder(filename, ROOT_FOLDER_ID)


def find_file_in_folder(filename, folder_id):
    """Find a file by name in a specific folder. Returns file ID or None."""
    return _find_sheet_in_folder(filename, folder_id)
