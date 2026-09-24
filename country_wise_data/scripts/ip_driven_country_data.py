#!/usr/bin/env python3
"""
edmingle_user_country_list_export.py

Pulls the full per-user list from Edmingle's /user/useranalyticslist
endpoint, paginated, capturing user_id + name/email + country (filterValue)
+ region + activity metrics per row. Unlike the earlier
useractivityadditionalstats collector, THIS endpoint returns user-level
rows with an _id you can join straight onto your enrollment file.

TIMESTAMPS: last_seen and created_at are written both as raw epoch
(last_seen_epoch, created_at_epoch -- for machine use / re-processing)
and as IST-formatted strings (last_seen_ist, created_at_ist, in
dd-mm-yyyy HH:MM:SS format matching the enrollment_day convention used
elsewhere in this pipeline). The epoch columns are never dropped, only
supplemented.

RATE LIMIT: capped at 30 requests/minute (config: rate_limit_per_minute).

RETRY-WITH-ROLLBACK: each page is fetched with retries. If a page
ultimately fails (exhausted retries or a permanent error), the script
stops immediately WITHOUT writing that page's rows and WITHOUT advancing
the checkpoint. Nothing partial is ever committed. Re-running the script
resumes cleanly from the last fully-completed page -- no duplicate rows,
no silently-skipped users. This matters more here than for the aggregate
endpoint: a skipped page here means ~per_page missing user records, not
just a missing summary number.

CRASH-SAFE RESUME: before each page, the CSV's current byte size is
recorded. Rows for a page are only appended AFTER a successful fetch,
and the checkpoint (last completed page + byte offset) is written
immediately after, via atomic replace. If the process dies between the
CSV append and the checkpoint write, the next run truncates the CSV back
to the last known-good checkpoint offset before resuming, guaranteeing
no duplicate or orphaned rows either way.

USAGE
  1. Copy ip_driven_country_data_config.example.json ->
     ip_driven_country_data_config.json and fill in the non-credential
     settings (base_url, filter_key, dates, etc).
  2. Run:
        python ip_driven_country_data.py --config ip_driven_country_data_config.json
  3. Safe to Ctrl+C / let a 429 penalty hit -- just re-run the same command.

CREDENTIAL HYGIENE
  apikey/orgid are never read from the config JSON file or argv. They're
  loaded from the shared ../credentials.yaml (one per ela_datasets/, used
  by every pipeline) and merged into the in-memory config dict.

LOGGING
  Every log line is timestamped (local system time). Each page's log
  line reports rows written so far and rows remaining, computed from the
  API's own total_rows figure minus cumulative rows written -- the
  cumulative count is itself persisted in the checkpoint, so "remaining"
  stays accurate across a Ctrl+C / resume, not just within one run.
"""

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set
# before any local module import below, so this and every module it pulls
# in gets compiled into one shared location instead of a scripts/__pycache__
# folder per pipeline.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import common
from common import RollingRateLimiter

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests")

try:
    import yaml
except ImportError:
    sys.exit("Missing dependency: pip install pyyaml")


SCRIPT_DIR = Path(__file__).resolve().parent
CREDENTIALS_PATH = SCRIPT_DIR.parent.parent / "credentials.yaml"
IST = ZoneInfo("Asia/Kolkata")


def log(msg: str):
    """Timestamped log line -- every log call goes through this so logs
    stay consistent and greppable/sortable in a long-running console or
    redirected log file."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def epoch_to_ist_str(epoch_value) -> str:
    """Convert a unix epoch (seconds) to a dd-mm-yyyy HH:MM:SS IST string,
    matching the enrollment_day dd-mm-yyyy convention used elsewhere in
    this pipeline. Returns '' for missing/invalid values rather than
    raising, so a single bad record doesn't kill the whole page."""
    if epoch_value in ("", None):
        return ""
    try:
        ts = int(float(epoch_value))
    except (TypeError, ValueError):
        return ""
    return datetime.fromtimestamp(ts, tz=IST).strftime("%d-%m-%Y %H:%M:%S")

PERMANENT_ERROR_CODES = {400, 401, 403, 404}
TRANSIENT_ERROR_COOLDOWN_SECONDS = 300  # known 429 penalty-state behavior
MAX_RETRIES_PER_PAGE = 5
REQUEST_TIMEOUT_SECONDS = 90

CSV_FIELDS = [
    "user_id", "name", "email", "contact_number",
    "country", "region", "time_spent_seconds", "total_sessions",
    "last_seen_epoch", "last_seen_ist",
    "created_at_epoch", "created_at_ist",
    "source_page",
]


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------

def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        sys.exit(
            f"Config file not found: {config_path}\n"
            f"Copy ip_driven_country_data_config.example.json to "
            f"{config_path.name} and fill in your settings first."
        )
    with open(config_path, encoding="utf-8") as f:
        cfg = json.load(f)

    required = ["base_url", "filter_key", "start_date", "end_date"]
    missing = [k for k in required if k not in cfg or cfg[k] in ("", None)]
    if missing:
        sys.exit(f"Config is missing required key(s): {', '.join(missing)}")

    # apikey/orgid are no longer stored in this pipeline's own config file --
    # they're credentials, and every pipeline under ela_datasets/ now reads
    # the single shared Edmingle API key/org id from ../credentials.yaml
    # (relative to this script's directory) instead of keeping its own copy.
    if not CREDENTIALS_PATH.exists():
        sys.exit(f"Shared credentials file not found: {CREDENTIALS_PATH}")
    edmingle_creds = common.load_credentials(CREDENTIALS_PATH)
    cfg["apikey"] = edmingle_creds.get("api_key")
    cfg["orgid"] = edmingle_creds.get("organization_id")

    if not cfg["apikey"] or cfg["apikey"] == "PASTE_YOUR_APIKEY_HERE":
        sys.exit(
            f"No valid Edmingle api_key found in {CREDENTIALS_PATH} -- "
            f"edit that shared credentials file first."
        )
    if not cfg["orgid"]:
        sys.exit(f"No valid Edmingle organization_id found in {CREDENTIALS_PATH}.")

    cfg.setdefault("per_page", 50)
    cfg.setdefault("sort_order", -1)
    cfg.setdefault("rate_limit_per_minute", 30)
    cfg.setdefault("output_csv", "user_country_list.csv")
    cfg.setdefault("checkpoint_file", "user_country_list_checkpoint.json")
    return cfg


# --------------------------------------------------------------------------
# Rate limiter (sliding window, 30/min default)
# --------------------------------------------------------------------------

FILE_IO_MAX_RETRIES = 6
FILE_IO_RETRY_BASE_DELAY = 0.5  # seconds, doubles each attempt


def _retry_file_op(op_name: str, fn, *args, **kwargs):
    """Retry a file-system operation on transient OSError/PermissionError
    (e.g. Windows Defender or OneDrive briefly locking a just-created
    file). This is a transient environment issue, not a logic bug --
    treated the same way as a transient API error: short exponential
    backoff, then give up loudly rather than silently losing data."""
    attempt = 0
    while True:
        attempt += 1
        try:
            return fn(*args, **kwargs)
        except (PermissionError, OSError) as e:
            if attempt >= FILE_IO_MAX_RETRIES:
                log(f"{op_name} failed after {FILE_IO_MAX_RETRIES} attempts: {e}")
                raise
            delay = FILE_IO_RETRY_BASE_DELAY * (2 ** (attempt - 1))
            log(f"{op_name} hit a transient file error (attempt {attempt}): {e} "
                f"-- retrying in {delay:.1f}s (likely antivirus/OneDrive briefly "
                f"locking the file, not a real problem)")
            time.sleep(delay)


# --------------------------------------------------------------------------
# Checkpoint (crash-safe, byte-offset resume)
# --------------------------------------------------------------------------

def load_checkpoint(path: Path) -> dict:
    if not path.exists():
        return {"last_completed_page": 0, "csv_byte_offset": 0, "rows_written": 0}
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    data.setdefault("rows_written", 0)  # backward-compatible with older checkpoints
    return data


def save_checkpoint(path: Path, last_completed_page: int, csv_byte_offset: int, rows_written: int):
    tmp_path = path.with_suffix(path.suffix + ".tmp")

    def _write_and_replace():
        # Clean up a stale .tmp left over from a prior interrupted attempt
        # before writing a fresh one -- avoids a second-layer permission
        # trap on Windows where the old tmp file itself is locked.
        if tmp_path.exists():
            try:
                tmp_path.unlink()
            except OSError:
                pass
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump({
                "last_completed_page": last_completed_page,
                "csv_byte_offset": csv_byte_offset,
                "rows_written": rows_written,
            }, f, indent=2)
        os.replace(tmp_path, path)  # atomic on POSIX and Windows

    _retry_file_op("save_checkpoint", _write_and_replace)


def truncate_to_offset(csv_path: Path, offset: int):
    """Roll back any partially-written rows from a crash between the CSV
    append and the checkpoint save -- guarantees no duplicate/orphan rows."""
    if not csv_path.exists():
        return
    current_size = csv_path.stat().st_size
    if current_size > offset:
        def _truncate():
            with open(csv_path, "r+b") as f:
                f.truncate(offset)
        _retry_file_op("truncate_to_offset", _truncate)
        log(f"Rolled back CSV from {current_size} to {offset} bytes "
            f"(recovering from an interrupted previous run)")


# --------------------------------------------------------------------------
# CSV writing
# --------------------------------------------------------------------------

def ensure_csv_header(csv_path: Path):
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        def _write_header():
            with open(csv_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
                writer.writeheader()
        _retry_file_op("ensure_csv_header", _write_header)


def append_rows(csv_path: Path, rows: list) -> int:
    """Append rows, flush + fsync, return the new file size in bytes."""
    def _append():
        with open(csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
            writer.writerows(rows)
            f.flush()
            os.fsync(f.fileno())
        return csv_path.stat().st_size
    return _retry_file_op("append_rows", _append)




# --------------------------------------------------------------------------
# API call
# --------------------------------------------------------------------------

def fetch_page(session, base_url, apikey, orgid, filter_key, sort_order,
               start_date, end_date, per_page, page, rate_limiter):
    url = f"{base_url}/user/useranalyticslist"
    headers = {"apikey": apikey, "ORGID": str(orgid)}
    params = {
        "page": page,
        "per_page": per_page,
        "is_export": 0,
        "filter_key": filter_key,
        "sort_order": sort_order,
        "start_date": start_date,
        "end_date": end_date,
    }

    attempt = 0
    while attempt < MAX_RETRIES_PER_PAGE:
        attempt += 1
        rate_limiter.acquire()
        try:
            resp = session.get(url, headers=headers, params=params,
                                timeout=REQUEST_TIMEOUT_SECONDS)
        except requests.RequestException as e:
            log(f"  [page {page}] network error (attempt {attempt}): {e} -- retrying")
            time.sleep(min(5 * attempt, 30))
            continue

        if resp.status_code == 200:
            try:
                return resp.json()
            except ValueError:
                log(f"  [page {page}] 200 but non-JSON body -- treating as failed")
                return None

        if resp.status_code == 429:
            log(f"  [page {page}] 429 rate limited -- cooling down "
                f"{TRANSIENT_ERROR_COOLDOWN_SECONDS}s and resetting limiter")
            time.sleep(TRANSIENT_ERROR_COOLDOWN_SECONDS)
            rate_limiter.reset()
            continue

        if resp.status_code in PERMANENT_ERROR_CODES:
            log(f"  [page {page}] permanent error {resp.status_code}: "
                f"{resp.text[:200]}")
            return None

        log(f"  [page {page}] transient error {resp.status_code} "
            f"(attempt {attempt}): {resp.text[:200]}")
        time.sleep(min(5 * attempt, 30))

    log(f"  [page {page}] giving up after {MAX_RETRIES_PER_PAGE} attempts")
    return None


# --------------------------------------------------------------------------
# Main collection loop
# --------------------------------------------------------------------------

def run_collection(cfg: dict, script_dir: Path):
    base_url = cfg["base_url"].rstrip("/")
    output_dir = script_dir.parent / "output"
    csv_path = output_dir / cfg["output_csv"]
    checkpoint_path = output_dir / cfg["checkpoint_file"]

    checkpoint_existed = checkpoint_path.exists()
    ensure_csv_header(csv_path)
    checkpoint = load_checkpoint(checkpoint_path)

    if checkpoint_existed:
        # Resuming a prior run -- roll back any rows written after the
        # last confirmed-good checkpoint (crash between CSV append and
        # checkpoint save).
        truncate_to_offset(csv_path, checkpoint["csv_byte_offset"])
    else:
        # Genuinely fresh run -- the header was just written above, so
        # that's the correct starting offset. Do NOT truncate here, or
        # the header we just wrote gets wiped.
        checkpoint["csv_byte_offset"] = csv_path.stat().st_size

    rate_limiter = RollingRateLimiter(cfg["rate_limit_per_minute"], window_seconds=60.0)
    session = requests.Session()

    page = checkpoint["last_completed_page"] + 1
    csv_byte_offset = checkpoint["csv_byte_offset"]
    rows_written = checkpoint["rows_written"]  # cumulative, survives resume
    total_rows_seen = None

    log(f"Starting from page {page} (rate limit: "
        f"{cfg['rate_limit_per_minute']}/min, per_page: {cfg['per_page']}, "
        f"rows already written: {rows_written})")

    while True:
        log(f"Fetching page {page}...")
        data = fetch_page(
            session, base_url, cfg["apikey"], cfg["orgid"], cfg["filter_key"],
            cfg["sort_order"], cfg["start_date"], cfg["end_date"],
            cfg["per_page"], page, rate_limiter,
        )

        if data is None:
            log(f"STOPPED at page {page}: fetch failed after retries.")
            log(f"No rows from this page were written -- checkpoint stays at "
                f"page {checkpoint['last_completed_page']}, "
                f"{rows_written} rows written so far.")
            log(f"Re-run the script with the same config to resume from page {page}.")
            sys.exit(1)

        user_list = data.get("user_list", [])
        page_context = data.get("page_context", {})
        total_rows_seen = page_context.get("total_rows", total_rows_seen)
        has_more = page_context.get("has_more_page", False)

        rows = [{
            "user_id": u.get("_id"),
            "name": u.get("name", ""),
            "email": u.get("email", ""),
            "contact_number": u.get("contact_number", ""),
            "country": u.get("filterValue", ""),
            "region": u.get("regionName", ""),
            "time_spent_seconds": u.get("timeSpent", 0),
            "total_sessions": u.get("totalSessions", 0),
            "last_seen_epoch": u.get("lastSeen", ""),
            "last_seen_ist": epoch_to_ist_str(u.get("lastSeen", "")),
            "created_at_epoch": u.get("created_at", ""),
            "created_at_ist": epoch_to_ist_str(u.get("created_at", "")),
            "source_page": page,
        } for u in user_list]

        csv_byte_offset = append_rows(csv_path, rows)
        rows_written += len(rows)
        save_checkpoint(checkpoint_path, page, csv_byte_offset, rows_written)

        if total_rows_seen is not None:
            remaining = max(total_rows_seen - rows_written, 0)
            remaining_str = f"{remaining} rows remaining"
        else:
            remaining_str = "remaining unknown (no total_rows in response yet)"

        log(f"  [page {page}] wrote {len(rows)} rows "
            f"({rows_written} written so far, {remaining_str}, "
            f"has_more_page: {has_more})")

        if not has_more or not user_list:
            break
        page += 1

    log(f"Done. {csv_path} contains {rows_written} rows across pages 1-{page}.")
    return csv_path


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                      formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", type=str, default="ip_driven_country_data_config.json",
                         help="Path to config JSON")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = SCRIPT_DIR / config_path

    cfg = load_config(config_path)
    run_collection(cfg, SCRIPT_DIR)


if __name__ == "__main__":
    main()