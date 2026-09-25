#!/usr/bin/env python3
"""
ip_driven_country_data.py

Stage 1. Pulls the full per-user list from Edmingle's /user/useranalyticslist endpoint (paginated),
capturing user_id + name/email + country (filterValue) + region + activity metrics per row, and
writes output/user_country_list.csv.

last_seen and created_at are written both as raw epoch (*_epoch, for machine use) and as
dd-mm-yyyy HH:MM:SS IST strings (*_ist).

The whole pull is ~130 pages (about 5 minutes at 30 requests/minute), so there is no resume machinery:
rows are written to user_country_list.csv.part and the file is renamed onto user_country_list.csv only
when every page succeeded -- the previous file stays intact until then. If a page keeps failing the
script stops and nothing is replaced; just run it again.

Usage:
    python3 ip_driven_country_data.py --config ip_driven_country_data_config.json

The config holds filter_key, start_date and an optional end_date (default: the end of today, IST).
The API key, organization id and base URL come from ../../credentials.yaml.
"""

import argparse
import csv
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set before any local module import below.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import common
from common import RollingRateLimiter

import requests

SCRIPT_DIR = Path(__file__).resolve().parent
IST = ZoneInfo("Asia/Kolkata")

TRANSIENT_ERROR_COOLDOWN_SECONDS = 300  # known 429 penalty-state behavior
MAX_RETRIES_PER_PAGE = 5
REQUEST_TIMEOUT_SECONDS = 90
DEFAULTS = {"per_page": 50, "sort_order": -1, "rate_limit_per_minute": 30, "output_csv": "user_country_list.csv"}

CSV_FIELDS = [
    "user_id", "name", "email", "contact_number",
    "country", "region", "time_spent_seconds", "total_sessions",
    "last_seen_epoch", "last_seen_ist",
    "created_at_epoch", "created_at_ist",
    "source_page",
]


def log(msg: str):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}")


_LOG = SimpleNamespace(warning=log, error=log)  # the logger shape common.get_json expects


def epoch_to_ist_str(epoch_value) -> str:
    """A unix epoch (seconds) as dd-mm-yyyy HH:MM:SS IST; '' for a missing/invalid value."""
    try:
        return datetime.fromtimestamp(int(float(epoch_value)), tz=IST).strftime("%d-%m-%Y %H:%M:%S")
    except (TypeError, ValueError):
        return ""


def today_end_ist() -> str:
    """End of today (IST) in the API's date format -- the default end_date."""
    return datetime.now(IST).strftime("%d-%m-%YT23:59:59+05:30")


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        sys.exit(f"Config file not found: {config_path}\nExpected {config_path.name} next to this script, or pass --config.")
    cfg = json.loads(config_path.read_text(encoding="utf-8"))
    missing = [key for key in ("filter_key", "start_date") if not cfg.get(key)]  # end_date is optional
    if missing:
        sys.exit(f"Config is missing required key(s): {', '.join(missing)}")
    edmingle = common.edmingle_settings()
    if not edmingle["api_key"]:
        sys.exit("No Edmingle api_key in credentials.yaml -- edit that shared credentials file first.")
    cfg.update(apikey=edmingle["api_key"], orgid=edmingle["organization_id"], base_url=edmingle["base_url"])
    for key, default in DEFAULTS.items():
        cfg.setdefault(key, default)
    return cfg


def fetch_page(session, cfg, end_date, page, rate_limiter):
    """One page of /user/useranalyticslist through common.get_json (up to MAX_RETRIES_PER_PAGE attempts, a
    TRANSIENT_ERROR_COOLDOWN_SECONDS wait on 429), or None if it kept failing."""
    params = {
        "page": page,
        "per_page": cfg["per_page"],
        "is_export": 0,
        "filter_key": cfg["filter_key"],
        "sort_order": cfg["sort_order"],
        "start_date": cfg["start_date"],
        "end_date": end_date,
    }
    try:
        return common.get_json(
            f"{cfg['base_url']}/user/useranalyticslist", headers={"apikey": cfg["apikey"], "ORGID": str(cfg["orgid"])},
            params=params, session=session, timeout=REQUEST_TIMEOUT_SECONDS, attempts=MAX_RETRIES_PER_PAGE, delay=5,
            max_delay=30, block_seconds=TRANSIENT_ERROR_COOLDOWN_SECONDS, rate_limiter=rate_limiter,
            label=f"  [page {page}]", logger=_LOG,
        )
    except common.ApiError as error:
        log(f"  [page {page}] giving up: {error}")
        return None


def run_collection(cfg: dict) -> Path:
    csv_path = SCRIPT_DIR.parent / "output" / cfg["output_csv"]
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    part = csv_path.with_name(csv_path.name + ".part")
    end_date = cfg.get("end_date") or today_end_ist()
    log(f"Date window: {cfg['start_date']} -> {end_date} (rate limit {cfg['rate_limit_per_minute']}/min, per_page {cfg['per_page']})")

    rate_limiter = RollingRateLimiter(cfg["rate_limit_per_minute"], window_seconds=60.0)
    session = requests.Session()
    rows_written, page = 0, 1

    with part.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS)
        writer.writeheader()
        while True:
            log(f"Fetching page {page}...")
            data = fetch_page(session, cfg, end_date, page, rate_limiter)
            if data is None:
                log(f"STOPPED at page {page}: fetch failed after retries. {csv_path.name} was not replaced -- run it again.")
                fh.close()
                part.unlink(missing_ok=True)
                sys.exit(1)

            user_list = data.get("user_list", [])
            page_context = data.get("page_context", {})
            writer.writerows({
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
            } for u in user_list)
            rows_written += len(user_list)

            total = page_context.get("total_rows")
            remaining = f"{max(total - rows_written, 0)} rows remaining" if total is not None else "remaining unknown"
            log(f"  [page {page}] wrote {len(user_list)} rows ({rows_written} so far, {remaining})")

            if not page_context.get("has_more_page", False) or not user_list:
                break
            page += 1
        fh.flush()
        os.fsync(fh.fileno())

    os.replace(part, csv_path)
    log(f"Done. {csv_path} contains {rows_written} rows across pages 1-{page}.")
    return csv_path


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", type=str, default="ip_driven_country_data_config.json", help="Path to config JSON")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = SCRIPT_DIR / config_path
    run_collection(load_config(config_path))


if __name__ == "__main__":
    main()
