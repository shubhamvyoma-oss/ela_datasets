"""
attendance_crossvalidation.py
--------------------------------------------------------------------------
Cross-validates report_type=55 attendance figures against the
/organization/attendances endpoint (session-level total/present/not_marked).

WHY THIS EXISTS
----------------
report_type=55 gives student-level marks (P/A/-) that you sum per session.
/organization/attendances gives Edmingle's OWN pre-aggregated total/present
per session, keyed by "id" (the session identifier — need to confirm this
maps 1:1 to your `attendance_id`).

If the two disagree, that's a real signal: either report_type=55 is missing
some student rows for a session, or "total" in this endpoint means something
different from "enrolled at session time" (e.g. cumulative bundle enrollment
rather than batch-at-that-date enrollment). Don't assume either source is
right until you've run this against a few known batches.

USAGE
-----
    python attendance_crossvalidation.py --class_id 199222 --start 2023-09-21 --end 2026-08-24

Reads credentials from config.yaml (same file as master_attendance_pipeline.py).
Writes a CSV with one row per session, plus a summary printed to stdout.

INTEGRATION NOTE
-----------------
This is deliberately standalone (does not import master_attendance_pipeline.py)
so you can run it independently first and eyeball the output before wiring it
into the main pipeline as a --validate flag or a scheduled post-run check.
"""

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml

CONFIG_PATH = Path(__file__).parent / "config.yaml"
BASE_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1"  # matches your other calls
ORG_ATTENDANCES_ENDPOINT = f"{BASE_URL}/organization/attendances"


def load_config(config_path: Path = CONFIG_PATH) -> dict:
    if not config_path.exists():
        print(f"[WARN] {config_path} not found — falling back to CLI-only mode "
              f"(pass --apikey explicitly).")
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def to_unix(date_str: str) -> int:
    """Parse YYYY-MM-DD as IST midnight -> unix timestamp.
    Mirrors the IST-boundary lesson learned from report_type=55."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    # IST is UTC+5:30; treat input date as IST midnight
    ist_offset_seconds = 5.5 * 3600
    utc_dt = dt.replace(tzinfo=timezone.utc)
    return int(utc_dt.timestamp() - ist_offset_seconds)


def fetch_org_attendances(apikey: str, org_id: int, start_ts: int, end_ts: int,
                           class_id: int = None, max_retries: int = 3) -> list:
    """Calls /organization/attendances. class_id is optional — omit for
    an org-wide pull across the date window (heavier, use with caution
    on wide date ranges)."""
    params = {
        "org_id": org_id,
        "apikey": apikey,
        "start": start_ts,
        "end": end_ts,
    }
    if class_id is not None:
        params["class_id"] = class_id

    # This endpoint's docs show apikey/orgid as HEADERS (unlike report_type=55,
    # which is query-param-only) — send both to cover whichever it actually checks.
    headers = {
        "apikey": apikey,
        "orgid": str(org_id),
        "ORGID": str(org_id),
    }

    print(f"[DEBUG] GET {ORG_ATTENDANCES_ENDPOINT} params={ {k: v for k, v in params.items() if k != 'apikey'} } "
          f"apikey=***{apikey[-4:] if apikey and len(apikey) > 4 else '????'}")

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(ORG_ATTENDANCES_ENDPOINT, params=params, headers=headers, timeout=30)
            if resp.status_code >= 400:
                # Print the actual response body — Edmingle's 400s usually explain
                # exactly which param it didn't like, don't guess from status code alone.
                print(f"[HTTP {resp.status_code}] Response body: {resp.text[:1000]}")
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") != 200:
                print(f"[WARN] API returned non-200 code: {data.get('code')} "
                      f"message={data.get('message')}")
                return []
            return data.get("classes", [])
        except requests.exceptions.RequestException as e:
            print(f"[RETRY {attempt}/{max_retries}] request failed: {e}")
            if attempt < max_retries:
                time.sleep(2 * attempt)
    print("[ERROR] all retries exhausted, returning empty result")
    return []


IST_OFFSET_SECONDS = 5.5 * 3600


def unix_to_ist(ts, fmt: str = "%Y-%m-%d %H:%M:%S"):
    """Convert a UTC unix timestamp to an IST-formatted string.
    Manual +5:30 offset (not zoneinfo/pytz) to avoid tzdata dependency issues
    on Windows — consistent with the IST-boundary handling already used
    elsewhere in the pipeline for report_type=55."""
    if ts is None:
        return None
    dt = datetime.fromtimestamp(ts + IST_OFFSET_SECONDS, tz=timezone.utc)
    return dt.strftime(fmt)


def sessions_to_dataframe(classes: list) -> pd.DataFrame:
    """Extract the session-wise fields relevant to cross-validation and reporting.
    ALL date/time fields below are converted to IST before being written out —
    the raw fields from Edmingle (class_date, gmt_start_time, gmt_end_time) are
    UTC unix timestamps and are NOT written to the CSV directly, only their
    IST-converted counterparts.

    Field mapping (from empirical exploration against the classroom UI):
      id                 -> session identifier (VERIFY vs your attendance_id before joining)
      class_id           -> subject/stream id (overloaded field — NOT the batch key)
      class_name          -> subject/stream display name
      master_batch_id     -> ACTUAL batch id — use this to join against course_batch_merge.csv
      master_batch_name   -> batch display name (note: often has a leading space in source data)
      class_date          -> IST calendar date, derived from unix timestamp + 5:30
      gmt_start_time/end  -> converted to IST session_start_ist / session_end_ist datetimes
      total                -> enrollment count AT THAT SESSION (matches UI denominator)
      present              -> present count (matches UI numerator)
      not_marked           -> total - present - absent, roughly
      taken_by_name        -> tutor who conducted the session
      signin_by_name        -> tutor who signed in
      signout_by_name       -> tutor who signed out
      individual_batch_attendance -> 0/1 flag: whether attendance is tracked per-individual-batch
                                      vs shared/broadcast across batches (relevant to your
                                      cross-batch broadcast session detection work)
    """
    rows = []
    for c in classes:
        gmt_start = c.get("gmt_start_time")
        gmt_end = c.get("gmt_end_time")
        rows.append({
            "session_id": c.get("id"),
            "class_id": c.get("class_id"),
            "class_name": c.get("class_name"),
            "master_batch_id": c.get("master_batch_id"),
            "master_batch_name": (
                c.get("master_batch_name").strip()
                if isinstance(c.get("master_batch_name"), str) else c.get("master_batch_name")
            ),
            "class_date": unix_to_ist(c.get("class_date"), "%Y-%m-%d"),
            "session_start_ist": unix_to_ist(gmt_start),
            "session_end_ist": unix_to_ist(gmt_end),
            "session_duration_min": (
                round((gmt_end - gmt_start) / 60, 1) if gmt_start and gmt_end else None
            ),
            "total_enrolled_at_session": c.get("total"),
            "present": c.get("present"),
            "not_marked": c.get("not_marked"),
            "attendance_pct": (
                round(100 * c["present"] / c["total"], 2)
                if c.get("total") else None
            ),
            "taken_by_name": c.get("taken_by_name"),
            "signin_by_name": c.get("signin_by_name"),
            "signout_by_name": c.get("signout_by_name"),
            "individual_batch_attendance": c.get("individual_batch_attendance"),
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["session_id"] = df["session_id"].astype("Int64")
        df["class_id"] = df["class_id"].astype("Int64")
        df["master_batch_id"] = df["master_batch_id"].astype("Int64")
        # Sort by actual IST session start rather than the (now-removed) raw unix column
        df = df.sort_values("session_start_ist")
    return df


def cross_validate_against_report55(org_att_df: pd.DataFrame,
                                     report55_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Merges org-attendances session totals against your existing report_type=55
    aggregated per-session P/A/- counts.

    report55_df is expected to have (at minimum) these columns, matching your
    existing pipeline's output before it collapses to summary stats:
        - attendance_id  (your confirmed correct session identifier)
        - present_count  (count of studentAttendanceStatus == "P")
        - absent_count   (count of studentAttendanceStatus == "A")
        - not_marked_count (count of studentAttendanceStatus == "-")
        - total_marked   (present_count + absent_count + not_marked_count)

    If you don't have this shaped yet, this function just returns the
    org_att_df with a placeholder — wire in your actual report_type=55
    session-level dataframe from master_attendance_pipeline.py and re-run.
    """
    if report55_df is None:
        print("[INFO] No report_type=55 dataframe passed — skipping merge, "
              "returning org_attendances data only.")
        org_att_df["report55_present"] = pd.NA
        org_att_df["present_diff"] = pd.NA
        return org_att_df

    merged = org_att_df.merge(
        report55_df[["attendance_id", "present_count", "total_marked"]],
        left_on="session_id",
        right_on="attendance_id",
        how="left",
        suffixes=("", "_r55"),
    )
    merged["present_diff"] = merged["present"] - merged["present_count"]
    merged["total_diff"] = merged["total_enrolled_at_session"] - merged["total_marked"]

    mismatches = merged[merged["present_diff"].abs() > 0]
    print(f"[SUMMARY] {len(merged)} sessions compared, {len(mismatches)} with "
          f"present-count mismatch between /organization/attendances and report_type=55")
    return merged


def main():
    parser = argparse.ArgumentParser(description="Cross-validate attendance sources")
    parser.add_argument("--class_id", type=int, default=None,
                         help="Subject/stream class_id to filter (omit for org-wide pull)")
    parser.add_argument("--start", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--apikey", type=str, default=None,
                         help="Override config.yaml api key")
    parser.add_argument("--out", type=str, default="org_attendances_sessions.csv")
    args = parser.parse_args()

    config = load_config()
    apikey = args.apikey or config.get("api_key") or config.get("apikey")
    org_id = config.get("org_id", 683)

    # Resolve output_folder relative to THIS SCRIPT's location, not the
    # process's current working directory — matters when launched via
    # run_pipeline.bat / Task Scheduler where cwd may not be the script folder.
    raw_output_folder = config.get("output_folder", ".")
    output_folder = Path(raw_output_folder)
    if not output_folder.is_absolute():
        output_folder = (Path(__file__).parent / output_folder).resolve()
    output_folder.mkdir(parents=True, exist_ok=True)

    if not apikey:
        print("[ERROR] No API key found. Pass --apikey or set api_key in config.yaml")
        sys.exit(1)

    start_ts = to_unix(args.start)
    end_ts = to_unix(args.end)

    print(f"[INFO] Fetching /organization/attendances "
          f"class_id={args.class_id or '(all)'} "
          f"start={args.start} end={args.end}")

    classes = fetch_org_attendances(apikey, org_id, start_ts, end_ts, args.class_id)
    df = sessions_to_dataframe(classes)

    if df.empty:
        print("[WARN] No sessions returned. Check class_id / date range / credentials.")
        return

    df = df.reset_index(drop=True)
    out_path = output_folder / args.out
    df.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"\n[RESULT] {len(df)} sessions written to {out_path.resolve()}\n")
    print(df[["class_date", "session_start_ist", "session_end_ist", "class_name",
              "master_batch_id", "master_batch_name",
              "total_enrolled_at_session", "present", "not_marked", "attendance_pct",
              "taken_by_name", "individual_batch_attendance"]].to_string(index=False))

    if not df.empty:
        first = df.iloc[0]
        print(f"\n[FIRST SESSION] {first['class_date']}: "
              f"{first['present']} present out of {first['total_enrolled_at_session']} "
              f"enrolled ({first['attendance_pct']}%)")


if __name__ == "__main__":
    main()