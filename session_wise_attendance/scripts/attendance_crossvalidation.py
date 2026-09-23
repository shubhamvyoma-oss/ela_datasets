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

Reads credentials from the shared ../../credentials.yaml (shared with every other ela_datasets/ pipeline).
Writes a CSV with one row per session, plus a summary printed to stdout.

INTEGRATION NOTE
-----------------
This is deliberately standalone (does not import build_course_catalog.py)
so you can run it independently first and eyeball the output. It also
supplies fetch_org_attendances/sessions_to_dataframe to
build_session_attendance.py (Stage 3's bulk pull), so the two scripts can't
drift on session-shaping/status-classification logic.
"""

import argparse
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set
# before any local module import below, so this and every module it pulls
# in gets compiled into one shared location instead of a scripts/__pycache__
# folder per pipeline.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

import pandas as pd
import requests

# Windows console/redirect default (cp1252) can't encode many batch-name
# characters (em-dashes, curly quotes, etc.) — force UTF-8 stdout so a
# print() never crashes the run mid-way through.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent))
from pipeline_common import (
    load_config, parse_retry_after_seconds, resolve_output_folder,
    PipelineRunLogger, send_run_report,
)

STAGE_NAME = "attendance_crossvalidation"
SCRIPT_DIR = Path(__file__).parent
BASE_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1"  # matches your other calls
ORG_ATTENDANCES_ENDPOINT = f"{BASE_URL}/organization/attendances"


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
    on wide date ranges).

    RATE LIMIT: Edmingle allows max 30 calls/min. On a 429, this waits out
    Edmingle's own reported block duration (parsed from the response
    message) rather than retrying quickly, which just wastes retries
    during an active block."""
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

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(ORG_ATTENDANCES_ENDPOINT, params=params, headers=headers, timeout=30)

            if resp.status_code == 429:
                wait_seconds = parse_retry_after_seconds(resp.text)
                print(f"\n[RATE LIMIT] class_id={class_id}: 429 received. "
                      f"Waiting {wait_seconds/60:.1f} min before retrying "
                      f"(attempt {attempt}/{max_retries})...")
                time.sleep(wait_seconds)
                continue

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

# Class-level status codes (0-7) confirmed against the classroom UI — see
# RULES.md § Stage 3 for the full code->label table and the conducted/
# not-conducted rationale. Only the not-conducted set is needed here.
NOT_CONDUCTED_STATUSES = {2, 3}  # Postponed, Cancelled

ATTENDANCE_DET_ENDPOINT = f"{BASE_URL}/bundle/general/attendancedet"


def fetch_attendance_summary(apikey: str, class_id: int, start_date: str, end_date: str,
                              max_retries: int = 3) -> dict:
    """Calls /bundle/general/attendancedet — Edmingle's own aggregated
    scheduled/cancelled/signed-in counts for a class_id. This is the
    'planned vs happened' summary at a glance, separate from the
    session-by-session detail in /organization/attendances.
    Dates must be ISO 8601 (YYYY-MM-DDTHH:MM:SSZ)."""
    params = {
        "apikey": apikey,
        "start_date": f"{start_date}T00:00:00Z",
        "end_date": f"{end_date}T23:59:59Z",
        "top": 1,
        "class_id": class_id,
    }
    headers = {"apikey": apikey}

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(ATTENDANCE_DET_ENDPOINT, params=params, headers=headers, timeout=30)

            if resp.status_code == 429:
                wait_seconds = parse_retry_after_seconds(resp.text)
                print(f"\n[RATE LIMIT] attendancedet class_id={class_id}: 429 received. "
                      f"Waiting {wait_seconds/60:.1f} min before retrying...")
                time.sleep(wait_seconds)
                continue

            if resp.status_code >= 400:
                print(f"[HTTP {resp.status_code}] attendancedet response: {resp.text[:500]}")
            resp.raise_for_status()
            data = resp.json()
            return data.get("avg_attendance_data", {})
        except requests.exceptions.RequestException as e:
            print(f"[RETRY {attempt}/{max_retries}] attendancedet request failed: {e}")
            if attempt < max_retries:
                time.sleep(2 * attempt)
    print("[WARN] Could not fetch attendancedet summary — continuing without it.")
    return {}


def unix_to_ist(ts, fmt: str = "%Y-%m-%d %H:%M:%S"):
    """Convert a UTC unix timestamp to an IST-formatted string.
    Manual +5:30 offset (not zoneinfo/pytz) to avoid tzdata dependency issues
    on Windows — consistent with the IST-boundary handling already used
    elsewhere in the pipeline for report_type=55."""
    if ts is None:
        return None
    dt = datetime.fromtimestamp(ts + IST_OFFSET_SECONDS, tz=timezone.utc)
    return dt.strftime(fmt)


# Base session columns, in the order they should appear in any CSV built
# from sessions_to_dataframe. build_session_attendance.py (Stage 3) extends
# this with bundle_id/bundle_name, which only it has access to via the
# class_id lookup file.
SESSION_BASE_COLUMNS = [
    "session_id", "class_id", "class_name", "master_batch_id", "master_batch_name",
    "class_date", "total_enrolled_at_session", "present", "not_marked", "attendance_pct",
    "taken_by_name", "individual_batch_attendance",
    "session_start_ist", "session_end_ist", "session_duration_min",
    "session_conducted", "session_number",
]


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
      master_batch_id     -> ACTUAL batch id — use this to join against course_catalog.csv
      master_batch_name   -> batch display name (note: often has a leading space in source data)
      class_date          -> IST calendar date, derived from unix timestamp + 5:30
      gmt_start_time/end  -> converted to IST session_start_ist / session_end_ist datetimes
      total                -> enrollment count AT THAT SESSION (matches UI denominator)
      present              -> present count (matches UI numerator)
      not_marked           -> total - present - absent, roughly
      taken_by_name        -> tutor who conducted the session
      individual_batch_attendance -> 0/1 flag: whether attendance is tracked per-individual-batch
                                      vs shared/broadcast across batches (relevant to your
                                      cross-batch broadcast session detection work)
      status                -> raw Edmingle status code, used only to derive session_conducted
                                (see RULES.md § Stage 3 for the full code table) — not written out
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
            "individual_batch_attendance": c.get("individual_batch_attendance"),
            "session_conducted": c.get("status") not in NOT_CONDUCTED_STATUSES,
        })
    df = pd.DataFrame(rows)
    if not df.empty:
        df["session_id"] = df["session_id"].astype("Int64")
        df["class_id"] = df["class_id"].astype("Int64")
        df["master_batch_id"] = df["master_batch_id"].astype("Int64")
        # Sort by actual IST session start rather than the (now-removed) raw unix column
        df = df.sort_values("session_start_ist").reset_index(drop=True)
        # session_number = sequential count within each batch, in chronological order.
        # Every row returned by the API for a given class_id/master_batch_id counts as
        # a PLANNED session (Edmingle only returns scheduled slots) — session_conducted
        # tells you whether that planned slot actually happened.
        df["session_number"] = df.groupby("master_batch_id").cumcount() + 1
        df = df[SESSION_BASE_COLUMNS]
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
    session-level dataframe and re-run.
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
                         help="Override the api_key from ../../credentials.yaml")
    parser.add_argument("--out", type=str, default=None,
                         help="Defaults to attendance_spotcheck.csv")
    args = parser.parse_args()

    config = load_config(SCRIPT_DIR)
    apikey = args.apikey or config.get("api_key") or config.get("apikey")
    org_id = config.get("org_id", 683)
    out_filename = args.out or "attendance_spotcheck.csv"

    output_folder = resolve_output_folder(config, Path(__file__))

    if not apikey:
        print("[ERROR] No API key found. Pass --apikey or set edmingle.api_key in ../../credentials.yaml")
        sys.exit(1)

    start_ts = to_unix(args.start)
    end_ts = to_unix(args.end)

    n_sessions = 0
    n_errors = 0
    start_time = datetime.now()
    out_path = output_folder / out_filename

    try:
        print(f"[INFO] Fetching /organization/attendances "
              f"class_id={args.class_id or '(all)'} "
              f"start={args.start} end={args.end}")

        classes = fetch_org_attendances(apikey, org_id, start_ts, end_ts, args.class_id)
        df = sessions_to_dataframe(classes)

        if df.empty:
            print("[WARN] No sessions returned. Check class_id / date range / credentials.")
            n_errors += 1
            return

        df = df.reset_index(drop=True)
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        n_sessions = len(df)

        print(f"\n[RESULT] {n_sessions} sessions written to {out_path.resolve()}\n")
        print(df[["session_number", "class_date", "session_start_ist", "session_end_ist",
                  "class_name", "master_batch_id", "session_conducted",
                  "total_enrolled_at_session", "present", "not_marked", "attendance_pct"]].to_string(index=False))

        # --- Planned vs. conducted, derived per-session from /organization/attendances ---
        n_planned = len(df)
        n_conducted = int(df["session_conducted"].sum())
        n_not_conducted = n_planned - n_conducted
        print(f"\n[PLANNED VS CONDUCTED — from /organization/attendances session statuses]")
        print(f"  Planned (all scheduled slots returned): {n_planned}")
        print(f"  Conducted (status not Postponed/Cancelled): {n_conducted}")
        print(f"  Not conducted (Postponed/Cancelled): {n_not_conducted}")

        # --- Cross-check against Edmingle's own aggregate summary, if class_id was given ---
        if args.class_id is not None:
            summary_det = fetch_attendance_summary(apikey, args.class_id, args.start, args.end)
            if summary_det:
                scheduled = summary_det.get("sessions_scheduled")
                cancelled = summary_det.get("sessions_cancelled")
                print(f"\n[PLANNED VS CONDUCTED — from /bundle/general/attendancedet aggregate]")
                print(f"  sessions_scheduled: {scheduled}")
                print(f"  sessions_cancelled: {cancelled}")
                if scheduled is not None and cancelled is not None:
                    print(f"  Implied conducted (scheduled - cancelled): {scheduled - cancelled}")
                print(f"  total_signed_ins: {summary_det.get('total_signed_ins')}  "
                      f"in_time_signins: {summary_det.get('in_time_signins')}  "
                      f"avg_attendance: {summary_det.get('avg_attendance')}")
                if scheduled is not None and scheduled != n_planned:
                    print(f"  [NOTE] attendancedet sessions_scheduled ({scheduled}) != "
                          f"session count from organization/attendances ({n_planned}) — "
                          f"date ranges or class_id scope may differ, worth checking.")

        if not df.empty:
            first = df.iloc[0]
            print(f"\n[FIRST SESSION] {first['class_date']}: "
                  f"{first['present']} present out of {first['total_enrolled_at_session']} "
                  f"enrolled ({first['attendance_pct']}%)")

    except Exception as e:
        print(f"[ERROR] {e}")
        n_errors += 1
    finally:
        end_time = datetime.now()
        summary = {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_sec": round((end_time - start_time).total_seconds(), 1),
            "sessions_written": n_sessions,
            "errors": n_errors,
            "output_file": str(out_path),
        }
        send_run_report(STAGE_NAME, summary, config)


if __name__ == "__main__":
    with PipelineRunLogger(STAGE_NAME, Path(__file__), args_summary=str(sys.argv[1:])):
        main()
