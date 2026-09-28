"""
build_session_attendance.py
--------------------------------------------------------------------------
STAGE 3. Reads class_id_lookup.csv (from resolve_class_ids.py) and pulls
session-wise attendance for EVERY class_id in it via /organization/attendances.
Output: session_wise_attendance_data.csv. See SESSION_WISE_ATTENDANCE.md for the
session-status->conducted mapping, IST conversion rule, and session_number
numbering rule.

RATE LIMITING (Edmingle allows max 30 calls/min):
  - Calls spaced at a safe ~24/min by default (--calls_per_minute to tune).
  - fetch_org_attendances handles 429s by waiting out Edmingle's own reported
    block duration (see pipeline_common.get_json).

CHECKPOINT / RESUME:
  - Every class_id's resolved session rows are appended to the output CSV
    IMMEDIATELY, not held in memory. A block, crash, or Ctrl+C loses
    nothing already pulled.
  - On startup, class_ids already present in the output CSV are skipped —
    just rerun the same command to resume. Use --restart to start clean.

USAGE
-----
    python build_session_attendance.py --start 2010-01-01 --end 2026-08-24
    python build_session_attendance.py --start 2010-01-01 --end 2026-08-24 --limit 10
    python build_session_attendance.py --start 2010-01-01 --end 2026-08-24 --restart
"""

import argparse
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

# Shared bytecode cache across every ela_datasets/ pipeline -- must be set before any local import.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

import pandas as pd

# Windows console/redirect default (cp1252) can't encode many batch-name
# characters (em-dashes, curly quotes, etc.) — force UTF-8 stdout so a
# print() never crashes the run mid-way through.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent))
from pipeline_common import (
    BASE_URL,
    ApiError,
    PipelineRunLogger,
    RateLimiter,
    append_rows_to_csv,
    auth_headers,
    get_json,
    load_config,
    load_processed_ids,
    require_config,
    resolve_output_folder,
    send_run_report,
)

STAGE_NAME = "build_session_attendance"
SCRIPT_DIR = Path(__file__).parent
DEFAULT_CALLS_PER_MINUTE = 24  # safety margin under Edmingle's 30/min limit
ORG_ATTENDANCES_ENDPOINT = f"{BASE_URL}/organization/attendances"
IST_OFFSET_SECONDS = 5.5 * 3600

# Class-level status codes (0-7) confirmed against the classroom UI; only the
# not-conducted set is needed here.
NOT_CONDUCTED_STATUSES = {2, 3}  # Postponed, Cancelled

# Base session columns, plus bundle_id/bundle_name (sourced from class_id_lookup.csv, this
# stage's own input) inserted just before the date fields.
SESSION_BASE_COLUMNS = [
    "session_id", "class_id", "class_name", "master_batch_id", "master_batch_name",
    "class_date", "total_enrolled_at_session", "present", "not_marked", "attendance_pct",
    "taken_by_name", "individual_batch_attendance",
    "session_start_ist", "session_end_ist", "session_duration_min",
    "session_conducted", "session_number",
]
_split_at = SESSION_BASE_COLUMNS.index("class_date")
MASTER_OUTPUT_COLUMNS = (
    SESSION_BASE_COLUMNS[:_split_at] + ["bundle_id", "bundle_name"] + SESSION_BASE_COLUMNS[_split_at:]
)


def to_unix(date_str: str) -> int:
    """Parse YYYY-MM-DD as IST midnight -> unix timestamp."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    ist_offset_seconds = 5.5 * 3600
    utc_dt = dt.replace(tzinfo=UTC)
    return int(utc_dt.timestamp() - ist_offset_seconds)


def unix_to_ist(ts, fmt: str = "%Y-%m-%d %H:%M:%S"):
    """Convert a UTC unix timestamp to an IST-formatted string.
    Manual +5:30 offset (not zoneinfo/pytz) to avoid tzdata dependency issues
    on Windows."""
    if ts is None:
        return None
    dt = datetime.fromtimestamp(ts + IST_OFFSET_SECONDS, tz=UTC)
    return dt.strftime(fmt)


def fetch_org_attendances(apikey: str, org_id: int, start_ts: int, end_ts: int,
                           class_id: int = None, max_retries: int = 3) -> list:
    """Calls /organization/attendances. class_id is optional — omit for an org-wide pull across the date
    window (heavier, use with caution on wide date ranges). Returns [] if the call keeps failing.
    RATE LIMIT: on a 429 this waits out Edmingle's own reported block duration (pipeline_common.get_json)."""
    params = {"org_id": org_id, "start": start_ts, "end": end_ts}
    if class_id is not None:
        params["class_id"] = class_id
    try:
        data = get_json(ORG_ATTENDANCES_ENDPOINT, auth_headers(apikey, org_id), params,
                        attempts=max_retries, label=f"class_id={class_id}")
    except ApiError as error:
        print(f"[ERROR] {error} -- returning empty result")
        return []
    if data.get("code") != 200:
        print(f"[WARN] API returned non-200 code: {data.get('code')} message={data.get('message')}")
        return []
    return data.get("classes", [])


def sessions_to_dataframe(classes: list) -> pd.DataFrame:
    """Extract the session-wise fields relevant to reporting. ALL date/time fields below are
    converted to IST before being written out -- the raw fields from Edmingle (class_date,
    gmt_start_time, gmt_end_time) are UTC unix timestamps and are NOT written to the CSV directly,
    only their IST-converted counterparts.

    Field mapping (from empirical exploration against the classroom UI):
      id                 -> session identifier
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
                                      vs shared/broadcast across batches
      status                -> raw Edmingle status code, used only to derive session_conducted
                                (not written out)
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
        # Sequential count within each batch, chronological. Every API row is a PLANNED
        # session (Edmingle only returns scheduled slots) -- session_conducted says whether it happened.
        df["session_number"] = df.groupby("master_batch_id").cumcount() + 1
        df = df[SESSION_BASE_COLUMNS]
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Pull session-wise attendance for every class_id in class_id_lookup.csv (Stage 3)")
    parser.add_argument("--in", dest="input_file", type=str, default="class_id_lookup.csv")
    parser.add_argument("--start", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--end", type=str, required=True, help="YYYY-MM-DD")
    parser.add_argument("--out", type=str, default="session_wise_attendance_data.csv")
    parser.add_argument("--limit", type=int, default=None,
                         help="Only process the first N NEW class_ids (after skipping already-done ones)")
    parser.add_argument("--calls_per_minute", type=float, default=DEFAULT_CALLS_PER_MINUTE)
    parser.add_argument("--restart", action="store_true",
                         help="Ignore existing progress and start fresh (overwrites previous output)")
    parser.add_argument("--apikey", type=str, default=None)
    args = parser.parse_args()

    config = load_config(SCRIPT_DIR)
    apikey = args.apikey or config.get("api_key")
    org_id = require_config(config, "org_id")

    output_folder = resolve_output_folder(config, Path(__file__))

    if not apikey:
        print("[ERROR] No API key found. Pass --apikey or set edmingle.api_key in ../../credentials.yaml")
        sys.exit(1)

    input_path = Path(args.input_file)
    if not input_path.is_absolute() and not input_path.exists():
        candidate = output_folder / args.input_file
        if candidate.exists():
            input_path = candidate
    if not input_path.exists():
        print(f"[ERROR] Input file not found: {input_path} (also checked {output_folder})")
        sys.exit(1)

    lookup_df = pd.read_csv(input_path, encoding="utf-8-sig")
    print(f"[INFO] Loaded {len(lookup_df)} rows from {input_path}")

    required_cols = {"class_id", "bundle_id", "bundle_name", "batch_id", "batch_name"}
    missing_cols = required_cols - set(lookup_df.columns)
    if missing_cols:
        print(f"[ERROR] Missing expected columns: {missing_cols}. "
              f"Available: {list(lookup_df.columns)}")
        sys.exit(1)

    unresolved = lookup_df["class_id"].isna().sum()
    if unresolved:
        print(f"[NOTE] Skipping {unresolved} rows with no resolved class_id "
              f"(self-paced/archived content with no attendance-trackable subject).")
    lookup_df = lookup_df.dropna(subset=["class_id"]).drop_duplicates(subset=["class_id"])

    out_path = output_folder / args.out

    if args.restart and out_path.exists():
        out_path.unlink()
        print(f"[INFO] --restart: removed existing {out_path}, starting fresh.")

    already_processed = load_processed_ids(out_path, "class_id")
    if already_processed:
        print(f"[INFO] Resuming: {len(already_processed)} class_ids already pulled in "
              f"{out_path.name}, skipping those.")
        lookup_df = lookup_df[~lookup_df["class_id"].astype(int).isin(already_processed)]

    if args.limit:
        lookup_df = lookup_df.head(args.limit)

    n_classes = len(lookup_df)
    start_time = datetime.now()
    n_no_sessions = 0
    n_errors = 0
    n_rows_written = 0

    if n_classes == 0:
        print("[INFO] Nothing left to process — all class_ids already pulled.")
    else:
        limiter = RateLimiter(args.calls_per_minute)
        est_seconds = n_classes * limiter.delay_seconds
        print(f"[INFO] Pulling attendance for {n_classes} remaining class_ids "
              f"at {args.calls_per_minute:.0f} calls/min "
              f"(rough estimate: ~{est_seconds/60:.1f} min, before any rate-limit waits)")

        start_ts = to_unix(args.start)
        end_ts = to_unix(args.end)

        for i, row in enumerate(lookup_df.itertuples(index=False), 1):
            class_id = int(row.class_id)
            print(f"  [{i}/{n_classes}] class_id={class_id} "
                  f"(batch_id={row.batch_id}, {row.batch_name})")

            limiter.start()
            try:
                classes = fetch_org_attendances(apikey, org_id, start_ts, end_ts, class_id)
            except Exception as e:
                print(f"    [ERROR] fetch failed for class_id={class_id}: {e}")
                n_errors += 1
                classes = []

            if classes:
                session_df = sessions_to_dataframe(classes)
                if not session_df.empty:
                    session_df["bundle_id"] = row.bundle_id
                    session_df["bundle_name"] = row.bundle_name
                    session_df = session_df[MASTER_OUTPUT_COLUMNS]
                    append_rows_to_csv(session_df, out_path)
                    n_rows_written += len(session_df)
                else:
                    n_no_sessions += 1
            else:
                n_no_sessions += 1

            limiter.wait()

    print(f"\n[RESULT] Run complete. {n_rows_written} new session rows written to "
          f"{out_path.resolve()}")
    print(f"[SUMMARY] class_ids processed this run: {n_classes}, "
          f"no sessions returned: {n_no_sessions}, errors: {n_errors}")

    total_rows = 0
    if out_path.exists():
        final_df = pd.read_csv(out_path, encoding="utf-8-sig")
        total_rows = len(final_df)
        print(f"[TOTAL] {total_rows} session rows across "
              f"{final_df['class_id'].nunique()} class_ids in {out_path.name}")
        if "session_conducted" in final_df.columns:
            print(f"  Conducted: {int(final_df['session_conducted'].sum())}  "
                  f"Not conducted: {int((~final_df['session_conducted'].astype(bool)).sum())}")

    end_time = datetime.now()
    summary = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_sec": round((end_time - start_time).total_seconds(), 1),
        "class_ids_processed": n_classes,
        "rows_written_this_run": n_rows_written,
        "no_sessions": n_no_sessions,
        "errors": n_errors,
        "total_rows_in_output": total_rows,
        "output_file": str(out_path),
    }
    send_run_report(STAGE_NAME, summary, config)


if __name__ == "__main__":
    with PipelineRunLogger(STAGE_NAME, Path(__file__), args_summary=str(sys.argv[1:])):
        main()
