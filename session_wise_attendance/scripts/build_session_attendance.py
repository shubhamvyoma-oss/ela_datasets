"""
build_session_attendance.py
--------------------------------------------------------------------------
STAGE 3. Reads class_id_lookup.csv (from resolve_class_ids.py) and pulls
session-wise attendance for EVERY class_id in it via /organization/attendances.
Output: session_wise_attendance_data.csv. See RULES.md § Stage 3 for the
session-status->conducted mapping, IST conversion rule, and session_number
numbering rule.

RATE LIMITING (Edmingle allows max 30 calls/min):
  - Calls spaced at a safe ~24/min by default (--calls_per_minute to tune).
  - fetch_org_attendances (imported from attendance_crossvalidation.py) now
    handles 429s by waiting out Edmingle's own reported block duration.

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

# Windows console/redirect default (cp1252) can't encode many batch-name
# characters (em-dashes, curly quotes, etc.) — force UTF-8 stdout so a
# print() never crashes the run mid-way through.
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent))
from attendance_crossvalidation import fetch_org_attendances, sessions_to_dataframe, SESSION_BASE_COLUMNS
from pipeline_common import (
    load_config, resolve_output_folder, RateLimiter, PipelineRunLogger, send_run_report,
)

STAGE_NAME = "build_session_attendance"
SCRIPT_DIR = Path(__file__).parent
DEFAULT_CALLS_PER_MINUTE = 24  # safety margin under Edmingle's 30/min limit

# Final Stage 3 output column order — SESSION_BASE_COLUMNS (from
# attendance_crossvalidation.py) plus bundle_id/bundle_name, which only
# this stage can add (sourced from class_id_lookup.csv).
_split_at = SESSION_BASE_COLUMNS.index("class_date")
MASTER_OUTPUT_COLUMNS = (
    SESSION_BASE_COLUMNS[:_split_at] + ["bundle_id", "bundle_name"] + SESSION_BASE_COLUMNS[_split_at:]
)


def to_unix(date_str: str) -> int:
    """Parse YYYY-MM-DD as IST midnight -> unix timestamp."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    ist_offset_seconds = 5.5 * 3600
    utc_dt = dt.replace(tzinfo=timezone.utc)
    return int(utc_dt.timestamp() - ist_offset_seconds)


def load_already_processed(out_path: Path) -> set:
    """class_ids already present in the output CSV from a prior run."""
    if not out_path.exists():
        return set()
    try:
        existing_df = pd.read_csv(out_path, encoding="utf-8-sig")
        if "class_id" in existing_df.columns:
            return set(existing_df["class_id"].dropna().astype(int).tolist())
    except Exception as e:
        print(f"[WARN] Could not read existing output for resume check: {e}")
    return set()


def append_df_to_csv(df: pd.DataFrame, out_path: Path):
    write_header = not out_path.exists()
    df.to_csv(out_path, mode="a", index=False, header=write_header, encoding="utf-8-sig")


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
    apikey = args.apikey or config.get("api_key") or config.get("apikey")
    org_id = config.get("org_id", 683)

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

    already_processed = load_already_processed(out_path)
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
                    append_df_to_csv(session_df, out_path)
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
