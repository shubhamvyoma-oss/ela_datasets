"""
resolve_class_ids.py
--------------------------------------------------------------------------
STAGE 2. Reads course_catalog.csv (from build_course_catalog.py) and
resolves class_id(s) for every batch_id via GET /masterbatch/<batchId>.
See RULES.md § Stage 2 for the response-shape and multi-class_id rules.

RATE LIMITING (Edmingle allows max 30 calls/min):
  - Calls are spaced at a safe ~24/min (2.5s apart) by default — configurable
    via --calls_per_minute if you want to tune it.
  - On HTTP 429, the script reads Edmingle's own message
    ("Try after X minutes") and actually waits that long, rather than
    burning through quick retries that don't help during an active block.

CHECKPOINT / RESUME (critical — a 429 block WILL happen on long runs):
  - Every batch's resolved row(s) are appended to the output CSV
    IMMEDIATELY after each successful call, not held in memory until the
    end. If the run gets rate-limited, killed, or crashes, nothing already
    resolved is lost.
  - On startup, the script reads any existing output CSV and skips
    batch_ids already present in it — so simply rerunning the same command
    after a block clears resumes automatically. Use --restart to ignore
    existing progress and start clean.

USAGE
-----
    python resolve_class_ids.py
    python resolve_class_ids.py --limit 20          # quick test
    python resolve_class_ids.py --restart            # ignore existing progress
"""

import argparse
import os
import sys
import time
from datetime import datetime
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
    PipelineRunLogger,
    RateLimiter,
    load_config,
    parse_retry_after_seconds,
    resolve_output_folder,
    send_run_report,
)

STAGE_NAME = "resolve_class_ids"
SCRIPT_DIR = Path(__file__).parent
BASE_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1"
MASTERBATCH_ENDPOINT = f"{BASE_URL}/masterbatch"

OUTPUT_COLUMNS = [
    "bundle_id", "bundle_name", "batch_id", "batch_name",
    "class_id", "tutor_name", "tutor_id",
    "total_classes", "completed_classes", "cancelled_classes",
    "num_users", "associated_masterbatches",
]

DEFAULT_CALLS_PER_MINUTE = 24  # safety margin under Edmingle's 30/min limit


def fetch_classes_for_batch(apikey: str, org_id: int, batch_id: int,
                             max_retries: int = 2, debug: bool = False) -> list:
    """Calls GET /masterbatch/<batchId>. On 429, waits out Edmingle's own
    reported block duration rather than retrying instantly — instant
    retries during an active IP block just waste time and risk extending it.
    debug=True prints the full raw response regardless of outcome — used
    for the very first call of a run to diagnose silent empty-result issues."""
    url = f"{MASTERBATCH_ENDPOINT}/{batch_id}"
    headers = {"apikey": apikey, "orgid": str(org_id), "ORGID": str(org_id)}
    params = {"apikey": apikey, "org_id": org_id}

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, headers=headers, timeout=30)

            if debug:
                print(f"\n[DEBUG] GET {resp.url}")
                print(f"[DEBUG] status_code={resp.status_code}")
                print(f"[DEBUG] raw response body (first 2000 chars):\n{resp.text[:2000]}\n")

            if resp.status_code == 429:
                wait_seconds = parse_retry_after_seconds(resp.text)
                print(f"\n[RATE LIMIT] batch_id={batch_id}: "
                      f"429 received. Waiting {wait_seconds/60:.1f} min before retrying "
                      f"(attempt {attempt}/{max_retries})...")
                time.sleep(wait_seconds)
                continue

            if resp.status_code >= 400:
                print(f"[HTTP {resp.status_code}] batch_id={batch_id} "
                      f"response: {resp.text[:300]}")
            resp.raise_for_status()
            data = resp.json()
            # REAL response shape (confirmed live — Edmingle's docs were wrong):
            # {"code":200,"class":{"courses_array":[{...actual subject/class_id...}],
            #  "class_id": <this is actually the BATCH id>, ...}}
            # The real subject-level class_id is nested under class.courses_array[],
            # already as proper dicts — no array-index mapping needed.
            class_obj = data.get("class", {})
            return class_obj.get("courses_array", [])

        except requests.exceptions.RequestException as e:
            print(f"[RETRY {attempt}/{max_retries}] batch_id={batch_id} "
                  f"request failed: {e}")
            if attempt < max_retries:
                time.sleep(5)

    print(f"[WARN] Could not resolve class_ids for batch_id={batch_id} "
          f"after {max_retries} attempts.")
    return []


def courses_array_to_records(courses_array: list) -> list:
    """The real courses_array items are already dicts — just pull out the
    fields we care about. associated_masterbatches is a list; joined to a
    comma string for CSV storage (relevant to cross-batch broadcast
    session detection — one class_id can span multiple batches)."""
    records = []
    for item in courses_array:
        assoc = item.get("associated_masterbatches")
        assoc_str = ",".join(str(x) for x in assoc) if isinstance(assoc, list) else assoc
        records.append({
            "class_id": item.get("class_id"),
            "tutor_name": item.get("tutor_name"),
            "tutor_id": item.get("tutor_id"),
            "total_classes": item.get("total_classes"),
            "completed_classes": item.get("completed"),
            "cancelled_classes": item.get("cancelled"),
            "num_users": item.get("num_users"),
            "associated_masterbatches": assoc_str,
        })
    return records


def load_already_processed(out_path: Path) -> set:
    """Returns the set of batch_ids already present in the output
    CSV from a prior (possibly interrupted) run."""
    if not out_path.exists():
        return set()
    try:
        existing_df = pd.read_csv(out_path, encoding="utf-8-sig")
        if "batch_id" in existing_df.columns:
            return set(existing_df["batch_id"].dropna().astype(int).tolist())
    except Exception as e:
        print(f"[WARN] Could not read existing output for resume check: {e}")
    return set()


def append_rows_to_csv(rows: list, out_path: Path):
    """Appends rows to the output CSV, writing the header only if the file
    doesn't exist yet. Called after EVERY batch so progress is never lost."""
    df = pd.DataFrame(rows, columns=OUTPUT_COLUMNS)
    write_header = not out_path.exists()
    df.to_csv(out_path, mode="a", index=False, header=write_header, encoding="utf-8-sig")


def main():
    parser = argparse.ArgumentParser(
        description="Resolve class_id(s) for every batch in course_catalog.csv (Stage 2)")
    parser.add_argument("--in", dest="input_file", type=str,
                         default="course_catalog.csv")
    parser.add_argument("--out", type=str, default="class_id_lookup.csv")
    parser.add_argument("--limit", type=int, default=None,
                         help="Only process the first N NEW batches (after skipping already-resolved ones)")
    parser.add_argument("--calls_per_minute", type=float, default=DEFAULT_CALLS_PER_MINUTE,
                         help="Stay safely under Edmingle's 30/min limit")
    parser.add_argument("--restart", action="store_true",
                         help="Ignore existing progress in the output CSV and start fresh "
                              "(WARNING: overwrites previous results)")
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

    catalog_df = pd.read_csv(input_path, encoding="utf-8-sig")
    print(f"[INFO] Loaded {len(catalog_df)} rows from {input_path}")

    required_cols = {"batch_id", "batch_name", "bundle_id", "bundle_name"}
    missing_cols = required_cols - set(catalog_df.columns)
    if missing_cols:
        print(f"[ERROR] Missing expected columns: {missing_cols}. "
              f"Available: {list(catalog_df.columns)}")
        sys.exit(1)

    catalog_df = catalog_df.dropna(subset=["batch_id"]).drop_duplicates(subset=["batch_id"])

    out_path = output_folder / args.out

    if args.restart and out_path.exists():
        out_path.unlink()
        print(f"[INFO] --restart: removed existing {out_path}, starting fresh.")

    already_processed = load_already_processed(out_path)
    if already_processed:
        print(f"[INFO] Resuming: {len(already_processed)} batches already resolved in "
              f"{out_path.name}, skipping those.")
        catalog_df = catalog_df[~catalog_df["batch_id"].astype(int).isin(already_processed)]

    if args.limit:
        catalog_df = catalog_df.head(args.limit)

    n_batches = len(catalog_df)
    start_time = datetime.now()

    if n_batches == 0:
        print("[INFO] Nothing left to process — all batches already resolved.")
    else:
        limiter = RateLimiter(args.calls_per_minute)
        est_seconds = n_batches * limiter.delay_seconds
        print(f"[INFO] Resolving class_id(s) for {n_batches} remaining batches "
              f"at {args.calls_per_minute:.0f} calls/min "
              f"(rough estimate: ~{est_seconds/60:.1f} min, before any rate-limit waits)")

        for i, row in enumerate(catalog_df.itertuples(index=False), 1):
            batch_id = int(row.batch_id)
            print(f"  [{i}/{n_batches}] batch_id={batch_id} ({row.batch_name})")

            limiter.start()
            courses_array = fetch_classes_for_batch(apikey, org_id, batch_id, debug=(i == 1))
            class_records = courses_array_to_records(courses_array)

            if not class_records:
                class_records = [{}]  # still emit a row so the batch isn't silently dropped

            rows_to_write = [{
                "bundle_id": row.bundle_id,
                "bundle_name": row.bundle_name,
                "batch_id": batch_id,
                "batch_name": row.batch_name,
                "class_id": cr.get("class_id"),
                "tutor_name": cr.get("tutor_name"),
                "tutor_id": cr.get("tutor_id"),
                "total_classes": cr.get("total_classes"),
                "completed_classes": cr.get("completed_classes"),
                "cancelled_classes": cr.get("cancelled_classes"),
                "num_users": cr.get("num_users"),
                "associated_masterbatches": cr.get("associated_masterbatches"),
            } for cr in class_records]

            # Write immediately — this is what makes the run resumable.
            append_rows_to_csv(rows_to_write, out_path)
            limiter.wait()

    print(f"\n[RESULT] Run complete. Output at {out_path.resolve()}")
    total_rows = 0
    unresolved = 0
    if out_path.exists():
        final_df = pd.read_csv(out_path, encoding="utf-8-sig")
        total_rows = len(final_df)
        unresolved = int(final_df["class_id"].isna().sum())
        print(f"[SUMMARY] {total_rows} total rows, {final_df['batch_id'].nunique()} "
              f"unique batches, {unresolved} rows with no resolvable class_id")

    end_time = datetime.now()
    summary = {
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_sec": round((end_time - start_time).total_seconds(), 1),
        "batches_processed": n_batches,
        "total_rows_in_output": total_rows,
        "unresolved_rows": unresolved,
        "output_file": str(out_path),
    }
    send_run_report(STAGE_NAME, summary, config)


if __name__ == "__main__":
    with PipelineRunLogger(STAGE_NAME, Path(__file__), args_summary=str(sys.argv[1:])):
        main()