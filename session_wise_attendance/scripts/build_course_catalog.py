"""
build_course_catalog.py
--------------------------------------------------------------------------
STAGE 1 (primary). Merges /institute/{id}/courses/catalogue with
/short/masterbatch into one row per batch — the catalog everything else
in the pipeline is built on top of. See RULES.md for the full inclusion/
exclusion rules (batch status filter, exclusion list, latest-batch and
Final_Status derivation, bundle enrollment rollup).

RATE LIMITING (Edmingle allows max 30 calls/min):
  - Catalogue + batch-page requests are spaced at a safe ~24/min by default
    (--calls_per_minute to tune), same convention as Stages 2 & 3.
  - On HTTP 429, waits out Edmingle's own reported block duration.

ROLLBACK NOTE: unlike Stages 2 & 3, this script does NOT support row-level
checkpoint/resume — Is_Latest_Batch and the bundle enrollment rollup are
GLOBAL aggregates that need the complete fetched dataset before any row can
be written, so there's nothing meaningful to resume mid-fetch. "Rollback
safety" here means: retries survive transient/429 failures without losing
already-fetched pages in memory, and the output CSV is only ever written
once, atomically, after every business rule has been computed — a crash
never leaves a partial/corrupt course_catalog.csv on disk.

USAGE
-----
    python build_course_catalog.py
    python build_course_catalog.py --calls_per_minute 20
"""

import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path

# Shared bytecode cache across every ela_datasets/ pipeline -- must be set before any local import.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).parent))
from pipeline_common import (
    BASE_URL,
    PipelineRunLogger,
    RateLimiter,
    load_config,
    parse_retry_after_seconds,
    require_config,
    resolve_output_folder,
    send_run_report,
)

STAGE_NAME = "build_course_catalog"
SCRIPT_DIR = Path(__file__).parent
DEFAULT_CALLS_PER_MINUTE = 24  # safety margin under Edmingle's 30/min limit

# ── Final output columns — matches vyoma_masters.csv exactly ────────
# Column order is preserved to match the reference sheet
OUTPUT_COLUMNS = [
    "bundle_id",
    "bundle_name",
    "batch_id",
    "batch_name",
    "batch_status",
    "start_date",
    "end_date",
    "tutor_name",
    "tutor_id",
    "batch_enrollment_count",       # admitted_students from batch API (per-batch)
    # Catalogue columns
    "Course Name",
    "Tutors",
    "Tutord Ids",
    "Course Ids",
    "Subject",
    "Level",
    "Language",
    "Examination",
    "Type",
    "Course Division",
    "Certificate",
    "Course Sponsor",
    "Course Title Sanskrit",
    "Status",
    "Number of Lectures",
    "Duration",
    "Personas",
    "Computer Based Assessment",
    "Product ID",
    "SSS Category",
    "Viniyoga",
    "Adhyayanam Category",
    "Term of Course",
    "Position in Funnel",
    "Division",
    # Derived / computed columns
    "Catalogue_Match",
    "bundle_enrollment_count",      # Num Students from catalogue API (per-bundle sum)
    "Is_Latest_Batch",
    "Has_Batch",
    "Catalogue_Status",
    "Final_Status",
]

# ═══════════════════════════════════════════════════════════════════
# EXCLUSION RULES — see RULES.md § Stage 1 for rationale
# ═══════════════════════════════════════════════════════════════════

# Specific batch_id values to exclude outright. Nothing here is matched
# by name — only by this exact ID list. (Deduped; 12464 appeared twice.)
BATCH_IDS_TO_EXCLUDE = {
    12458, 12459, 12464, 12472, 12473, 12474, 12475, 12485, 12487,
    12513, 12522, 12550, 12551, 12554, 12606, 12607,
    70572, 42632, 70587, 53438,
}


def is_excluded_batch_id(batch_id):
    try:
        return int(batch_id) in BATCH_IDS_TO_EXCLUDE
    except (TypeError, ValueError):
        return False


def log_progress(message):
    now = datetime.now().strftime("%H:%M:%S")
    print(f"{now} - {message}")


def _request_with_retry(url, headers, params=None, max_retries=3, label=""):
    """Shared GET-with-429/backoff wrapper for both catalogue and batch calls."""
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 429:
                wait_seconds = parse_retry_after_seconds(response.text)
                print(f"\n[RATE LIMIT] {label}: 429 received. "
                      f"Waiting {wait_seconds/60:.1f} min before retrying "
                      f"(attempt {attempt}/{max_retries})...")
                time.sleep(wait_seconds)
                continue
            return response
        except requests.exceptions.RequestException as e:
            print(f"[RETRY {attempt}/{max_retries}] {label} request failed: {e}")
            if attempt < max_retries:
                time.sleep(2 * attempt)
    print(f"[ERROR] {label} exhausted retries.")
    return None


def get_catalogue(institute_id, headers):
    log_progress("Fetching Course Catalogue...")
    url = f"{BASE_URL}/institute/{institute_id}/courses/catalogue?institution_id={institute_id}"
    response = _request_with_retry(url, headers, label="catalogue")

    if response is None or response.status_code != 200:
        print("Error fetching catalogue!")
        if response is not None:
            print("Status Code:", response.status_code)
            print("Response body:", response.text[:500])
            print("Tip: If 400/401, the API key may have rotated — update edmingle.api_key in ../../credentials.yaml.")
        return pd.DataFrame()

    # Guard against HTML response (wrong URL resolving to web page)
    content_type = response.headers.get("Content-Type", "")
    if "text/html" in content_type:
        print("Error: Catalogue endpoint returned HTML, not JSON. Check the URL.")
        return pd.DataFrame()

    data = response.json()
    rows = data.get("response", [])

    df = pd.DataFrame(rows)
    log_progress(f"Catalogue fetched: {len(df)} rows")
    return df


def get_batches_by_status(status_code, status_label, org_id, headers, limiter: RateLimiter):
    log_progress(f"  Fetching batches with status={status_code} ({status_label})...")
    batches_url = f"{BASE_URL}/short/masterbatch"

    page = 1
    all_batch_rows = []

    while True:
        limiter.start()
        url = (
            f"{batches_url}"
            f"?status={status_code}"
            f"&page={page}"
            f"&per_page=1000"
            f"&organization_id={org_id}"
        )
        response = _request_with_retry(url, headers, label=f"batches page={page} status={status_label}")
        limiter.wait()  # rate-limit spacing regardless of success/failure

        if response is None:
            break

        data = response.json()
        courses = data.get("courses", [])

        if not courses:
            break

        # Flatten each course's nested batch array into one row per batch
        for course in courses:
            for b in course.get("batch", [{}]):
                row = {
                    "bundle_id":               course.get("bundle_id"),
                    "bundle_name":             str(course.get("bundle_name", "")).strip(),
                    "batch_id":                b.get("class_id"),
                    "batch_name":              str(b.get("class_name", "")).strip(),
                    "batch_status":            status_label,
                    "start_date":              b.get("start_date"),
                    "end_date":                b.get("end_date"),
                    "tutor_name":              b.get("tutor_name"),
                    "tutor_id":                b.get("tutor_id"),
                    # admitted_students from API = per-batch enrollment count
                    "batch_enrollment_count":  b.get("admitted_students", 0) or 0,
                }
                all_batch_rows.append(row)

        # Pagination: stop when fewer rows than per_page were returned
        if len(courses) < 1000:
            break
        page += 1

    log_progress(f"  Status {status_label}: {len(all_batch_rows)} batch rows")
    return all_batch_rows


def get_all_batches(org_id, headers, limiter: RateLimiter):
    log_progress("Fetching All Batches...")

    # Archived batches are excluded entirely — no point fetching them.
    status_codes = {0: "Active", 3: "Completed"}

    all_data = []
    for code, label in status_codes.items():
        rows = get_batches_by_status(code, label, org_id, headers, limiter)
        all_data.extend(rows)

    df = pd.DataFrame(all_data)
    log_progress(f"Total batch rows fetched (Archived excluded): {len(df)}")
    return df


def filter_excluded_batches(df):
    """Drops rows whose batch_id is in BATCH_IDS_TO_EXCLUDE."""
    clean_df = df.copy()
    keep_mask = ~clean_df["batch_id"].apply(is_excluded_batch_id)
    removed = len(clean_df) - keep_mask.sum()
    clean_df = clean_df[keep_mask].reset_index(drop=True)
    log_progress(f"Excluded {removed} rows (batch_id list). Remaining: {len(clean_df)}")
    return clean_df


def mark_latest_batch(df):
    working_df = df.copy()

    # Convert Unix timestamp start_date to a number for sorting
    working_df["_sort_date"] = pd.to_numeric(working_df["start_date"], errors="coerce").fillna(0)

    # Sort: per bundle, newest start_date first; use batch_id as tiebreaker
    working_df = working_df.sort_values(
        ["bundle_id", "_sort_date", "batch_id"],
        ascending=[True, False, False]
    ).reset_index(drop=True)

    working_df["Is_Latest_Batch"] = 0

    last_bundle = None
    for i in range(len(working_df)):
        current_bundle = working_df.loc[i, "bundle_id"]
        if current_bundle != last_bundle:
            working_df.loc[i, "Is_Latest_Batch"] = 1
            last_bundle = current_bundle

    working_df = working_df.drop(columns=["_sort_date"])
    return working_df


def apply_business_logic(df):
    working_df = df.copy()

    # Catalogue_Status mirrors the raw catalogue Status column
    working_df["Catalogue_Status"] = working_df["Status"]

    # Default: all non-latest batches are Completed
    working_df["Final_Status"] = "Completed"

    valid_statuses = ["Completed", "Ongoing", "Upcoming"]

    for i in range(len(working_df)):
        if working_df.loc[i, "Is_Latest_Batch"] == 1:
            raw_status = str(working_df.loc[i, "Status"]).strip()
            # Latest batch uses the catalogue Status as its Final_Status
            if raw_status in valid_statuses:
                working_df.loc[i, "Final_Status"] = raw_status
            else:
                # Latest batch but no catalogue match — leave blank
                working_df.loc[i, "Final_Status"] = ""

    return working_df


def compute_bundle_enrollment(df):
    # Sum batch_enrollment_count (admitted_students) per bundle -> bundle_enrollment_count
    bundle_totals = (
        df.groupby("bundle_id")["batch_enrollment_count"]
        .sum()
        .reset_index()
        .rename(columns={"batch_enrollment_count": "bundle_enrollment_count"})
    )
    merged = df.merge(bundle_totals, on="bundle_id", how="left")
    return merged


def add_courses_without_batches(merged_df, catalogue_df):
    # Identify bundle IDs that already have at least one batch row
    existing_ids = set(merged_df["bundle_id"].unique())

    # Find catalogue rows whose bundle_id is NOT in the batch data
    missing_mask = ~catalogue_df["Bundle id"].isin(existing_ids)
    missing = catalogue_df[missing_mask].copy().reset_index(drop=True)

    if len(missing) == 0:
        log_progress("No courses without batches found.")
        return merged_df

    # Map the catalogue join key to bundle_id
    missing["bundle_id"] = missing["Bundle id"]
    missing["bundle_name"] = missing.get("Course Name", pd.Series(dtype=str))

    # Fill derived columns for no-batch rows
    missing["Has_Batch"] = 0
    missing["Is_Latest_Batch"] = 1
    missing["bundle_enrollment_count"] = 0
    missing["Catalogue_Match"] = 1          # These came from catalogue so they matched
    missing["Catalogue_Status"] = missing["Status"]
    missing["Final_Status"] = missing["Status"]

    # Batch-specific columns are empty for no-batch rows
    for col in ["batch_id", "batch_name", "batch_status", "start_date", "end_date",
                "tutor_name", "tutor_id", "batch_enrollment_count"]:
        missing[col] = None

    final_df = pd.concat([merged_df, missing], ignore_index=True)
    log_progress(f"Added {len(missing)} catalogue-only (no-batch) rows. Total: {len(final_df)}")
    return final_df


def main():
    parser = argparse.ArgumentParser(description="Build the Vyoma course/batch catalog (Stage 1)")
    parser.add_argument("--apikey", type=str, default=None)
    parser.add_argument("--out", type=str, default="course_catalog.csv")
    parser.add_argument("--calls_per_minute", type=float, default=DEFAULT_CALLS_PER_MINUTE)
    args = parser.parse_args()

    config = load_config(SCRIPT_DIR)
    apikey = args.apikey or config.get("api_key") or config.get("apikey")
    org_id = require_config(config, "org_id")
    institute_id = require_config(config, "institute_id")

    if not apikey:
        print("[ERROR] No API key found. Pass --apikey or set edmingle.api_key in ../../credentials.yaml")
        sys.exit(1)

    output_folder = resolve_output_folder(config, Path(__file__))
    out_path = output_folder / args.out

    headers = {"apikey": apikey, "ORGID": str(org_id), "Accept": "application/json"}
    limiter = RateLimiter(args.calls_per_minute)

    n_rows_written = 0
    n_errors = 0
    start_time = datetime.now()

    try:
        log_progress("Starting Course Catalog Build...")

        cat_df = get_catalogue(institute_id, headers)
        if cat_df.empty:
            print("Catalogue fetch failed. Aborting.")
            n_errors += 1
            return

        batch_df = get_all_batches(org_id, headers, limiter)
        if batch_df.empty:
            print("Batch fetch returned no data. Aborting.")
            n_errors += 1
            return

        batch_df = filter_excluded_batches(batch_df)
        batch_df = compute_bundle_enrollment(batch_df)
        batch_df = mark_latest_batch(batch_df)
        batch_df["Has_Batch"] = 1

        # Join key: batch side = "bundle_id", catalogue side = "Bundle id"
        merged = batch_df.merge(cat_df, left_on="bundle_id", right_on="Bundle id", how="left")
        merged["Catalogue_Match"] = merged["Bundle id"].notna().astype(int)
        merged = apply_business_logic(merged)
        final_df = add_courses_without_batches(merged, cat_df)

        # Unix timestamp -> YYYY-MM-DD
        final_df["start_date"] = pd.to_datetime(
            pd.to_numeric(final_df["start_date"], errors="coerce"), unit="s", errors="coerce"
        ).dt.date
        final_df["end_date"] = pd.to_datetime(
            pd.to_numeric(final_df["end_date"], errors="coerce"), unit="s", errors="coerce"
        ).dt.date

        existing_cols = [col for col in OUTPUT_COLUMNS if col in final_df.columns]
        missing_cols = [col for col in OUTPUT_COLUMNS if col not in final_df.columns]
        if missing_cols:
            print(f"Warning: these expected columns were not found and will be skipped: {missing_cols}")

        final_report = final_df[existing_cols].fillna("")

        # Written once, atomically, only after every business rule above has run —
        # a crash before this point never leaves a partial/corrupt output file.
        final_report.to_csv(out_path, index=False, encoding="utf-8-sig")
        n_rows_written = len(final_report)

        log_progress(f"SUCCESS! Saved {n_rows_written} rows to {out_path}")
        log_progress(f"Columns written: {len(existing_cols)}")

    except Exception as e:
        print("ERROR occurred during execution!")
        print("Error message:", str(e))
        import traceback
        traceback.print_exc()
        n_errors += 1
    finally:
        end_time = datetime.now()
        summary = {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_sec": round((end_time - start_time).total_seconds(), 1),
            "rows_written": n_rows_written,
            "errors": n_errors,
            "output_file": str(out_path),
        }
        send_run_report(STAGE_NAME, summary, config)


if __name__ == "__main__":
    with PipelineRunLogger(STAGE_NAME, Path(__file__), args_summary=str(sys.argv[1:])):
        main()
