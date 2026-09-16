"""
build_course_catalog_alt.py
--------------------------------------------------------------------------
STAGE 1 (backup/alternate). Fetches + cleans the full Vyoma batch catalog
from /short/masterbatch alone (no catalogue merge). Kept as a simpler
alternative — build_course_catalog.py is the one actually feeding Stage 2;
this script can drift from it, so don't treat its output as authoritative
without checking build_course_catalog.py first. See RULES.md § Stage 1 for
the shared inclusion/exclusion rules.

CONFIRMED RESPONSE SCHEMA (from a live /short/masterbatch response):
    {
      "code": 200, "message": "Success",
      "courses": [                              <- one entry per BUNDLE
        {
          "bundle_id": 6339,
          "bundle_name": "Siddhanta-Kaumudi - Atmanepada Prakaranam",
          "is_woolf_accredited": 0,
          "online_only": 1,
          "batch": [                             <- usually 1, can be more
            {
              "class_id": 12442,                 <- see note below
              "class_name": "...",
              "start_date": 1390435200,           (unix, nullable)
              "end_date": 1392336000,             (unix, nullable)
              "individual_batch_attendance": 0,
              "tutor_id": 18983595,
              "tutor_name": "Dr. Venkatasubramanian P",
              "online_only": 1,
              "mb_archived": 0,
              "registered_students": 0,
              "admitted_students": 399,
              "total_classes": 44,
              "completed_classes": 0,
              "cancelled_classes": 0,
              "attendance_progress": 0,
              "progress": ["2"],
              "classes": [ [ ... 17 index-based fields, undocumented ... ] ]
            }
          ]
        }
      ],
      "page_context": {"page":1,"per_page":5,"has_more_page":true,"total_rows":1363}
    }

NOTE ON "class_id" IN THIS ENDPOINT
-------------------------------------
This is almost certainly the BATCH id (i.e. what other endpoints call
master_batch_id), not the subject/stream class_id seen in
/organization/attendances (e.g. 199222). Mapped to a "master_batch_id"
column below on that assumption — VERIFY once against the admin UI before
fully trusting.

The nested "classes" array inside each batch is index-based and NOT fully
documented — deliberately not deep-parsed here since total_classes /
completed_classes / cancelled_classes already give the same information
cleanly at the batch level.

FILTERING
---------
Removes: archived batches (mb_archived), test/flipbook/audio content
(keyword match against bundle_name/class_name), and a fixed exclusion list.

RATE LIMITING / ROLLBACK: same 30/min cap and 429 backoff convention as
every other pipeline script (--calls_per_minute to tune). Like
build_course_catalog.py, this script has no per-row resume — the cleaned/
raw/removed-rows CSVs are only written once, at the end, after every page
has been fetched, so a crash never leaves a partial output file.

OUTPUT
------
    course_catalog_raw_export.csv  -> raw, unfiltered (audit trail)
    course_catalog.csv             -> cleaned catalog
    removed_rows_log.csv           -> every dropped row + reason

USAGE
-----
    python build_course_catalog_alt.py
    python build_course_catalog_alt.py --per_page 100
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
import requests

sys.path.insert(0, str(Path(__file__).parent))
from pipeline_common import (
    load_config, parse_retry_after_seconds, resolve_output_folder,
    RateLimiter, PipelineRunLogger, send_run_report,
)

STAGE_NAME = "build_course_catalog_alt"
CONFIG_PATH = Path(__file__).parent / "config.yaml"
BASE_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1"
MASTERBATCH_ENDPOINT = f"{BASE_URL}/short/masterbatch"
DEFAULT_CALLS_PER_MINUTE = 24  # safety margin under Edmingle's 30/min limit

IST_OFFSET_SECONDS = 5.5 * 3600

# One-off exclusion list, deduped (12464 was given twice).
EXCLUDED_BATCH_IDS = sorted(set([
    12458, 12459, 12464, 12472, 12473, 12474, 12475, 12485, 12487,
    12513, 12522, 12550, 12551, 12554, 12606, 12607, 70572, 42632,
    70587, 53438,
]))

# Case-insensitive substring match against bundle_name / class_name.
CONTENT_KEYWORDS = ["test", "flipbook", "audio"]


# ==========================================================================
# STEP 1: FETCH
# ==========================================================================

def unix_to_ist(ts, fmt: str = "%Y-%m-%d"):
    """Manual +5:30 offset, consistent with the rest of the pipeline
    (avoids zoneinfo/pytz/tzdata dependency issues on Windows)."""
    if ts is None or pd.isna(ts):
        return None
    dt = datetime.fromtimestamp(int(ts) + IST_OFFSET_SECONDS, tz=timezone.utc)
    return dt.strftime(fmt)


def fetch_page(apikey: str, org_id: int, page: int, per_page: int,
                max_retries: int = 3) -> dict:
    """Deliberately omits `status` and `bundle_id` params — both are known
    to silently break results (status=0 misses recent batches, bundle_id=0
    returns zero rows)."""
    params = {
        "apikey": apikey,
        "organization_id": org_id,
        "page": page,
        "per_page": per_page,
    }
    headers = {"apikey": apikey, "orgid": str(org_id), "ORGID": str(org_id)}

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(MASTERBATCH_ENDPOINT, params=params, headers=headers, timeout=30)

            if resp.status_code == 429:
                wait_seconds = parse_retry_after_seconds(resp.text)
                print(f"\n[RATE LIMIT] page={page}: 429 received. "
                      f"Waiting {wait_seconds/60:.1f} min before retrying "
                      f"(attempt {attempt}/{max_retries})...")
                import time
                time.sleep(wait_seconds)
                continue

            if resp.status_code >= 400:
                print(f"[HTTP {resp.status_code}] page={page} response: {resp.text[:400]}")
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            print(f"[RETRY {attempt}/{max_retries}] page={page} request failed: {e}")
            if attempt < max_retries:
                import time
                time.sleep(2 * attempt)
    print(f"[ERROR] page={page} exhausted retries, skipping")
    return {}


def fetch_all_courses(apikey: str, org_id: int, limiter: RateLimiter, per_page: int = 100,
                       max_pages_safety: int = 500) -> list:
    """Paginates on page_context.has_more_page (NOT total_rows, which is
    unreliable for multi-subject batches). Stops early after 3 consecutive
    empty pages to avoid spinning through hundreds of pages on a bad param."""
    all_courses = []
    page = 1
    consecutive_empty_pages = 0

    while page <= max_pages_safety:
        limiter.start()
        data = fetch_page(apikey, org_id, page, per_page)
        limiter.wait()  # rate-limit spacing regardless of success/failure

        if not data or data.get("code") != 200:
            print(f"[WARN] page={page} returned no usable data, stopping pagination.")
            break

        courses = data.get("courses", [])
        if not courses:
            print(f"[WARN] page={page}: empty 'courses' array.")
            consecutive_empty_pages += 1
            if consecutive_empty_pages >= 3:
                print(f"[ERROR] {consecutive_empty_pages} consecutive empty pages — stopping.")
                break
        else:
            consecutive_empty_pages = 0

        all_courses.extend(courses)

        page_context = data.get("page_context", {})
        has_more = page_context.get("has_more_page", False)
        total_rows = page_context.get("total_rows")
        print(f"[INFO] page={page}: +{len(courses)} bundles "
              f"(running total {len(all_courses)}"
              f"{f' of ~{total_rows}' if total_rows else ''}), has_more_page={has_more}")

        if not has_more:
            break
        page += 1

    return all_courses


# ==========================================================================
# STEP 2: FLATTEN  (courses -> bundle -> batch, one row per batch)
# ==========================================================================

def flatten_courses(courses: list) -> pd.DataFrame:
    rows = []
    for course in courses:
        bundle_id = course.get("bundle_id")
        bundle_name = course.get("bundle_name")
        is_woolf_accredited = course.get("is_woolf_accredited")

        batches = course.get("batch", [])
        if not batches:
            # Bundle with no batch entries at all — still worth a row so it's
            # visible in the raw export, flagged clearly.
            rows.append({
                "bundle_id": bundle_id,
                "bundle_name": bundle_name,
                "is_woolf_accredited": is_woolf_accredited,
                "master_batch_id": None,
                "batch_name": None,
                "no_batch_entries": True,
            })
            continue

        for b in batches:
            progress = b.get("progress")
            progress_str = ",".join(str(p) for p in progress) if isinstance(progress, list) else progress

            rows.append({
                "bundle_id": bundle_id,
                "bundle_name": bundle_name,
                "is_woolf_accredited": is_woolf_accredited,
                # See module docstring: this "class_id" is treated as master_batch_id.
                "master_batch_id": b.get("class_id"),
                "batch_name": b.get("class_name"),
                "start_date": unix_to_ist(b.get("start_date")),
                "end_date": unix_to_ist(b.get("end_date")),
                "individual_batch_attendance": b.get("individual_batch_attendance"),
                "tutor_id": b.get("tutor_id"),
                "tutor_name": b.get("tutor_name"),
                "online_only": b.get("online_only"),
                "mb_archived": b.get("mb_archived"),
                "registered_students": b.get("registered_students"),
                "admitted_students": b.get("admitted_students"),
                "total_classes": b.get("total_classes"),
                "completed_classes": b.get("completed_classes"),
                "cancelled_classes": b.get("cancelled_classes"),
                "attendance_progress": b.get("attendance_progress"),
                "progress": progress_str,
                "num_class_configs": len(b.get("classes", [])),
                "no_batch_entries": False,
            })

    df = pd.DataFrame(rows)
    if not df.empty:
        df["batch_status"] = df["mb_archived"].apply(
            lambda x: "Archived" if x in (1, True, "1") else ("Active" if pd.notna(x) else None)
        )
        for int_col in ["bundle_id", "master_batch_id", "tutor_id"]:
            if int_col in df.columns:
                df[int_col] = df[int_col].astype("Int64")
    return df


# ==========================================================================
# STEP 3: FILTER
# ==========================================================================

def filter_catalog(df: pd.DataFrame) -> "tuple[pd.DataFrame, pd.DataFrame]":
    """Returns (cleaned_df, removed_df) — removed_df has a 'removal_reason' column."""
    df = df.copy()
    df["removal_reason"] = None

    # --- 1. Explicit exclusion list (matched on master_batch_id) ---
    excluded_mask = df["master_batch_id"].isin(EXCLUDED_BATCH_IDS)
    df.loc[excluded_mask, "removal_reason"] = "explicit_exclusion_list"

    matched_ids = set(df.loc[excluded_mask, "master_batch_id"].dropna().astype(int).tolist())
    missing_ids = set(EXCLUDED_BATCH_IDS) - matched_ids
    if missing_ids:
        print(f"[WARN] {len(missing_ids)} batch_ids from your exclusion list were NOT "
              f"found in this page/batch of the catalog: {sorted(missing_ids)} "
              f"(expected if you're testing on a partial page — check the full run)")

    # --- 2. Archived batches ---
    archived_mask = df["batch_status"].astype(str).str.lower().eq("archived")
    df.loc[archived_mask & df["removal_reason"].isna(), "removal_reason"] = "archived"

    # --- 3. Test / flipbook / audio content — check bundle_name + batch_name ---
    keyword_mask = pd.Series(False, index=df.index)
    matched_keyword_col = {}
    for col in ["bundle_name", "batch_name"]:
        if col not in df.columns:
            continue
        col_lower = df[col].astype(str).str.lower()
        for kw in CONTENT_KEYWORDS:
            hits = col_lower.str.contains(kw, na=False)
            newly_matched = hits & ~keyword_mask
            for idx in df.index[newly_matched]:
                matched_keyword_col[idx] = f"{col}~'{kw}'"
            keyword_mask |= hits

    for idx, reason in matched_keyword_col.items():
        if pd.isna(df.at[idx, "removal_reason"]):
            df.at[idx, "removal_reason"] = f"keyword_match:{reason}"

    removed_df = df[df["removal_reason"].notna()].copy()
    cleaned_df = df[df["removal_reason"].isna()].drop(columns=["removal_reason"])

    return cleaned_df, removed_df


# ==========================================================================
# MAIN
# ==========================================================================

def main():
    parser = argparse.ArgumentParser(description="Fetch + clean the full Vyoma batch catalog (Stage 1, alt)")
    parser.add_argument("--apikey", type=str, default=None)
    parser.add_argument("--per_page", type=int, default=100)
    parser.add_argument("--raw_out", type=str, default="course_catalog_raw_export.csv")
    parser.add_argument("--clean_out", type=str, default="course_catalog.csv")
    parser.add_argument("--log_out", type=str, default="removed_rows_log.csv")
    parser.add_argument("--calls_per_minute", type=float, default=DEFAULT_CALLS_PER_MINUTE)
    args = parser.parse_args()

    config = load_config(CONFIG_PATH)
    apikey = args.apikey or config.get("api_key") or config.get("apikey")
    org_id = config.get("org_id", 683)

    if not apikey:
        print("[ERROR] No API key found. Pass --apikey or set api_key in config.yaml")
        sys.exit(1)

    output_folder = resolve_output_folder(config, Path(__file__))
    limiter = RateLimiter(args.calls_per_minute)

    n_rows_written = 0
    n_removed = 0
    n_errors = 0
    start_time = datetime.now()
    clean_path = output_folder / args.clean_out

    try:
        print(f"[INFO] Fetching full batch catalog for org_id={org_id} ...")
        courses = fetch_all_courses(apikey, org_id, limiter, args.per_page)
        if not courses:
            print("[WARN] No bundles returned at all. Check credentials / endpoint availability.")
            n_errors += 1
            return

        raw_df = flatten_courses(courses)
        if raw_df.empty:
            print("[WARN] Bundles were returned but could not be parsed into rows.")
            n_errors += 1
            return

        raw_path = output_folder / args.raw_out
        raw_df.to_csv(raw_path, index=False, encoding="utf-8-sig")
        print(f"\n[RESULT] {len(raw_df)} raw batch rows written to {raw_path.resolve()}")

        no_batch_count = int(raw_df["no_batch_entries"].sum()) if "no_batch_entries" in raw_df.columns else 0
        if no_batch_count:
            print(f"[NOTE] {no_batch_count} bundles had no batch entries at all (empty 'batch' array).")

        cleaned_df, removed_df = filter_catalog(raw_df)
        cleaned_df = cleaned_df.drop(columns=["no_batch_entries"], errors="ignore")

        log_path = output_folder / args.log_out
        # Written once, after every page has been fetched and filtered —
        # a crash before this point never leaves a partial output file.
        cleaned_df.to_csv(clean_path, index=False, encoding="utf-8-sig")
        removed_df.to_csv(log_path, index=False, encoding="utf-8-sig")
        n_rows_written = len(cleaned_df)
        n_removed = len(removed_df)

        print(f"[RESULT] {n_rows_written} rows kept -> {clean_path.resolve()}")
        print(f"[RESULT] {n_removed} rows removed -> {log_path.resolve()}\n")

        if not removed_df.empty:
            reason_summary = removed_df["removal_reason"].apply(lambda r: r.split(":")[0]).value_counts()
            print("Removal breakdown:")
            print(reason_summary.to_string())

    except Exception as e:
        print(f"[ERROR] {e}")
        n_errors += 1
    finally:
        end_time = datetime.now()
        summary = {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_sec": round((end_time - start_time).total_seconds(), 1),
            "rows_written": n_rows_written,
            "rows_removed": n_removed,
            "errors": n_errors,
            "output_file": str(clean_path),
        }
        send_run_report(STAGE_NAME, summary, config)


if __name__ == "__main__":
    with PipelineRunLogger(STAGE_NAME, Path(__file__), args_summary=str(sys.argv[1:])):
        main()
