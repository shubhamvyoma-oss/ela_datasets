import os
import sys

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set
# before any third-party import below.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

import requests
import pandas as pd
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(os.path.abspath(__file__)).resolve().parents[2]))
import common

# ═══════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════

# Security key for Edmingle
# Load shared Edmingle credentials from ../../credentials.yaml (single
# source of truth for every pipeline under ela_datasets/)
_SCRIPT_DIR_CREDS = os.path.dirname(os.path.abspath(__file__))
_CREDENTIALS_PATH = os.path.normpath(os.path.join(_SCRIPT_DIR_CREDS, "..", "..", "credentials.yaml"))

_edmingle = common.load_credentials(_CREDENTIALS_PATH)

API_KEY = _edmingle.get("api_key", "")

# Organizational IDs
ORGANIZATION_ID = str(_edmingle.get("organization_id", ""))
INSTITUTE_ID = str(_edmingle.get("institute_id", ""))

# Where we save the final cleaned file (the pipeline's output/ folder)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "output", "course_batch_merge.csv")

# Web addresses for the data
CATALOGUE_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1/institute/483/courses/catalogue"
BATCHES_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1/short/masterbatch"

# The specific columns needed for the Power BI report
# Order and names match the verified 41-column reference file (vyoma_master.csv)
OUTPUT_COLUMNS = [
    "bundle_id", "bundle_name", "batch_id", "batch_name",
    "batch_status", "start_date", "end_date", "tutor_name", "tutor_id",
    "batch_enrollment_count", "Course Name", "Tutors", "Tutord Ids",
    "Course Ids", "Subject", "Level", "Language", "Examination", "Type",
    "Course Division", "Certificate", "Course Sponsor",
    "Course Title Sanskrit", "Status", "Number of Lectures", "Duration",
    "Personas", "Computer Based Assessment", "Product ID", "SSS Category",
    "Viniyoga", "Adhyayanam Category", "Term of Course",
    "Position in Funnel", "Division", "Catalogue_Match",
    "bundle_enrollment_count", "Is_Latest_Batch", "Has_Batch",
    "Catalogue_Status", "Final_Status"
]

# Headers for the web requests
HEADERS = {
    "apikey": API_KEY,
    "ORGID": ORGANIZATION_ID,
    "Accept": "application/json"
}


# ── Helper Function to Log Progress ─────────────────────────────────
def log_progress(message):
    # Get current time for logging
    now = datetime.now().strftime("%H:%M:%S")
    # Print message with timestamp
    print(f"{now} - {message}")


# ── Function to Fetch Catalogue ─────────────────────────────────────
def get_catalogue():
    # Log the start of operation
    log_progress("Fetching Course Catalogue...")

    # Make the API call
    response = requests.get(CATALOGUE_URL, headers=HEADERS)

    # Check if request was successful
    if response.status_code != 200:
        print("Error fetching catalogue!")
        print("Status Code:", response.status_code)
        return pd.DataFrame()

    # Convert response to Python dictionary
    data = response.json()

    # Get the list of courses
    rows = data.get("response", [])

    # Convert to DataFrame
    return pd.DataFrame(rows)


# ── Function to Fetch Batches for One Status ────────────────────────
def get_batches_by_status(status_code, status_label):
    # Build the URL with filters
    url = f"{BATCHES_URL}?status={status_code}&page=1&per_page=1000&organization_id={ORGANIZATION_ID}"

    # Make the API call
    response = requests.get(url, headers=HEADERS)

    # Convert response to dictionary
    data = response.json()

    # Get courses list
    courses = data.get("courses", [])

    # List to store all batch rows
    batch_rows = []

    # Loop through each course
    i = 0
    while i < len(courses):
        course = courses[i]

        # Get batches inside this course
        batches = course.get("batch", [{}])

        # Loop through each batch
        j = 0
        while j < len(batches):
            b = batches[j]

            # Create one row for this batch
            row = {
                "bundle_id": course.get("bundle_id"),
                "bundle_name": str(course.get("bundle_name", "")).strip(),
                "batch_id": b.get("class_id"),
                "batch_name": str(b.get("class_name", "")).strip(),
                "batch_status": status_label,
                "start_date": b.get("start_date"),
                "end_date": b.get("end_date"),
                "tutor_name": b.get("tutor_name"),
                # NOTE: exact Edmingle key for tutor id is unconfirmed here — verify
                # against a raw batch API response and adjust the fallback order below.
                "tutor_id": b.get("tutor_id") or b.get("faculty_id") or b.get("tutorId"),
                "batch_enrollment_count": b.get("admitted_students")
            }
            batch_rows.append(row)
            j = j + 1

        i = i + 1

    return batch_rows


# ── Function to Fetch All Batches ───────────────────────────────────
def get_all_batches():
    log_progress("Fetching All Batches...")

    # Status mapping: code -> label
    status_codes = {0: "Active", 1: "Archived", 3: "Completed"}

    all_data = []

    # Loop through each status
    for code, label in status_codes.items():
        # Get batches for this status
        rows = get_batches_by_status(code, label)
        # Add to main list
        all_data.extend(rows)

    # Convert to DataFrame
    return pd.DataFrame(all_data)


# ── Function to Remove Test Batches ────────────────────────────────
def filter_test_batches(df):
    # Create a clean copy
    clean_df = df.copy()

    # loop to remove test batches
    i = 0
    while i < len(clean_df):
        batch_name = str(clean_df.loc[i, "batch_name"]).lower()
        if "test batch" in batch_name:
            clean_df = clean_df.drop(i).reset_index(drop=True)
            # Do not increase i because we dropped a row
        else:
            i = i + 1

    log_progress(f"Filtered out test batches. Remaining rows: {len(clean_df)}")
    return clean_df


# ── Function to Mark Latest Batch ───────────────────────────────────
def mark_latest_batch(df):
    # Make a copy to avoid modifying original
    working_df = df.copy()

    # Convert start_date to number for sorting
    working_df["_sort_date"] = pd.to_numeric(working_df["start_date"], errors="coerce").fillna(0)

    # Sort by bundle and then newest date first
    working_df = working_df.sort_values(["bundle_id", "_sort_date"], ascending=[True, False])
    working_df = working_df.reset_index(drop=True)

    # Add new column
    working_df["Is_Latest_Batch"] = 0

    last_bundle = None
    i = 0
    while i < len(working_df):
        current_bundle = working_df.loc[i, "bundle_id"]

        # If new bundle, mark as latest
        if current_bundle != last_bundle:
            working_df.loc[i, "Is_Latest_Batch"] = 1
            last_bundle = current_bundle

        i = i + 1

    # Remove temporary column
    working_df = working_df.drop(columns=["_sort_date"])
    return working_df


# ── Function to Apply Business Logic ────────────────────────────────
def apply_business_logic(df):
    working_df = df.copy()

    working_df["Has_Batch"] = 1
    working_df["Catalogue_Status"] = working_df["Status"]
    working_df["Status_Adjustment_Reason"] = ""
    working_df["Final_Status"] = "Completed"
    working_df["Include_In_Course_Count"] = 0

    valid_statuses = ["Completed", "Ongoing", "Upcoming"]

    i = 0
    while i < len(working_df):
        if working_df.loc[i, "Is_Latest_Batch"] == 1:
            raw_status = str(working_df.loc[i, "Status"]).strip()
            working_df.loc[i, "Final_Status"] = raw_status

            if raw_status in valid_statuses:
                working_df.loc[i, "Include_In_Course_Count"] = 1
        i = i + 1

    return working_df


# ── Function to Add Courses Without Batches ─────────────────────────
def add_courses_without_batches(merged_df, catalogue_df):
    # Get bundle ids that already have batches
    existing_ids = merged_df["bundle_id"].unique()

    # Find missing courses
    missing = catalogue_df.copy()
    i = 0
    while i < len(missing):
        if missing.loc[i, "Bundle id"] in existing_ids:
            missing = missing.drop(i).reset_index(drop=True)
        else:
            i = i + 1

    # Prepare missing rows
    missing["bundle_id"] = missing["Bundle id"]
    missing["Has_Batch"] = 0
    missing["Is_Latest_Batch"] = 1
    missing["Include_In_Course_Count"] = 0
    missing["Final_Status"] = missing["Status"]
    missing["Catalogue_Match"] = True

    # Combine both dataframes
    final_df = pd.concat([merged_df, missing], ignore_index=True)
    return final_df


# ── MAIN EXECUTION ──────────────────────────────────────────────────
def main():
    try:
        log_progress("Starting Master Build Process...")

        # Step 1: Fetch data
        cat_df = get_catalogue()
        batch_df = get_all_batches()

        # Step 2: Filter test batches
        batch_df = filter_test_batches(batch_df)

        # Step 3: Merge catalogue and batches
        # indicator=True adds a '_merge' column so we know which batch rows
        # actually found a matching catalogue entry (used for Catalogue_Match below)
        merged = batch_df.merge(
            cat_df, left_on="bundle_id", right_on="Bundle id",
            how="left", indicator=True
        )
        merged["Catalogue_Match"] = merged["_merge"] == "both"
        merged = merged.drop(columns=["_merge"])

        # Step 3b: Compute bundle_enrollment_count (sum of all batch enrollments per course)
        bundle_totals = (
            merged.groupby("bundle_id")["batch_enrollment_count"]
            .sum()
            .reset_index()
            .rename(columns={"batch_enrollment_count": "bundle_enrollment_count"})
        )
        merged = merged.merge(bundle_totals, on="bundle_id", how="left")

        # Step 4: Apply logic
        merged = mark_latest_batch(merged)
        merged = apply_business_logic(merged)

        # Step 5: Add courses with no batches
        final_df = add_courses_without_batches(merged, cat_df)

        # Step 6: Format dates
        final_df["start_date"] = pd.to_datetime(
            pd.to_numeric(final_df["start_date"], errors='coerce'),
            unit='s', errors='coerce'
        ).dt.date

        final_df["end_date"] = pd.to_datetime(
            pd.to_numeric(final_df["end_date"], errors='coerce'),
            unit='s', errors='coerce'
        ).dt.date

        # Step 7: Select only required columns
        # NOTE: unlike before, missing columns are now WARNED about instead of
        # silently dropped, so schema gaps (like the earlier tutor_id issue)
        # surface immediately in the log instead of disappearing quietly.
        existing_cols = []
        missing_cols = []
        i = 0
        while i < len(OUTPUT_COLUMNS):
            col = OUTPUT_COLUMNS[i]
            if col in final_df.columns:
                existing_cols.append(col)
            else:
                missing_cols.append(col)
            i = i + 1

        if missing_cols:
            log_progress(f"WARNING: these expected columns were not found in the fetched data and will be MISSING from the output: {missing_cols}")

        # Final cleanup and save (existing_cols already preserves OUTPUT_COLUMNS order)
        final_report = final_df[existing_cols].fillna("")

        # Save to CSV
        final_report.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

        log_progress(f"SUCCESS! Saved {len(final_report)} rows to file.")

    except Exception as e:
        print("ERROR occurred during execution!")
        print("Error message:", str(e))


# Run the program
if __name__ == "__main__":
    main()