import os
import sys

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set before any third-party import.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

from datetime import datetime

import pandas as pd
import requests

sys.path.insert(0, os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..")))
import common

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CREDENTIALS_PATH = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "..", "credentials.yaml"))
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "output", "course_batch_merge.csv")

_edmingle = common.load_credentials(CREDENTIALS_PATH)
API_KEY = _edmingle.get("api_key", "")
ORGANIZATION_ID = str(_edmingle.get("organization_id", ""))
INSTITUTE_ID = str(_edmingle.get("institute_id", ""))

CATALOGUE_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1/institute/483/courses/catalogue"
BATCHES_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1/short/masterbatch"

# Order/names match the verified 41-column reference file (vyoma_master.csv), used for Power BI.
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

HEADERS = {"apikey": API_KEY, "ORGID": ORGANIZATION_ID, "Accept": "application/json"}


def log_progress(message):
    print(f"{datetime.now():%H:%M:%S} - {message}")


def get_catalogue():
    log_progress("Fetching Course Catalogue...")
    response = requests.get(CATALOGUE_URL, headers=HEADERS)
    if response.status_code != 200:
        print("Error fetching catalogue!")
        print("Status Code:", response.status_code)
        return pd.DataFrame()
    return pd.DataFrame(response.json().get("response", []))


def get_batches_by_status(status_code, status_label):
    url = f"{BATCHES_URL}?status={status_code}&page=1&per_page=1000&organization_id={ORGANIZATION_ID}"
    data = requests.get(url, headers=HEADERS).json()
    return [
        {
            "bundle_id": course.get("bundle_id"),
            "bundle_name": str(course.get("bundle_name", "")).strip(),
            "batch_id": b.get("class_id"),
            "batch_name": str(b.get("class_name", "")).strip(),
            "batch_status": status_label,
            "start_date": b.get("start_date"),
            "end_date": b.get("end_date"),
            "tutor_name": b.get("tutor_name"),
            # NOTE: exact Edmingle key for tutor id is unconfirmed -- verify against a raw
            # batch API response and adjust the fallback order below.
            "tutor_id": b.get("tutor_id") or b.get("faculty_id") or b.get("tutorId"),
            "batch_enrollment_count": b.get("admitted_students"),
        }
        for course in data.get("courses", [])
        for b in course.get("batch", [{}])
    ]


def get_all_batches():
    log_progress("Fetching All Batches...")
    status_codes = {0: "Active", 1: "Archived", 3: "Completed"}
    all_data = [row for code, label in status_codes.items() for row in get_batches_by_status(code, label)]
    return pd.DataFrame(all_data)


def filter_test_batches(df):
    clean_df = df[~df["batch_name"].str.lower().str.contains("test batch", na=False)].reset_index(drop=True)
    log_progress(f"Filtered out test batches. Remaining rows: {len(clean_df)}")
    return clean_df


def mark_latest_batch(df):
    working_df = df.copy()
    working_df["_sort_date"] = pd.to_numeric(working_df["start_date"], errors="coerce").fillna(0)
    working_df = working_df.sort_values(["bundle_id", "_sort_date"], ascending=[True, False]).reset_index(drop=True)
    # First row per bundle after the sort above is the newest -- exactly "latest batch".
    working_df["Is_Latest_Batch"] = (~working_df.duplicated(subset="bundle_id")).astype(int)
    return working_df.drop(columns=["_sort_date"])


def apply_business_logic(df):
    working_df = df.copy()
    working_df["Has_Batch"] = 1
    working_df["Catalogue_Status"] = working_df["Status"]
    working_df["Final_Status"] = "Completed"
    working_df["Include_In_Course_Count"] = 0

    valid_statuses = ["Completed", "Ongoing", "Upcoming"]
    is_latest = working_df["Is_Latest_Batch"] == 1
    # .map(str) matches Python's str(x) semantics exactly (missing -> literal "nan" text) --
    # .astype(str) on this column's dtype would instead preserve missingness, silently changing
    # output content for unmatched-catalogue rows.
    working_df.loc[is_latest, "Final_Status"] = working_df.loc[is_latest, "Status"].map(lambda v: str(v).strip())
    is_valid = is_latest & working_df["Final_Status"].isin(valid_statuses)
    working_df.loc[is_valid, "Include_In_Course_Count"] = 1
    return working_df


def add_courses_without_batches(merged_df, catalogue_df):
    existing_ids = merged_df["bundle_id"].unique()
    missing = catalogue_df[~catalogue_df["Bundle id"].isin(existing_ids)].reset_index(drop=True)
    missing["bundle_id"] = missing["Bundle id"]
    missing["Has_Batch"] = 0
    missing["Is_Latest_Batch"] = 1
    missing["Include_In_Course_Count"] = 0
    missing["Final_Status"] = missing["Status"]
    missing["Catalogue_Match"] = True
    return pd.concat([merged_df, missing], ignore_index=True)


def main():
    try:
        log_progress("Starting Master Build Process...")
        cat_df = get_catalogue()
        batch_df = filter_test_batches(get_all_batches())

        # indicator=True adds a '_merge' column so we know which batch rows actually found a
        # matching catalogue entry (used for Catalogue_Match below).
        merged = batch_df.merge(cat_df, left_on="bundle_id", right_on="Bundle id", how="left", indicator=True)
        merged["Catalogue_Match"] = merged["_merge"] == "both"
        merged = merged.drop(columns=["_merge"])

        bundle_totals = (
            merged.groupby("bundle_id")["batch_enrollment_count"]
            .sum()
            .reset_index()
            .rename(columns={"batch_enrollment_count": "bundle_enrollment_count"})
        )
        merged = merged.merge(bundle_totals, on="bundle_id", how="left")
        merged = apply_business_logic(mark_latest_batch(merged))
        final_df = add_courses_without_batches(merged, cat_df)

        final_df["start_date"] = pd.to_datetime(
            pd.to_numeric(final_df["start_date"], errors="coerce"), unit="s", errors="coerce"
        ).dt.date
        final_df["end_date"] = pd.to_datetime(
            pd.to_numeric(final_df["end_date"], errors="coerce"), unit="s", errors="coerce"
        ).dt.date

        # Missing columns are WARNED about instead of silently dropped, so schema gaps (like the
        # earlier tutor_id issue) surface immediately in the log instead of disappearing quietly.
        existing_cols = [c for c in OUTPUT_COLUMNS if c in final_df.columns]
        missing_cols = [c for c in OUTPUT_COLUMNS if c not in final_df.columns]
        if missing_cols:
            log_progress(f"WARNING: these expected columns were not found in the fetched data and will be MISSING from the output: {missing_cols}")

        final_report = final_df[existing_cols].fillna("")
        final_report.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
        log_progress(f"SUCCESS! Saved {len(final_report)} rows to file.")

    except Exception as e:
        print("ERROR occurred during execution!")
        print("Error message:", str(e))


if __name__ == "__main__":
    main()
