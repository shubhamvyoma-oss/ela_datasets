import os
import sys

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set
# before any third-party import below.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

import requests
import pandas as pd
import yaml
from datetime import datetime

# ================= CONFIG =================
# Load shared Edmingle credentials from ../../credentials.yaml (single
# source of truth for every pipeline under ela_datasets/)
_SCRIPT_DIR_CREDS = os.path.dirname(os.path.abspath(__file__))
_CREDENTIALS_PATH = os.path.normpath(os.path.join(_SCRIPT_DIR_CREDS, "..", "..", "credentials.yaml"))

with open(_CREDENTIALS_PATH, "r", encoding="utf-8") as _f:
    _credentials = yaml.safe_load(_f) or {}
_edmingle = _credentials.get("edmingle", {})

INSTITUTE_ID = int(_edmingle.get("institute_id", 0))
ORGANIZATION_ID = int(_edmingle.get("organization_id", 0))
API_KEY = _edmingle.get("api_key", "")  # Security key for Edmingle

BASE_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1/institute/483/courses/catalogue"

HEADERS = {
    "apikey": API_KEY,
    "ORGID": "683"
}

# Output file will be saved in the pipeline's output/ folder
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "..", "output", "course_catalogue_data.csv")


# ================= FETCH FUNCTION =================
def fetch_courses():
    # Create parameters for the API request
    params = {
        "org_id": ORGANIZATION_ID
    }
    
    # Make the API call to get course data
    response = requests.get(BASE_URL, headers=HEADERS, params=params)
    
    # Show the status code so we know if request succeeded
    print("Status:", response.status_code)
    
    # Check if request was successful
    if response.status_code != 200:
        print("Error from API:")
        print(response.text)
        return []
    
    # Convert response to Python dictionary
    data = response.json()
    
    # Print keys for debugging
    print("Response keys:", data.keys())
    
    # Return the list of courses (empty list if not found)
    return data.get("response", [])


# ================= CLEAN COLUMN NAMES FUNCTION =================
def clean_column_names(dataframe):
    # This function makes column names clean and consistent
    new_column_names = []
    
    i = 0
    while i < len(dataframe.columns):
        old_name = dataframe.columns[i]
        # Remove extra spaces and convert to lowercase with underscores
        clean_name = old_name.strip().lower().replace(" ", "_")
        new_column_names.append(clean_name)
        i = i + 1
    
    # Update the dataframe with new column names
    dataframe.columns = new_column_names
    return dataframe


# ================= MAIN PART =================
def main():
    print("\nFetching course catalogue...\n")

    # Get the courses from API
    courses_list = fetch_courses()

    if courses_list:
        # Convert list of dictionaries into a pandas DataFrame
        # (no column filtering - keep every raw column the API returns)
        df = pd.json_normalize(courses_list)

        # Clean the column names (make them lowercase with underscores)
        df = clean_column_names(df)

        # Add a column to track when data was fetched
        df["ingested_at"] = datetime.now()

        # ================= SAVE TO CSV =================
        # Save the data to CSV file in the pipeline's output/ folder
        df.to_csv(OUTPUT_FILE, index=False)

        print("\n====================================")
        print("Success! Data saved successfully.")
        print("Total records:", len(df))
        print("Total columns:", len(df.columns))
        print("Columns saved:", list(df.columns))
        print("File saved at:", OUTPUT_FILE)
        print("====================================")

    else:
        print("\n❌ No data returned from API")


# Only run the pipeline when this file is executed directly -- not when it's
# imported (e.g. for a syntax/import verification check), so importing this
# module can never trigger a live Edmingle API call or a real output write.
if __name__ == "__main__":
    main()
