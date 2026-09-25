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
OUTPUT_FILE = os.path.join(SCRIPT_DIR, "..", "output", "course_catalogue_data.csv")

_edmingle = common.edmingle_settings(need_institute=True, path=CREDENTIALS_PATH)
ORGANIZATION_ID = int(_edmingle["organization_id"])
API_KEY = _edmingle["api_key"]

BASE_URL = f"{_edmingle['base_url']}/institute/{_edmingle['institute_id']}/courses/catalogue"
HEADERS = {"apikey": API_KEY, "ORGID": _edmingle["organization_id"]}
REQUEST_TIMEOUT_SECONDS = 120  # the catalogue takes ~15 s; never wait forever


def fetch_courses():
    response = requests.get(BASE_URL, headers=HEADERS, params={"org_id": ORGANIZATION_ID}, timeout=REQUEST_TIMEOUT_SECONDS)
    print("Status:", response.status_code)
    if response.status_code != 200:
        print("Error from API:")
        print(response.text)
        return []
    data = response.json()
    print("Response keys:", data.keys())
    return data.get("response", [])


def main():
    print("\nFetching course catalogue...\n")
    courses_list = fetch_courses()

    if not courses_list:
        print("\n❌ No data returned from API")
        return

    df = pd.json_normalize(courses_list)  # no column filtering -- keep every raw column Edmingle returns
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    df["ingested_at"] = datetime.now()
    df.to_csv(OUTPUT_FILE, index=False)

    print("\n====================================")
    print("Success! Data saved successfully.")
    print("Total records:", len(df))
    print("Total columns:", len(df.columns))
    print("Columns saved:", list(df.columns))
    print("File saved at:", OUTPUT_FILE)
    print("====================================")


if __name__ == "__main__":  # never runs on import -- keeps a verification import side-effect-free
    main()
