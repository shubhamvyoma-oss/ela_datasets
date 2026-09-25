# course_catalogue_data — Run Guide

Fetches the full course catalogue into one CSV (one API call, seconds). Details: [COURSE_CATALOGUE_DATA.md](COURSE_CATALOGUE_DATA.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid key. No config, input or notifications.
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.

## Run
```bash
source /home/projectdev/ela_datasets/.venv/bin/activate
cd /home/projectdev/ela_datasets/course_catalogue_data/scripts
python3 course_catalogue_data.py
```
Finished with `Success! Data saved successfully.`

## Check
- `output/course_catalogue_data.csv` was rewritten (last run: 566 records, 61 columns).
- Don't count rows with `wc -l` — quoted fields contain line breaks. Use the count the script prints.

## If it fails
`No data returned from API` → read the status printed above it (usually the API key).

No tmux section: too short to need one.
