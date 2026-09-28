# course_catalogue_data — Run Guide

Downloads the full, raw course catalogue from Edmingle into one file — one API call, takes seconds. Full technical detail: [COURSE_CATALOGUE_DATA.md](COURSE_CATALOGUE_DATA.md).

## Before you start
- Make sure `credentials.yaml` (shared) has a working key.
- Nothing else is needed — no input file, no email setup. `scripts/course_catalogue_data_config.json` only holds the request timeout, and the default is fine.
- Only run one pipeline at a time.

## Steps
1. Turn on the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
2. Go to the folder: `cd /home/projectdev/ela_datasets/course_catalogue_data/scripts`
3. Run it: `python3 course_catalogue_data.py`
4. Wait for `Success! Data saved successfully.`

## How to tell it worked
- `output/course_catalogue_data.csv` was just rewritten. Use the row/column count the script prints, not `wc -l` — some course descriptions have line breaks in them, which throws off a plain line count.

## If something goes wrong
- **`No data returned from API`** — look at the status code printed just above it; it's almost always the API key.

No need for tmux here — it finishes in a few seconds.
