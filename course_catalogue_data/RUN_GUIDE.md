# course_catalogue_data — Run Guide

Fetches the whole course catalogue and saves it as one CSV, unfiltered. One API call, a few seconds. Full documentation:
[COURSE_CATALOGUE_DATA.md](COURSE_CATALOGUE_DATA.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid Edmingle key. No config file, input folder or notifications are used.
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.

## Step by step
1. Activate the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate` — your prompt now starts with `(.venv)`.
2. Go to the scripts folder: `cd /home/projectdev/ela_datasets/course_catalogue_data/scripts`
3. Run it (no arguments):
   ```bash
   python3 course_catalogue_data.py
   ```
4. Wait for `Success! Data saved successfully.` with a record and column count.

## Check the result
- `output/course_catalogue_data.csv` was just rewritten. At the last run: **566 records, 61 columns**, including `ingested_at`.
- **Don't count rows with `wc -l`.** Several fields contain line breaks inside quoted text, so `wc -l` reports tens of
  thousands of lines. Use the count the script prints.

## If something goes wrong
| Symptom | What to do |
|---|---|
| `ModuleNotFoundError: pandas` | You forgot step 1 — activate the environment. |
| `No data returned from API` | Read the status/error printed above it; usually the API key. |

This dataset is short and has no long-running mode, so it has no tmux section.
