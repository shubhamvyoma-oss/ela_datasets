# Country-wise User Analytics Export

## What this pipeline does
This pipeline pulls a per-user list of country/region/engagement analytics from Edmingle, one row per user, and writes it to a local CSV. For each user it captures their user id, name, email, contact number, country, region, total time spent, total session count, and last-seen/created-at timestamps.

This is a **different Edmingle endpoint** than the enrollment/attendance pipelines use (`/user/useranalyticslist`, a user-level analytics list — not enrollment records and not attendance records). It returns a `_id` per row that can be joined onto the enrollment file if needed, but it is not itself an enrollment or attendance export.

**Note:** this data domain (per-user country/engagement analytics) was never part of the original 5 documented pipelines for this project. It is an additional, later-added export. Whoever owns the project's requirements/documentation should be told about it if it's meant to become a permanent, ongoing pipeline.

## Folder layout

This folder is split into two subfolders:

- `scripts/` -- all source code: `edmingle_user_country_list_export_v3.py`
  and `edmingle_user_country_list_config.json`. Run everything from inside
  `scripts/`.
- `output/` -- generated data files: `user_country_list.csv` and (once the
  script has run at least once) `user_country_list_checkpoint.json`.

Compiled bytecode (`__pycache__`) for every pipeline under `ela_datasets/`
is redirected to a single shared `ela_datasets/.pycache/` directory (via
`sys.pycache_prefix`, set at the top of `edmingle_user_country_list_export_v3.py`
before any third-party import) instead of a separate `__pycache__` folder
per pipeline.

## Edmingle endpoint used
- `GET {base_url}/user/useranalyticslist`
- Headers: `apikey`, `ORGID`
- Query params: `page`, `per_page`, `is_export=0`, `filter_key` (e.g. `geoLocationInfo.country`), `sort_order`, `start_date`, `end_date`

## Business rules that determine correct data
- **Retry-with-rollback per page.** Each page is fetched with up to 5 retries (network errors and transient HTTP errors are retried with backoff; a 429 triggers a 300-second cooldown and resets the rate limiter; 400/401/403/404 are treated as permanent and not retried). If a page ultimately fails after retries are exhausted, the script stops immediately **without** writing that page's rows and **without** advancing the checkpoint. Nothing partial is ever committed, so a re-run resumes cleanly from the same page with no duplicate or skipped users.
- **Crash-safe resume via byte-offset truncation.** Before each page is processed, the CSV's current byte size is tracked. Rows are appended to the CSV only after a successful fetch, and the checkpoint (last completed page + CSV byte offset + cumulative rows written) is saved immediately after, via atomic file replace. If the process dies between the CSV append and the checkpoint save, the next run truncates the CSV back to the last known-good checkpoint offset before resuming — this rolls back any partially-written rows from a crash and guarantees no duplicate/orphaned rows either way.
- **Timestamps written both ways.** `last_seen` and `created_at` are written as raw epoch (`last_seen_epoch`, `created_at_epoch`, for machine use / re-processing) **and** as IST-formatted strings (`last_seen_ist`, `created_at_ist`, in `dd-mm-yyyy HH:MM:SS` format, matching the `enrollment_day` dd-mm-yyyy convention used elsewhere in this project). The epoch columns are never dropped, only supplemented with the formatted ones.
- **Rate limit capped at 30 requests/minute** by default (`rate_limit_per_minute` in config), enforced by a sliding 60-second window.
- **Windows file-lock retry (documented, not Linux-specific in effect).** File writes (CSV header, row appends, checkpoint save, truncate-on-resume) retry up to 6 times with exponential backoff on `PermissionError`/`OSError`. This is called out in the code as a known Windows condition — antivirus or OneDrive can transiently lock a just-written file — and is treated as a transient, retryable condition, not a real error.

## Configuration
- `../../credentials.yaml` (shared, two levels up from `scripts/`, used by every pipeline under `ela_datasets/`): `edmingle.api_key` -> mapped to `apikey`, `edmingle.organization_id` -> mapped to `orgid`. These are merged into the config dict in memory at load time and are never read from this folder's own config file or from argv (per the fix made earlier today, which corrected a wrong-API-key bug).
- `edmingle_user_country_list_config.json` (in `scripts/`) holds the non-credential settings: `base_url`, `filter_key`, `sort_order`, `per_page`, `start_date`, `end_date`, `rate_limit_per_minute`, `output_csv`, `checkpoint_file`.
- There is **no** `notifications.yaml` use in this script — it has no email/alerting capability. On an unrecoverable page failure it logs the failure and exits with status 1; it does not send any notification.

Current values in `edmingle_user_country_list_config.json`:
```json
{
  "base_url": "https://vyoma-api.edmingle.com/nuSource/api/v1",
  "filter_key": "geoLocationInfo.country",
  "sort_order": -1,
  "per_page": 500,
  "start_date": "01-01-2020T00:00:00+05:30",
  "end_date": "19-08-2026T23:59:59+05:30",
  "rate_limit_per_minute": 30,
  "output_csv": "user_country_list.csv",
  "checkpoint_file": "user_country_list_checkpoint.json"
}
```

## How to run
From `scripts/` on the server:
```
cd scripts
python3 edmingle_user_country_list_export_v3.py --config edmingle_user_country_list_config.json
```
`--config` defaults to `edmingle_user_country_list_config.json` already, so it can also be run with no arguments from inside `scripts/`. The config path (if relative) is resolved relative to the script's own directory (`SCRIPT_DIR = Path(__file__).resolve().parent`), not the current working directory. The output CSV and checkpoint file are resolved relative to `SCRIPT_DIR.parent / "output"`, so they always land in `country_wise_data/output/` regardless of where the command is invoked from.

It is safe to Ctrl+C or let a 429 penalty run out — just re-run the same command; the checkpoint/byte-offset logic resumes correctly.

## Output files produced
Both written to `output/` (`country_wise_data/output/`):
- `user_country_list.csv` — the per-user analytics rows. Columns: `user_id, name, email, contact_number, country, region, time_spent_seconds, total_sessions, last_seen_epoch, last_seen_ist, created_at_epoch, created_at_ist, source_page`.
- `user_country_list_checkpoint.json` — resume state: `last_completed_page`, `csv_byte_offset`, `rows_written`. Not present until the script has been run at least once (as of this writing, only the CSV header exists in `output/` and no checkpoint file has been created yet).

## Known limitations / things to watch for
- **Not part of the original documented scope.** This is a newer/additional data pull that was never among the original 5 documented pipelines for this project. If it's meant to be permanent, the project's requirements documentation should be updated to include it.
- **Referenced example config file doesn't exist.** The script's docstring/usage instructions tell a new user to copy `edmingle_user_country_list_config.example.json` to `edmingle_user_country_list_config.json` and fill it in, but no `.example.json` file currently exists in `scripts/`. The real config file already exists there and is in use, so this only matters for someone trying to bootstrap the pipeline from scratch.
- **"Remaining rows" depends on the API.** The rows-remaining figure in each log line is computed from the API's own `total_rows` field in `page_context`; if a response doesn't include `total_rows`, remaining is logged as unknown until a page does include it.
- **Permanent vs. transient error classification is fixed.** Only HTTP 400/401/403/404 are treated as permanent (no retry, page fails immediately); everything else (including unexpected 5xx codes) is retried up to 5 times with backoff before giving up.
- **No email/alerting on failure.** Unlike pipelines that use a `notifications.yaml`, this script only logs to stdout/stderr and exits non-zero — failures need to be caught by whoever/whatever is monitoring the job, not by an automated alert from this script itself.
