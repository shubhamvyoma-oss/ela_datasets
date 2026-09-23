# Country-wise User Analytics (3-stage: ip-driven + dial-code + merge)

## What this pipeline does

Determines each student's country from two independent signals, then
combines them into one file:

1. **Stage 1 -- `ip_driven_country_data.py`**: pulls per-user analytics
   from Edmingle's `/user/useranalyticslist` endpoint, one row per user,
   including a country derived by Edmingle itself from the user's
   geolocated IP (`geoLocationInfo.country`, via the `filter_key` config
   and returned as `filterValue`). Also captures user id, name, email,
   contact number, region, total time spent, session count, and
   last-seen/created-at timestamps.
2. **Stage 2 -- `dial_code_to_country.py`**: reads a manually-exported
   `Student-Export*.csv` (dropped into `input/` by hand -- this is a
   student roster export from Edmingle's admin panel, not fetched via the
   API) and derives a country guess from each student's
   `Contact Number Dial Code` (e.g. `+91` -> `India`).
3. **Stage 3 -- `merge_country_data.py`**: joins Stage 1 and Stage 2's
   output on email (case-insensitive) and produces one row per
   Student-Export student with three country columns -- see "Merge logic"
   below.

This data domain (per-user country/engagement analytics) was never part of
the original 5 documented pipelines for this project. It is an additional,
later-added export -- whoever owns the project's requirements/documentation
should be told about it if it's meant to become a permanent, ongoing
pipeline.

## Folder layout

This folder has three subfolders:

- `scripts/` -- all source code: `ip_driven_country_data.py` (Stage 1),
  `ip_driven_country_data_config.json`, `dial_code_to_country.py`
  (Stage 2), `merge_country_data.py` (Stage 3). Run everything from inside
  `scripts/`.
- `input/` -- **gitignored, real student PII** (names, emails, phone
  numbers, addresses, parent contacts). Drop a fresh
  `Student-Export*.csv` export here before running Stage 2. Nothing else
  in this pipeline writes to `input/` -- it's for manually-supplied files
  only.
- `output/` -- everything the three scripts generate: `user_country_list.csv`
  (+ its checkpoint, Stage 1), `Student-Export_with_country.csv` (Stage 2),
  `merged_country_data.csv` (Stage 3, the final result).

Compiled bytecode (`__pycache__`) for every pipeline under `ela_datasets/`
is redirected to the single shared `ela_datasets/.pycache/` directory (via
`sys.pycache_prefix`, set at the top of each of the 3 scripts) instead of a
separate `__pycache__` folder per pipeline.

## Stage 1: `ip_driven_country_data.py`

(Renamed from `edmingle_user_country_list_export_v3.py` -- same script,
same behavior, new name reflecting what it actually produces: an
IP-geolocation-driven country signal, to distinguish it from Stage 2's
dial-code-driven guess.)

- **Endpoint:** `GET {base_url}/user/useranalyticslist`
- Headers: `apikey`, `ORGID`. Query params: `page`, `per_page`,
  `is_export=0`, `filter_key` (`geoLocationInfo.country`), `sort_order`,
  `start_date`, `end_date`.
- **Retry-with-rollback per page.** Up to 5 retries (backoff on
  network/transient errors; 429 triggers a 300s cooldown and resets the
  rate limiter; 400/401/403/404 are permanent, no retry). A page that
  ultimately fails is never partially committed -- nothing is written and
  the checkpoint doesn't advance, so a re-run resumes cleanly.
- **Crash-safe resume via byte-offset truncation** -- same pattern used
  throughout `ela_datasets/`: CSV byte size tracked before each page,
  checkpoint saved atomically right after a successful append, truncate
  back to the last good offset on resume.
- **Timestamps written both ways**: epoch (`last_seen_epoch`,
  `created_at_epoch`) and IST-formatted strings, epoch never dropped.
- **Rate limit** 30 requests/minute by default, sliding 60s window.

### Configuration

Credentials loading and the rate limiter come from the shared
`../../common.py`.

- `../../credentials.yaml` (shared): `edmingle.api_key` -> `apikey`,
  `edmingle.organization_id` -> `orgid`.
- `ip_driven_country_data_config.json` (in `scripts/`, renamed from
  `edmingle_user_country_list_config.json`): `base_url`, `filter_key`,
  `sort_order`, `per_page`, `start_date`, `end_date`,
  `rate_limit_per_minute`, `output_csv`, `checkpoint_file`.
- No `notifications.yaml` -- no email/alerting capability. Failures log
  and exit non-zero.

### How to run

```bash
cd scripts
python3 ip_driven_country_data.py --config ip_driven_country_data_config.json
```
`--config` already defaults to `ip_driven_country_data_config.json`, so it
also runs with no arguments from inside `scripts/`. Safe to Ctrl+C or let a
429 penalty run out -- re-run the same command, it resumes from checkpoint.

### Output

`output/user_country_list.csv` -- columns: `user_id, name, email,
contact_number, country, region, time_spent_seconds, total_sessions,
last_seen_epoch, last_seen_ist, created_at_epoch, created_at_ist,
source_page`. `output/user_country_list_checkpoint.json` -- resume state.

**As of this writing this script has never completed a real run** -- only
the CSV header exists in `output/`, no data rows. Run it for real before
Stage 3's merge will have any `ip_driven_country` matches.

## Stage 2: `dial_code_to_country.py`

Reads a manually-exported Edmingle student roster CSV and adds a country
guess derived from the `Contact Number Dial Code` column (e.g. `+91` ->
`India`, via the `phonenumbers`/`pycountry` packages). Does **not** touch
any pre-existing `Country Name` column in the export (that column is
essentially unused in practice -- blank in 129,884 of 130,188 rows in the
current `Student-Export.csv`).

- Handles a possible leading junk title line above the real header
  (some Edmingle exports have one; the current `Student-Export.csv` does
  not, but the script tolerates it either way).
- Dial codes stored as `-`/blank -> blank result, not an error.
- Where one dial code maps to multiple countries (e.g. `+1` -> US/Canada/etc,
  `+44` -> UK, `+7` -> Russia/Kazakhstan), picks the primary/most common
  country for that code (`phonenumbers`' own convention).

### How to run

```bash
cd scripts
python3 dial_code_to_country.py
```
With no `--input`, auto-picks the most recently modified file matching
`Student-Export*.csv` in `../input/`. Drop a fresh export there and just
re-run. `--input`/`--output`/`--dial-code-column` can override the
defaults; both input and output default paths are resolved relative to
this script's own location (`../input/`, `../output/`), not the caller's
working directory.

### Output

`output/Student-Export_with_country.csv` -- every column from the input
export, plus a new `Derived Country (Dial Code)` column (this becomes
`dial_country` after Stage 3's merge renames it). On the current
`Student-Export.csv` (130,188 rows): 90,939 rows got a derived country,
39,249 were left blank (missing/`-`/unrecognized dial code).

## Stage 3: `merge_country_data.py`

Joins Stage 1 and Stage 2's output and produces the final three-column
country picture.

### Merge logic

- **Join key: email only**, normalized (lowercase + trimmed). Chosen over
  phone number because in `Student-Export.csv` only ~0.02% of rows have a
  blank email vs. ~29% with a blank/dash Contact Number.
- **Left join, Student-Export as the base**: every row from
  `Student-Export_with_country.csv` is kept in the output. Wherever a
  matching email is found in `user_country_list.csv`, `ip_driven_country`
  is filled in from its `country` column; otherwise it's left blank.
- **`final_country`: ip_driven_country wins whenever it's present** (a
  direct, current geo-IP signal beats a dial-code guess). `dial_country`
  is used only as a fallback when there's no ip-driven match for that
  student at all. If neither source has a value, `final_country` is blank.
- Duplicate emails in `user_country_list.csv` are logged as a warning and
  the first row seen for that email is kept -- this shouldn't happen in
  practice (one row per Edmingle user), but the script doesn't fail on it.

### How to run

Must run after both Stage 1 and Stage 2 have produced their output files
(this script does not run either of them itself):
```bash
cd scripts
python3 merge_country_data.py
```
`--dial-input`, `--ip-input`, `--output` can override the default paths
(`../output/Student-Export_with_country.csv`, `../output/user_country_list.csv`,
`../output/merged_country_data.csv`).

### Output

`output/merged_country_data.csv` -- every column from
`Student-Export_with_country.csv` (with `Derived Country (Dial Code)`
renamed to `dial_country`, not duplicated), plus `ip_driven_country` and
`final_country` appended at the end. Prints a summary on every run: total
rows, how many resolved via `ip_driven_country`, how many fell back to
`dial_country`, and how many had no country from either source.

**Known current-state caveat**: since Stage 1 has never completed a real
run (see above), running Stage 3 today produces `ip_driven_country` blank
for all 130,188 rows and `final_country` == `dial_country` everywhere.
Run Stage 1 for real, then re-run Stage 3, to get a merge that actually
exercises the override.

## Known limitations / things to watch for

- **Not part of the original documented scope.** Flag to whoever owns
  project requirements if this 3-stage pipeline is meant to be permanent.
- **`input/` is gitignored and never committed** -- `Student-Export*.csv`
  contains real student PII. If you need to hand this data to someone
  else, do it through a channel appropriate for PII, not via this repo.
- **Referenced example config file doesn't exist**:
  `ip_driven_country_data.py`'s docstring mentions copying
  `ip_driven_country_data_config.example.json` -- no such file currently
  exists in `scripts/`; the real config file is already there and in use.
- **Stage 1's "remaining rows" depends on the API** returning `total_rows`
  in `page_context`; logged as unknown otherwise.
- **Stage 1 has no email/alerting on failure** -- logs to stdout/stderr
  and exits non-zero only.
- **Country name formats aren't normalized between the two sources.**
  `dial_country` comes from `pycountry`'s official country names;
  `ip_driven_country` comes verbatim from whatever Edmingle's own geo-IP
  lookup returns as `filterValue`. These could disagree on formatting for
  the same country (e.g. "United States" vs. "United States of America")
  even when both are "correct" -- not reconciled by this pipeline.
