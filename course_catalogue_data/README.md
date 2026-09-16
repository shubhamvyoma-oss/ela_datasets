# Course Catalogue Data (simple flatten)

## What this pipeline does
The simplest of the catalogue-domain scripts. It makes a single API call to
fetch the Edmingle course catalogue, flattens whatever JSON comes back with a
generic `pandas.json_normalize`, cleans up the column names, and writes the
result straight to CSV. It applies no filtering and no business rules --
every course/bundle Edmingle returns ends up in the output.

## Folder layout

This folder is split into two subfolders:

- `scripts/` -- all source code: `course_catalogue_data.py`. Run everything
  from inside `scripts/`.
- `output/` -- generated data: `course_catalogue_data.xlsx` (a leftover from
  an earlier version of the script; the code today only ever writes
  `course_catalogue_data.csv` here, via `OUTPUT_FILE`).

Compiled bytecode (`__pycache__`) for every pipeline under `ela_datasets/`
is redirected to a single shared `ela_datasets/.pycache/` directory (via
`sys.pycache_prefix`, set at the top of `course_catalogue_data.py` before
any third-party import) instead of a separate `__pycache__` folder per
pipeline.

## Edmingle endpoint used
```
GET https://vyoma-api.edmingle.com/nuSource/api/v1/institute/483/courses/catalogue?org_id={ORGANIZATION_ID}
```
Headers sent: `apikey` (from credentials) and `ORGID`. Note: the `ORGID`
header is currently a hardcoded literal `"683"` in this file, **not** the
`ORGANIZATION_ID` value loaded from `credentials.yaml` (that loaded value is
only used in the `org_id` query parameter, not the header) -- if the
organization id in `credentials.yaml` ever differs from `683`, the header and
query param would disagree. Documented here as-is per this task's scope
(output-path fix and README only); not changed.

## Business rules
There really aren't any:
- `fetch_courses` checks for HTTP status `200`; on any other status it prints
  the error body and returns an empty list (no retry).
- The response's `"response"` key is taken as the raw list of course records,
  with no field-level filtering of any kind (no test/demo keyword exclusion,
  no status filtering).
- `pd.json_normalize(courses_list)` is the entire flattening step -- it
  auto-expands nested dictionaries into dot-notation columns (e.g.
  `some_field.sub_field`), but does **not** expand nested lists into
  separate rows/columns; any list-valued field in a record stays as a raw
  Python list object in that cell (rendered as its string form in the CSV).
  There is no custom recursive list-flattening logic in this file.
- `clean_column_names` lower-cases every column name and replaces spaces
  with underscores -- that is the only transformation applied.
- An `ingested_at` timestamp column (script run time) is appended to every
  row before saving.
- In short: whatever Edmingle returns becomes the columns, verbatim.

## Configuration
- `../../credentials.yaml` (shared, relative to `scripts/`): `API_KEY`,
  `ORGANIZATION_ID`, `INSTITUTE_ID` -- no local config file, no
  notifications.
- Note: `INSTITUTE_ID` is loaded from credentials but never referenced again
  in this script; the institute id (`483`) is hardcoded directly into the
  endpoint URL instead.

## How to run
```
cd /home/projectdev/ela_datasets/course_catalogue_data/scripts
python3 course_catalogue_data.py
```

## Output
`course_catalogue_data.csv`, written into the `output/` folder at this
pipeline's root (i.e. `../output/course_catalogue_data.csv` relative to the
script in `scripts/`) via `df.to_csv(OUTPUT_FILE, index=False)`, where
`OUTPUT_FILE = os.path.join(SCRIPT_DIR, "..", "output", "course_catalogue_data.csv")`
and `SCRIPT_DIR` is anchored to the script's own file location (not the
working directory). The `course_catalogue_data.xlsx` file now sitting in
`output/` is a leftover from an earlier version of the script -- the code as
it stands today only ever writes CSV, never Excel.

## Known limitations
- No retry/backoff logic, and no `try/except` at all around the network
  call or JSON parsing -- `fetch_courses` only guards against a non-200 HTTP
  status (prints the error and returns `[]`, so the script just logs
  "No data returned from API" and exits). Any other failure -- a connection
  error, timeout, or malformed JSON body -- raises an unhandled exception
  and stops the script.
- Output shape is entirely whatever Edmingle's catalogue endpoint happens to
  return that day; there is no schema validation, so an upstream field
  rename/addition/removal on Edmingle's side silently changes the output
  columns.
- The `ORGID` header mismatch noted above (hardcoded `"683"` vs. the
  credentials-driven `ORGANIZATION_ID`) is a latent inconsistency in the
  current file.
