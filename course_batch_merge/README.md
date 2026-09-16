# Course Batch Merge

## What this pipeline does
Builds one master course/batch report by merging the Edmingle course catalogue
with masterbatch data pulled across **all three** batch statuses -- Active,
Archived, and Completed. Unlike the similar catalogue-builder used by the
`session_wise_attendance` pipeline (a different folder, not touched here),
this script deliberately includes Archived batches, so its output is not
expected to match that pipeline row-for-row -- see "Known limitations".

## Folder layout

This folder is split into two subfolders:

- `scripts/` -- all source code: `Course_Batch_Merge.py`. Run everything
  from inside `scripts/`.
- `output/` -- generated data: `course_batch_merge.csv`.

Compiled bytecode (`__pycache__`) for every pipeline under `ela_datasets/`
is redirected to a single shared `ela_datasets/.pycache/` directory (via
`sys.pycache_prefix`, set at the top of `Course_Batch_Merge.py` before any
third-party import) instead of a separate `__pycache__` folder per
pipeline.

## Edmingle endpoints used
- `GET https://vyoma-api.edmingle.com/nuSource/api/v1/institute/483/courses/catalogue`
  (institute id `483` is hardcoded directly in the URL string, not built from
  the `INSTITUTE_ID` value loaded from credentials -- see limitations)
- `GET https://vyoma-api.edmingle.com/nuSource/api/v1/short/masterbatch?status={status}&page=1&per_page=1000&organization_id={ORGANIZATION_ID}`
  -- called once per status code in `{0: "Active", 1: "Archived", 3: "Completed"}`

## Business rules that determine correct data

- **`filter_test_batches`** -- drops any batch row whose `batch_name`
  (lower-cased) contains the substring `"test batch"`. This is the only
  test-data filter in this script; there is no separate keyword-based
  course/bundle filter (no demo/dummy/sample/cbt_test/payment_test/smoke
  check anywhere in this file).

- **`mark_latest_batch`** -- for each `bundle_id`, sorts its batches by
  `start_date` (parsed as a numeric epoch timestamp, blank/unparseable dates
  treated as `0`) newest-first, then flags the first row per bundle as
  `Is_Latest_Batch = 1`. Because the sort is stable, if two batches for the
  same bundle share the exact same `start_date` the tie is broken by
  whichever one appeared first in the original fetch order (Active batches
  fetched before Archived, before Completed, and within a status in whatever
  order Edmingle's API returned the courses/batches).

- **`apply_business_logic`** -- sets `Has_Batch = 1` on every batch row and
  copies the catalogue's own `Status` column into `Catalogue_Status`.
  `Final_Status` defaults to `"Completed"` for every row, then for rows where
  `Is_Latest_Batch == 1` it is overwritten with that bundle's catalogue
  `Status` value verbatim (trimmed). In other words, **`Final_Status` for the
  latest batch of a bundle reflects the course's catalogue status, not the
  batch's own `batch_status`** (Active/Archived/Completed label); non-latest
  batches simply keep the `"Completed"` default regardless of their real
  catalogue status. An internal `Include_In_Course_Count` flag is set to `1`
  when that catalogue status is one of `Completed`, `Ongoing`, or `Upcoming`
  -- this flag (and `Status_Adjustment_Reason`, which is never populated
  beyond an empty string) are computed but are **not** part of
  `OUTPUT_COLUMNS`, so neither appears in the final CSV.

- **`Catalogue_Match`** (computed inline in `main`, not a separate function)
  -- the batch data is left-merged onto the catalogue on
  `bundle_id == "Bundle id"` with `indicator=True`; `Catalogue_Match` is
  `True` when the merge indicator is `"both"` (the batch's bundle exists in
  the catalogue) and `False` when a batch's `bundle_id` has no catalogue
  match at all.

- **`compute_bundle_enrollment`** (also inline in `main`, via `groupby`) --
  `bundle_enrollment_count` is the sum of `batch_enrollment_count`
  (Edmingle's `admitted_students`) across every remaining batch row (all
  three statuses, after test-batch filtering) sharing the same `bundle_id`.
  This single total is broadcast onto every batch row of that bundle.

- **`add_courses_without_batches`** -- after the merge, any catalogue
  `Bundle id` that has **zero** batch rows at all (across all three
  statuses) gets one synthetic row added: `Has_Batch = 0`,
  `Is_Latest_Batch = 1`, `Include_In_Course_Count = 0`,
  `Final_Status` = the catalogue's own `Status` field, and
  `Catalogue_Match` hardcoded to `True`. All batch-only fields (`batch_id`,
  `batch_name`, `start_date`, etc.) are left blank on these rows.

- Dates: `start_date`/`end_date` are parsed as Unix epoch seconds and
  converted to plain dates before saving.

- Output columns are the fixed 41-column `OUTPUT_COLUMNS` list matching the
  verified `vyoma_master.csv` reference schema. Any expected column missing
  from the fetched/derived data is logged as a `WARNING` (naming the missing
  columns) rather than silently dropped; whatever remains is filled with
  empty strings for blanks (`fillna("")`) before saving.

## Configuration
- `../../credentials.yaml` (shared, relative to `scripts/`): `API_KEY`,
  `ORGANIZATION_ID`, `INSTITUTE_ID` -- no local config file for this script,
  and it does not send any email/notification on success or failure.
- Note: `INSTITUTE_ID` is loaded from credentials but is not actually wired
  into the catalogue request -- the institute id (`483`) is hardcoded
  directly in `CATALOGUE_URL` instead.

## How to run
```
cd /home/projectdev/ela_datasets/course_batch_merge/scripts
python3 Course_Batch_Merge.py
```

## Output
`course_batch_merge.csv`, written into the `output/` folder at this
pipeline's root (i.e. `../output/course_batch_merge.csv` relative to the
script in `scripts/`), via
`OUTPUT_PATH = os.path.join(SCRIPT_DIR, "..", "output", "course_batch_merge.csv")`
where `SCRIPT_DIR` is anchored to the script's own file location -- so it
does not depend on the working directory the script is launched from.

## Known limitations
- No retry/backoff logic anywhere. `get_catalogue`/`get_batches_by_status`
  do not raise on a bad HTTP status in every case (the catalogue call checks
  `status_code` and returns an empty DataFrame on failure), but a failure
  partway through (bad JSON, missing keys, a masterbatch call that returns a
  non-200 body) will raise inside those helpers. The entire `main()` body is
  wrapped in one broad `try/except Exception`, so such a failure is caught,
  printed to stdout, and the process **exits normally (code 0)** without
  writing a CSV -- a scheduler/cron wrapper watching the exit code alone
  would not detect this as a failure.
- This pipeline and the catalogue builder used by the `session_wise_attendance`
  pipeline (a separate folder) intentionally use different inclusion rules
  (this one includes Archived batches, that one may not) -- they are **not**
  expected to reconcile with each other, by design.
- `INSTITUTE_ID` from `credentials.yaml` is loaded but unused; the institute
  id is hardcoded to `483` in the catalogue URL.
