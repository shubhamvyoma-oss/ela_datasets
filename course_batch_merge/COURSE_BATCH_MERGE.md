# Course Batch Merge Pipeline

## 1. Overview & Purpose

`Course_Batch_Merge.py` (357 lines, `course_batch_merge/scripts/`) builds one master
course/batch report by merging the Edmingle course catalogue with masterbatch data across **all
three** batch statuses — Active, Archived, Completed. A single-file, single-run script (no CLI
args, no config file): fetch, merge, apply business rules, write one CSV.

**Purpose:** a Power-BI-ready, 41-column course/batch report including every batch regardless of
status (unlike the separate `session_wise_attendance` catalogue builder, which uses different
inclusion rules by design — the two are not expected to reconcile row-for-row).

## 2. High-Level Data Flow

```mermaid
flowchart TD
    A[main] --> B[get_catalogue: GET institute/483/courses/catalogue]
    A --> C[get_all_batches: loop status 0/1/3]
    C --> C1[get_batches_by_status status=0 Active]
    C --> C2[get_batches_by_status status=1 Archived]
    C --> C3[get_batches_by_status status=3 Completed]
    C1 --> D[filter_test_batches: drop batch_name containing 'test batch']
    C2 --> D
    C3 --> D
    B --> E[merge: batch_df LEFT JOIN cat_df on bundle_id == Bundle id, indicator=True]
    D --> E
    E --> F[Catalogue_Match = merge indicator == both]
    F --> G[compute_bundle_enrollment: groupby bundle_id, sum batch_enrollment_count]
    G --> H[mark_latest_batch: sort by bundle_id + start_date desc, flag first per bundle]
    H --> I[apply_business_logic: Has_Batch=1, Catalogue_Status, Final_Status, Include_In_Course_Count]
    I --> J[add_courses_without_batches: synth rows for catalogue bundles with zero batches]
    J --> K[format start_date/end_date: epoch -> date]
    K --> L[select existing_cols from fixed 41-col OUTPUT_COLUMNS, fillna]
    L --> M[(course_batch_merge.csv)]
```

**Lineage:** catalogue API + masterbatch API (3 calls, one per status) → left join on
`bundle_id` → business-rule functions → `course_batch_merge.csv`. Believed to feed a downstream
Power BI report (see Section 8) — not automated or confirmed from this repo.

## 3. Repository Structure

| Path | Purpose |
|---|---|
| `scripts/Course_Batch_Merge.py` | Entire pipeline — fetch, merge, business rules, save, all in one file. |
| `output/course_batch_merge.csv` | The only file this pipeline produces. |
| `../../credentials.yaml` | Shared `API_KEY`/`ORGANIZATION_ID`/`INSTITUTE_ID` via `common.load_credentials()`. |
| `../../common.py` | Supplies `load_credentials()` only — no rate limiter or atomic-write helpers used here. |

## 4. Source System

| Source | Endpoint | Method | Auth | Parameters | Pagination | Rate Limit |
|---|---|---|---|---|---|---|
| Course catalogue | `.../institute/483/courses/catalogue` (institute id hardcoded) | GET | `apikey`/`ORGID` headers | None beyond headers | Single call returns the full catalogue | Not identified — no retry/backoff of any kind |
| Masterbatch | `.../short/masterbatch?status={0/1/3}&page=1&per_page=1000` | GET | Same headers | `status`, `page=1` (hardcoded, never incremented), `per_page=1000` | **`page=1` never advances** — a status with over 1,000 batches would silently lose the rest | Not identified |

## 5. Extraction Process

`get_catalogue()` — one GET, empty DataFrame on non-200 (no raise). `get_all_batches()` loops
statuses `{0: Active, 1: Archived, 3: Completed}` calling `get_batches_by_status()` for each,
concatenating results. Each call flattens `courses[].batch[]` into rows (`bundle_id`, `batch_id`,
`batch_name`, dates, `tutor_id` via a fallback chain flagged in-code as unconfirmed,
`batch_enrollment_count`). `filter_test_batches()` drops rows with `"test batch"` in the name.
The batches are left-merged onto the catalogue on `bundle_id`, `Catalogue_Match` set from the
merge indicator. `bundle_enrollment_count` sums `batch_enrollment_count` per bundle across all
statuses. `mark_latest_batch()` flags the newest-`start_date` row per bundle (stable-sort
tie-break by fetch order). `apply_business_logic()` sets `Has_Batch=1`, copies catalogue `Status`
into `Catalogue_Status`, and sets `Final_Status` = catalogue status for the latest batch only,
`"Completed"` for every other batch of that bundle. `add_courses_without_batches()` appends one
synthetic zero-batch row per catalogue course with no real batches. Dates convert epoch→date, the
fixed 41-column schema is selected (missing columns logged, not silently dropped), blanks filled,
written to CSV.

## 6. Function Reference

### `get_catalogue() -> pd.DataFrame`
Single GET to the catalogue endpoint; empty DataFrame + printed error on any non-200.

### `get_batches_by_status(status_code, status_label) -> list[dict]`
One GET (`page=1&per_page=1000`), flattens nested `courses[].batch[]` via manual index-based
loops; `tutor_id` uses a fallback chain (`tutor_id`→`faculty_id`→`tutorId`) explicitly flagged
in-code as unconfirmed against Edmingle's real field name.

### `filter_test_batches(df) -> pd.DataFrame`
Drops rows whose lower-cased `batch_name` contains `"test batch"` — the **only** test-data filter
in this script (no demo/dummy/sample keyword filtering).

### `mark_latest_batch(df) -> pd.DataFrame`
Parses `start_date` as numeric (blank/unparseable → 0), sorts by `(bundle_id, date desc)`, flags
the first row per bundle. Exact-date ties break by original fetch order (Active → Archived →
Completed), since the sort is stable.

### `apply_business_logic(df) -> pd.DataFrame`
`Has_Batch=1` for all rows; `Final_Status` = catalogue status only for the latest batch per
bundle, else hardcoded `"Completed"` regardless of that batch's real status.
`Include_In_Course_Count` is computed but excluded from `OUTPUT_COLUMNS` — never reaches the CSV.

### `add_courses_without_batches(merged_df, catalogue_df) -> pd.DataFrame`
Appends one synthetic row per catalogue course with zero real batches: `Has_Batch=0`,
`Is_Latest_Batch=1`, `Include_In_Course_Count=0`, `Catalogue_Match=True` (hardcoded, not derived).

### `main()`
Orchestrates the full run inside one broad `try/except Exception` that **prints** the error and
returns — does not re-raise or `sys.exit(1)`, so the process exits 0 even after a mid-run failure
(see Known Limitations).

## 7. Configuration & Parameters

| Source | Key(s) | Purpose |
|---|---|---|
| `../../credentials.yaml` | `edmingle.api_key`, `edmingle.organization_id`, `edmingle.institute_id` (loaded but unused — see Section 14) | Auth for both endpoints. |
| Hardcoded | `CATALOGUE_URL` (institute id `483` baked in), `BATCHES_URL`, `OUTPUT_COLUMNS` (41 columns), status map `{0,1,3}`, `page=1&per_page=1000` | All runtime behaviour — no config file, no CLI args. |

## 8. Data Transformation, Output & Schema

**Transformations:** test-batch filtering (substring match) · catalogue-match flag from merge
indicator · bundle enrollment rollup (sum across all 3 statuses) · latest-batch flagging (newest
`start_date`, stable-sort tie-break) · status derivation (catalogue status for latest batch,
fixed `"Completed"` otherwise) · synthetic zero-batch rows for batch-less courses · epoch→date
conversion · fixed 41-column schema enforcement.

**Output:** `course_batch_merge.csv` (UTF-8-with-BOM), one row per batch plus synthetic rows,
written via a direct (non-atomic) `to_csv()` — a crash mid-write could leave a partial file.

**Confirmed state (2026-09-24):** 3,193 data rows, 482,628 bytes, last modified 2026-09-16.

**Database integration:** not applicable — CSV output only.

**Schema** (41 columns, verified against the live file header):

| Column | Description | Source/Derived |
|---|---|---|
| `bundle_id`, `bundle_name`, `batch_id`, `batch_name` | Batch identity | Source |
| `batch_status` | Which status pull this row came from | Derived (fetch-time label) |
| `start_date` / `end_date` | Batch dates | Source (epoch→date) |
| `tutor_name`, `tutor_id` | Tutor identity — `tutor_id` mapping unconfirmed | Source |
| `batch_enrollment_count` | `admitted_students` for this batch | Source |
| `Course Name`, `Tutors`, `Tutord Ids`, `Course Ids`, `Subject`, `Level`, `Language`, `Examination`, `Type`, `Course Division`, `Certificate`, `Course Sponsor`, `Course Title Sanskrit`, `Status`, `Number of Lectures`, `Duration`, `Personas`, `Computer Based Assessment`, `Product ID`, `SSS Category`, `Viniyoga`, `Adhyayanam Category`, `Term of Course`, `Position in Funnel`, `Division` | Catalogue fields, passed through verbatim (`Tutord Ids` is Edmingle's own spelling, not a typo introduced here) | Source (catalogue) |
| `Catalogue_Match` | Whether this batch matched a catalogue row | Derived |
| `bundle_enrollment_count` | Sum of enrollment across all batches of the bundle | Derived |
| `Is_Latest_Batch` | 1 for the newest batch per bundle | Derived |
| `Has_Batch` | 0 only for synthetic no-batch rows | Derived |
| `Catalogue_Status` | Copy of the catalogue's `Status` | Derived |
| `Final_Status` | Catalogue status if latest batch, else `"Completed"` | Derived |

## 9. Data Quality & Known Limitations

**Implemented checks:** non-200 catalogue response → empty DataFrame + log (other failures fall
through to the broad `except`); test-batch substring filter; missing output columns logged, not
silently dropped; strict 41-column schema enforcement.

**Confirmed limitations:**
- **No pagination on the masterbatch call** — hardcoded `page=1&per_page=1000`; a status over 1,000 batches would silently lose the excess. Not currently observable as a problem (3,193 total rows across all statuses) but not proven safe either.
- No retry/backoff on either endpoint — one transient blip fails the whole run.
- Only `"test batch"` is filtered — no demo/dummy/sample/cbt_test/payment_test keyword filtering.
- `tutor_id`'s real source field is explicitly unconfirmed in the code's own comment.
- **A mid-run failure is swallowed** — `main()`'s broad `except` prints and returns, exiting 0. A cron/scheduler watching only the exit code would never see this as a failure.
- `INSTITUTE_ID` is loaded from credentials but never used — the id `483` is hardcoded into the URL instead.

**Requires confirmation:** whether any status currently has, or will have, more than 1,000 batches; the correct Edmingle field name for tutor id.

## 10. Error Handling & Logging

`log_progress()` prints `HH:MM:SS - message` to stdout — no file logging, no rotation.
`get_catalogue()` checks HTTP status explicitly; `get_batches_by_status()` has **no** status check
at all (calls `.json()` unconditionally). The entire `main()` body is one broad
`try/except Exception`, so any failure anywhere is caught, printed, and the process exits 0
without writing a CSV. Example real log line: `11:49:39 - SUCCESS! Saved 3193 rows to file.`

## 11. Dependencies

| Dependency | Purpose |
|---|---|
| `requests` | HTTP calls |
| `pandas` | DataFrame construction, merge, groupby, dates |
| `common` (repo root) | `load_credentials()` |
| stdlib (`os`, `sys`, `datetime`, `pathlib`) | Paths, timestamps |

## 12. Setup & How to Run

**Step by step:**
1. `source /home/projectdev/ela_datasets/.venv/bin/activate` — one time per shell session. Your
   prompt shows `(.venv)` when it's active; a plain `python3` after this already has `requests`,
   `pandas` installed, so no `pip install` step is needed.
2. Populate `../../credentials.yaml` (`institute_id` is loaded but unused) — shared by every
   pipeline, likely already done. No config file, `input/` folder, or notification setup needed.
3. `cd /home/projectdev/ela_datasets/course_batch_merge/scripts`.
4. Run the script below. **The filename is capitalized exactly like this** —
   `Course_Batch_Merge.py`, not `course_batch_merge.py` — Linux is case-sensitive and will error
   with "No such file or directory" on the lowercase form.

```bash
source /home/projectdev/ela_datasets/.venv/bin/activate
cd /home/projectdev/ela_datasets/course_batch_merge/scripts
python3 Course_Batch_Merge.py
```
No CLI arguments exist.

## 13. Automation / Scheduling

None — triggered manually, no cron/systemd/Task Scheduler entry, and no restart wrapper.

## 14. Important Business / Technical Rules

- Archived batches are deliberately included, unlike `session_wise_attendance`'s catalogue builder — the two are not expected to reconcile row-for-row, by design.
- `INSTITUTE_ID` from credentials is never actually used — `483` is hardcoded in the URL; changing the credentials value would not follow through without a code edit.
- `Final_Status` is asymmetric by design: the latest batch of a bundle gets the course's catalogue status; every other batch of that bundle is hardcoded `"Completed"` regardless of its real status.
- `bundle_enrollment_count` sums across all three statuses, not just active enrollment.
- Latest-batch tie-breaking on an exact `start_date` match depends on fetch order, not a deterministic rule.

## 15. Troubleshooting

| Symptom | Likely cause | Check |
|---|---|---|
| "Error fetching catalogue!", no CSV produced | Catalogue endpoint returned non-200 | Printed status code / credentials |
| "ERROR occurred during execution!", exits 0, no CSV | Unhandled exception in `main()` (e.g. bad masterbatch JSON) | The printed `Error message:` line — the broad `except` swallows the traceback |
| Output row count looks capped near 1,000 for one status | Hardcoded single-page fetch | Confirm actual batch counts against Edmingle directly |
| A known batch is missing from output | `"test batch"` in its name, or status not 0/1/3 | Check `filter_test_batches()` and the status map |
| `tutor_id` blank for many rows | Fallback field chain doesn't match the real response field | Inspect a raw masterbatch response |

## 16. Maintenance Guide

- **Endpoint/params change** → `CATALOGUE_URL`/`BATCHES_URL`/`HEADERS`.
- **Add pagination** → `get_batches_by_status()` needs a loop incrementing `page` (confirm Edmingle's "more pages" signal first).
- **Output schema change** → `OUTPUT_COLUMNS` — missing columns are already handled gracefully.
- **New test-data filter** → extend `filter_test_batches()` or add a new filter function.
- **Fix the silent-failure exit code** → have `main()`'s `except` call `sys.exit(1)` after printing.

## 17. Security Considerations

`credentials.yaml` holds the shared API key and is gitignored. No PII beyond tutor names — no
individual student records. `log_progress()` never prints credential values.

## 18. Future Improvements

1. **Email the output on completion** — send a completion email that includes the run status *and* attaches the generated dataset file(s), not just a status notification.
2. **Scheduled automation** — run automatically on a defined schedule instead of a manual trigger.
3. **Data cleaning layer** — a dedicated cleaning step/script (nulls, duplicates, standardization) inside the pipeline, instead of leaving it to downstream consumers.

---
*Initial documentation: 2026-09-24. Downstream: per prior documentation, this schema matches a
"verified `vyoma_master.csv` reference," implying a Power BI consumer — not confirmed from this
repo. Project/technical owner: requires confirmation.*
