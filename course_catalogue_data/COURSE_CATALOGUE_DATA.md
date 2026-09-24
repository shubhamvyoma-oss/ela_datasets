# Course Catalogue Data Pipeline

## 1. Overview & Purpose

`course_catalogue_data.py` (116 lines, `course_catalogue_data/scripts/`) is the simplest
catalogue-domain script in `ela_datasets`: one API call, a generic `pandas.json_normalize`
flatten, column-name cleanup, an `ingested_at` timestamp, straight to CSV. No filtering, no
business rules — every course/bundle Edmingle returns ends up in the output.

**Purpose:** a raw, unfiltered, always-current catalogue snapshot with no assumptions about which
courses matter — deliberately the "rawest" catalogue pipeline, unlike `course_batch_merge`
(business-rule-heavy) or `session_wise_attendance`'s catalogue builder.

## 2. High-Level Data Flow

```mermaid
flowchart TD
    A[main] --> B[fetch_courses: GET institute/483/courses/catalogue?org_id=ORGANIZATION_ID]
    B -->|status != 200| C[print error, return empty list -> 'No data returned from API', exit]
    B -->|status == 200| D[courses_list = response.json data]
    D --> E[pd.json_normalize: flatten nested dicts to dot-notation columns]
    E --> F[clean_column_names: lower-case, spaces -> underscores]
    F --> G[df ingested_at = datetime.now]
    G --> H[df.to_csv OUTPUT_FILE, index=False]
```

**Lineage:** catalogue API → `json_normalize` flatten → column cleanup → `+ingested_at` →
`course_catalogue_data.csv`. Downstream consumer not identified from this repo.

## 3. Repository Structure

| Path | Purpose |
|---|---|
| `scripts/course_catalogue_data.py` | Entire pipeline — fetch, flatten, clean, timestamp, save. |
| `output/course_catalogue_data.csv` | The only file this pipeline produces. |
| `../../credentials.yaml` | Shared `API_KEY`/`ORGANIZATION_ID`/`INSTITUTE_ID` (`INSTITUTE_ID` loaded but unused). |
| `../../common.py` | Supplies `load_credentials()` only. |

## 4. Source System

| Source | Endpoint | Method | Auth | Parameters | Pagination | Rate Limit |
|---|---|---|---|---|---|---|
| Course catalogue | `.../institute/483/courses/catalogue` (institute id hardcoded) | GET | `apikey` header; `ORGID` header **hardcoded to `"683"`, not the credentials-driven value** | `org_id` query param — the *only* place the real `ORGANIZATION_ID` is actually used | Single call returns the full catalogue | Not identified — no retry/backoff of any kind |

## 5. Extraction Process

`fetch_courses()` — one GET with `apikey` + hardcoded `ORGID: "683"` headers and
`params={"org_id": ORGANIZATION_ID}`. Non-200 → prints the body, returns `[]`, and `main()` exits
with "No data returned from API," writing nothing. On success, `response.json()["response"]`
becomes the raw record list. `pd.json_normalize()` flattens nested dicts into dot-notation
columns (list-valued fields are left as raw Python list objects, not expanded).
`clean_column_names()` lower-cases names and replaces spaces with underscores — the only
transformation applied. An `ingested_at` timestamp column is appended, and the result is written
via `df.to_csv()` (default UTF-8, no BOM).

## 6. Function Reference

### `fetch_courses() -> list`
One GET; prints status and (on failure) the error body, returning `[]`. **No `try/except`** —
a connection error, timeout, or non-JSON body raises unhandled and stops the script.

### `clean_column_names(dataframe) -> pd.DataFrame`
Lower-cases and underscore-izes every column name via a manual index-based loop; no other
renaming, reordering, or dropping.

### `main()`
Fetches → builds the DataFrame with **no column filtering** (explicit code comment confirms this
is intentional — every raw column is kept) → cleans names → appends `ingested_at` → saves →
prints a summary (record/column counts, column list, file path). Guarded by
`if __name__ == "__main__":` so importing the module never triggers a live call.

## 7. Configuration & Parameters

| Source | Key(s) | Purpose |
|---|---|---|
| `../../credentials.yaml` | `edmingle.api_key`, `edmingle.organization_id`, `edmingle.institute_id` (loaded, never referenced again) | Auth for the catalogue call. |
| Hardcoded | `BASE_URL` (institute id `483` baked in), `HEADERS["ORGID"] = "683"` (ignores the credentials-driven org id), `OUTPUT_FILE` | All runtime behaviour — no config file, no CLI args. |

## 8. Data Transformation, Output & Schema

**Transformations:** JSON flattening via `json_normalize` (the entire flattening step, no custom
recursion) · list-valued fields left as raw Python list string reprs · column-name cleanup
(lower_snake_case) · `ingested_at` timestamp appended · **no field-level filtering** — every
column Edmingle returns ends up in the output.

**Output:** `course_catalogue_data.csv`, direct (non-atomic) `to_csv()`.

**Confirmed state (2026-09-24):** 1,846 data rows, 168,438 bytes, modified 2026-09-24 00:21 UTC —
a genuinely current file, not a stale artifact. `output/` contains only this CSV, no `.xlsx`.

**Database integration:** not applicable — CSV output only.

**Schema** (22 columns, verified against the live file header): `bundle_id`, `course_name`,
`num_students`, `tutors`, `tutord_ids` (Edmingle's own "Tutord Ids" spelling, lower-cased),
`course_ids`, `subject`, `level`, `language`, `texts`, `type`, `course_division`, `certificate`,
`course_sponsor`, `status`, `number_of_lectures`, `duration`, `personas`, `sss_category`,
`adhyayanam_category`, `term_of_course`, `position_in_funnel` — all Source (verbatim from
Edmingle, only the column name is cleaned).

**Confirmed discrepancy:** the current code unconditionally adds an `ingested_at` column, which
would make 23 columns — but the live file has exactly 22, with **no** `ingested_at` column at
all. `git log` shows only 2 commits and no uncommitted changes, so this isn't an uncommitted local
edit either. **Requires confirmation** how this specific file was actually produced (an older
script revision, most likely, but not confirmed).

## 9. Data Quality & Known Limitations

**Implemented checks:** HTTP status check (non-200 → error + empty list, no retry). No JSON
validation, no field filtering, no schema validation — whatever Edmingle returns becomes the
output schema, unchecked.

**Confirmed limitations:**
- No retry/backoff and no `try/except` at all around the network call or JSON parsing — any failure beyond a non-200 status stops the script with an unhandled exception.
- Output shape is entirely dictated by Edmingle's response that day — an upstream field change silently changes the output columns, undetected.
- The `ORGID` header (hardcoded `"683"`) and the `org_id` query parameter (from credentials) could disagree if the real org id ever changes — unverified which one Edmingle actually honors.
- No test/demo course filtering of any kind.
- List-valued fields aren't flattened — unusable directly from the CSV without further parsing.
- The live output file lacks the `ingested_at` column the current code always adds (see Section 8) — a confirmed code/output mismatch.

**Requires confirmation:** why the output lacks `ingested_at`; whether `ORGID` should use the real organization id instead of `"683"`; whether `INSTITUTE_ID` should replace the hardcoded `483`.

## 10. Error Handling & Logging

No `logging` module — all output via `print()` (status code, response keys, error text, summary).
Only a non-200 HTTP status is guarded; any other failure (connection error, timeout, malformed
JSON) raises unhandled with no top-level `try/except` anywhere in the file. Example real output:

```
Status: 200
Success! Data saved successfully.
Total records: 1846
Total columns: 22
File saved at: .../output/course_catalogue_data.csv
```

## 11. Dependencies

| Dependency | Purpose |
|---|---|
| `requests` | HTTP call |
| `pandas` | `json_normalize`, DataFrame ops, CSV write |
| `common` (repo root) | `load_credentials()` |
| stdlib (`os`, `sys`, `datetime`, `pathlib`) | Paths, timestamp |

## 12. Setup & How to Run

1. Populate `../../credentials.yaml` (`institute_id` loaded but unused).
2. `pip install requests pandas`.
3. No config file, `input/` folder, or notification setup needed.

```bash
cd /home/projectdev/ela_datasets/course_catalogue_data/scripts
python3 course_catalogue_data.py
```
No CLI arguments exist.

## 13. Automation / Scheduling

None — triggered manually, no cron/systemd/Task Scheduler entry.

## 14. Important Business / Technical Rules

- No business rules exist here — deliberately the raw, unfiltered flatten, unlike `course_batch_merge.py`'s filtering/status logic.
- `ORGID` header is hardcoded to `"683"`, not `credentials.yaml`'s `ORGANIZATION_ID` (used only in the query param) — a latent inconsistency, not a deliberate rule.
- `INSTITUTE_ID` is loaded but never used — `483` is hardcoded into `BASE_URL` instead, the same pattern seen in `course_batch_merge.py`.
- Whatever Edmingle returns becomes the columns, verbatim — no "expected vs. unexpected field" concept.

## 15. Troubleshooting

| Symptom | Likely cause | Check |
|---|---|---|
| "No data returned from API", exits | Non-200 status, or empty `response` list | Printed status/error body; API key |
| Unhandled exception / traceback | Connection error, timeout, or non-JSON body — none caught | Network connectivity; the traceback's failure point |
| Output columns differ from a prior run | Edmingle changed a catalogue field — no schema validation here | Compare "Columns saved:" against a prior header |
| `ingested_at` missing from a file | Older script revision, or the confirmed code/output discrepancy (Section 8) | Re-run the current script and re-check the header |
| Row count differs from expected | Edmingle's own data changed — this script filters nothing | Cross-check against `course_batch_merge.csv` or a direct call |

## 16. Maintenance Guide

- **Endpoint/params change** → `BASE_URL` and the `HEADERS`/`params` construction.
- **Fix the `ORGID` inconsistency** → change the hardcoded `"683"` to `str(ORGANIZATION_ID)`, once confirmed safe.
- **Wire in `INSTITUTE_ID`** → replace the hardcoded `483` in `BASE_URL`.
- **Add error handling** → wrap `fetch_courses()`'s request/JSON parsing in `try/except`, consider retry/backoff similar to `attendance.py`.
- **Restore `ingested_at` in future output** → the current code already adds it; simply re-running produces a consistent file.

## 17. Security Considerations

`credentials.yaml` holds the shared API key and is gitignored. No PII — catalogue-level data
only, no student records. No `print()` call includes credential values.

## 18. Future Improvements

1. **Email the output on completion** — send a completion email that includes the run status *and* attaches the generated dataset file(s), not just a status notification.
2. **Scheduled automation** — run automatically on a defined schedule instead of a manual trigger.
3. **Data cleaning layer** — a dedicated cleaning step/script (nulls, duplicates, standardization) inside the pipeline, instead of leaving it to downstream consumers.

---
*Initial documentation: 2026-09-24, including the `ingested_at` column discrepancy verified
against `git log` and the live output file. Downstream consumer and project/technical owner:
requires confirmation.*
