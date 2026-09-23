# Session-wise Attendance Pipeline (3-stage)

## What this pipeline does

This pipeline answers "how many students has Vyoma served, and how well did they attend?" by pulling course/batch/attendance data out of Edmingle (Vyoma's LMS) and stitching it into one usable dataset.

The reason it's three separate stages instead of one script is a hard constraint in Edmingle's API: **you cannot fetch attendance with a `batch_id`.** Attendance is only queryable per `class_id` (a hidden subject/stream identifier), and Edmingle has no accurate documented way to go straight from `batch_id` → `class_id` — the real response shape had to be reverse-engineered. So the pipeline runs as a funnel:

1. Find out what courses/batches exist (Stage 1).
2. For each batch, resolve the real `class_id`(s) attendance is tracked against (Stage 2).
3. Only then can attendance actually be pulled, per `class_id` (Stage 3).

Each stage's output CSV is the next stage's input, so **they must be run in that order** any time upstream data changes.

## Folder layout

This folder is split into two subfolders:

- `scripts/` -- all source code and config: `pipeline_common.py`, the 4
  entry-point scripts (`build_course_catalog.py`, `resolve_class_ids.py`,
  `build_session_attendance.py`, `attendance_crossvalidation.py`),
  `config.yaml`, `notifications.yaml`, and `tests/` (the unit test suite --
  see Tests below). Run everything from inside `scripts/`.
- `output/` -- everything the scripts generate: `course_catalog.csv`,
  `class_id_lookup.csv`, `session_wise_attendance_data.csv`,
  `attendance_spotcheck.csv`, `master_attendance.csv`, per-run `.log` files,
  and the `logs/<stage_name>/` timestamped log directories.

`README.md`, `CLAUDE.md`, `PIPELINE.md`, and `RULES.md` stay at this
folder's root (not moved into `scripts/`).

Compiled bytecode (`__pycache__`) for every pipeline under `ela_datasets/`
is redirected to a single shared `ela_datasets/.pycache/` directory (via
`sys.pycache_prefix`, set at the top of each of the 5 entry-point scripts
before any local import) instead of a separate `__pycache__` folder per
pipeline.

## Data model (read PIPELINE.md/RULES.md in this folder for the authoritative version)

```
Bundle (permanent course, e.g. "Vishnu Sahasranama")
  -> Batch (batch_id: one time-limited run of that course)
       -> Class / subject (class_id: what attendance is actually tracked against)
            -> Session (one individual live class meeting)
```

One `class_id` can be broadcast/shared across multiple `batch_id`s (surfaced via `associated_masterbatches`) — relevant to avoiding cross-batch overcounting.

## Stage 1: build_course_catalog.py -> course_catalog.csv

- **Endpoints:**
  - `GET {base_url}/institute/{institute_id}/courses/catalogue?institution_id={institute_id}` — the catalogue of bundles.
  - `GET {base_url}/short/masterbatch?status={0|3}&page={n}&per_page=1000&organization_id={org_id}` — paginated batch listing, called once for status `0` (Active) and once for `3` (Completed).
- **Business rules:**
  - **Archived batches are never fetched at all** (not filtered after the fact — only status codes 0/Active and 3/Completed are requested).
  - **Explicit exclusion list** — `BATCH_IDS_TO_EXCLUDE` is a hardcoded set of 20 exact `batch_id` values (manually curated test courses/duplicates); matched only by exact ID, never by name.
  - **`Is_Latest_Batch`** — per `bundle_id`, the batch with the highest `start_date` wins; ties broken by the highest `batch_id`.
  - **`Final_Status`** — the latest batch per bundle takes its status from the catalogue if it's one of `Completed`/`Ongoing`/`Upcoming`, else blank. Every non-latest batch is forced to `"Completed"` regardless of its own catalogue status.
  - **`bundle_enrollment_count`** = sum of `batch_enrollment_count` (`admitted_students`) across every batch in a bundle, broadcast back onto every row of that bundle.
  - **Catalogue-only bundles** (no batch at all) still get a row with `Has_Batch = 0`, so the full course list is preserved even for courses with nothing currently scheduled.
  - No row-level resume — `Is_Latest_Batch` and `bundle_enrollment_count` are global aggregates that need the complete fetched dataset before any row can be written, so the output CSV is written once, atomically, after every rule above is applied.
## Stage 2: resolve_class_ids.py -> class_id_lookup.csv

- **Endpoint:** `GET {base_url}/masterbatch/{batch_id}`
- **Business rules:**
  - The real response shape nests the actual subject-level class records under `class.courses_array[]`. The `class_id` field at the *top level* of the response is actually the **batch id**, not a class id — a documented gotcha that contradicts Edmingle's own docs; trust the live response, not the docs.
  - A batch can resolve to **more than one** `class_id` (multiple subjects/streams in one batch) — every one becomes its own output row.
  - A batch that resolves to **zero** classes still emits one row with `class_id` left blank, so it's never silently dropped from the output.
  - Full row-level checkpoint/resume via the output CSV (see "How to run" below).

## Stage 3: build_session_attendance.py -> session_wise_attendance_data.csv

- **Endpoint:** `GET {base_url}/organization/attendances?org_id={org_id}&start={unix_ts}&end={unix_ts}&class_id={class_id}` (apikey/org_id sent as both query params and headers, since Edmingle's docs disagree with themselves about which it reads).
- Imports `fetch_org_attendances`, `sessions_to_dataframe`, and `SESSION_BASE_COLUMNS` directly from **`attendance_crossvalidation.py`**, so the two scripts can never drift apart on session-shaping/status-classification logic. `attendance_crossvalidation.py` also works standalone as a single-`class_id` spot-check tool (compares against a second endpoint, `/bundle/general/attendancedet`, for a "planned vs. happened" sanity check) — output: `attendance_spotcheck.csv`.
- **Business rules:**
  - Rows in `class_id_lookup.csv` with no resolved `class_id` are skipped; duplicate `class_id`s are deduplicated before pulling.
  - Every session row is tagged with `bundle_id`/`bundle_name` so the output can be joined back to the catalog.
  - **Session status -> `session_conducted` mapping** (raw Edmingle status codes, confirmed against the classroom UI):

    | Code | Label | Conducted? |
    |---|---|---|
    | 0 | NotSignedIn | Yes |
    | 1 | SignedIn | Yes |
    | 2 | Postponed | **No** |
    | 3 | Cancelled | **No** |
    | 4 | LateSignIn | Yes |
    | 5 | MissedSignIn | Yes |
    | 6 | ExcusedAbsent | Yes |
    | 7 | Absent | Yes |

    Every row Edmingle returns is a *planned* slot; only codes `2` and `3` mean the slot didn't happen. Everything else means the class occurred — how attendance was marked is a separate concern. The raw status code/label are **not** written to the CSV, only the derived `session_conducted` boolean.
  - **Timestamps** are converted to IST (`+5:30`, a manual offset — no `pytz`/`zoneinfo` dependency) before being written; raw UTC unix fields from Edmingle never reach the CSV.
  - **`session_number`** is assigned per `master_batch_id` (not per `class_id`), in chronological order of `session_start_ist` — a broadcast `class_id` spanning multiple batches gets independently numbered in each batch.
  - Output columns fetched from the API but deliberately dropped before writing: `signin_by_name`, `signout_by_name`, `class_status_code`, `class_status_label`, `catalog_batch_name` (superseded by `bundle_name`).
  - Final output column order: `session_id, class_id, class_name, master_batch_id, master_batch_name, bundle_id, bundle_name, class_date, total_enrolled_at_session, present, not_marked, attendance_pct, taken_by_name, individual_batch_attendance, session_start_ist, session_end_ist, session_duration_min, session_conducted, session_number`
  - Full row-level checkpoint/resume via the output CSV.

## Configuration

- **`../../credentials.yaml`** (shared across all `ela_datasets/` pipelines, two folders up from `scripts/`): `api_key` (rotates ~every 30 days — update here only), `organization_id` (merged into both `org_id` and `orgid` keys, since some endpoints expect the uppercase param name), `institute_id`. Merged in automatically by `pipeline_common.load_config()` / `_merge_credentials()`.
- **`notifications.yaml`** (in `scripts/`, `chmod 600`): SMTP host/port/username/app_password/from_address/use_tls and the recipient list, under `channels.email`. Only merged onto `config["smtp"]` if `channels.email.enabled` is true. Best-effort — a missing/placeholder SMTP config just logs a `[WARN]`, never crashes a run.
- **`config.yaml`** (in `scripts/`): `base_url`, `output_folder` / `checkpoint_folder` / `log_folder` (see Known limitations — the latter two are dead config), `roster_gap_filler_enabled`, `exclude_archived_students`, `timezone`, and a `crossvalidation:` block of defaults for the standalone spot-check tool.

## How to run

Run everything from inside `scripts/`. Run in order — each stage's output feeds the next one:

```bash
cd scripts

# Stage 1: catalog
python build_course_catalog.py

# Stage 2: resolve class_ids (needs course_catalog.csv from Stage 1)
python resolve_class_ids.py

# Stage 3: pull attendance (needs class_id_lookup.csv from Stage 2)
python build_session_attendance.py --start YYYY-MM-DD --end YYYY-MM-DD
```

Resume after a crash/429/Ctrl+C: rerun the exact same command — Stages 2 & 3 auto-skip any `batch_id`/`class_id` already present in the output CSV. Pass `--restart` to wipe prior progress and start clean. Stage 1 has no row-level resume (see Known limitations) — a crash means rerunning it from scratch.

Standalone spot-check tool (not part of the ordered run — for manually verifying one class_id against the Edmingle UI):

```bash
python attendance_crossvalidation.py --class_id <id> --start YYYY-MM-DD --end YYYY-MM-DD
```

## Output files produced

All land in this pipeline's `output/` folder, anchored to the script's own location (`scripts/../output`) regardless of the invoking process's working directory (see Known limitations):

- `course_catalog.csv` — Stage 1, one row per batch (+ catalogue-only rows for bundles with no batch)
- `class_id_lookup.csv` — Stage 2, one row per resolved class_id (or one blank-class_id row per unresolved batch)
- `session_wise_attendance_data.csv` — Stage 3, session-level attendance across all batches
- `attendance_spotcheck.csv` — from the standalone `attendance_crossvalidation.py` tool, one class_id at a time

## Tests

`tests/` covers only deterministic, pure logic — never anything that makes a live HTTP call (that's what the crossvalidation spot-check tool is for). Every test builds fabricated input; none touch the real CSVs in this folder or the network.

| File | Covers |
|---|---|
| `test_build_course_catalog.py` | Exclusion-list matching (incl. non-numeric input), latest-batch selection per bundle with date tie-breaking, `Final_Status` rules for latest vs. non-latest and valid vs. invalid catalogue status, bundle enrollment summation/broadcast |
| `test_resolve_class_ids.py` | 429 message parsing (with/without fallback), `courses_array` -> record mapping (incl. joining `associated_masterbatches`), empty results, resume-set loading from an existing CSV, checkpoint CSV append (header written once, then appended) |
| `test_attendance_crossvalidation.py` | IST midnight conversion both directions, 429 parsing, empty session list, `session_conducted` for Cancelled/Postponed vs. everything else (incl. unknown status codes), `attendance_pct` division-by-zero safety, per-`master_batch_id` chronological `session_number`, whitespace-stripped batch names |
| `test_build_session_attendance.py` | Resume-set loading keyed on `class_id`, checkpoint CSV append |

Run with (from inside `scripts/`):

```bash
cd scripts
python -m pytest tests/          # all tests, ~1.5s, no network calls
python -m pytest tests/ -v
python -m pytest tests/test_build_course_catalog.py
```

Per PIPELINE.md, this was last known to be 29 tests, all passing. Run the suite after any change to shared logic (`sessions_to_dataframe`, `to_unix`/`unix_to_ist`, or the catalog business-logic functions) before trusting a real pipeline run.

## Known limitations / things to watch for

- **Stage 1 has no row-level resume.** `Is_Latest_Batch` and `bundle_enrollment_count` are global aggregates that require the complete fetched dataset, so a crash mid-fetch means the whole run restarts (Stages 2 & 3 both checkpoint against their output CSV and resume automatically).
- **`checkpoint_folder` and `log_folder` in `config.yaml` are confirmed dead config** — verified against the current code: neither key is referenced anywhere in `pipeline_common.py` or any of the 5 scripts. Checkpointing is done by re-reading the output CSV itself (Stages 2 & 3) or writing atomically at the end (Stage 1); real per-run logs always go to `logs/<stage_name>/<stage_name>_<timestamp>.log` (hardcoded relative to the script's folder), never to the configured `log_folder`.
- **Output path resolution is already correctly anchored.** `pipeline_common.resolve_output_folder()` resolves a relative `output_folder` against `<script's folder>/../output` (i.e. `scripts/../output`, this pipeline's `output/` subfolder), not the process's current working directory — confirmed by reading the code and by a functional check (calling `resolve_output_folder({'output_folder': '.'}, <absolute script path>)` from an unrelated cwd still returns the `session_wise_attendance/output/` folder). So output always lands in `output/` even if a script is ever invoked via an absolute path from a different working directory (e.g. Task Scheduler). `PipelineRunLogger` anchors its `logs/` directory the same way, so per-run logs land in `output/logs/<stage_name>/`, not next to the scripts.
- **`smtp.app_password` / notifications config**: if `notifications.yaml` is missing, disabled, or has a placeholder app password, `send_run_report()` just logs a `[WARN]` and the pipeline itself is unaffected — email reports are best-effort only.
- **Pre-recorded/self-paced content** can legitimately return session rows with `present = 0` across the board (`NotSignedIn` everywhere) — that's expected, not a fetch bug; it means live attendance was never tracked for that subject. The planned Stage 4 (`has_attendance_data` flag, catalog-anchored join) exists specifically to let downstream analysis exclude these instead of treating "no sessions returned" as zero attendance.
- **`num_users`** (from Stage 2) is an enrollment count, not attendance — don't confuse it with `present`, which only comes from Stage 3's `/organization/attendances` call.
- **Stages 4 and 5 are planned, not built**: Stage 4 would join `session_wise_attendance_data.csv` back onto `course_catalog.csv`; Stage 5 would join against a lifetime enrollment dataset for cohort retention analysis.
