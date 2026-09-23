# ELA MIS Datasets -- Student & Course Enrollment Sync

## What this pipeline does

This pipeline (`edmingle_student_course_sync.py`) pulls the full Vyoma Samskrta Pathasala student roster and each student's course enrollment / attendance history out of Edmingle and writes them to local CSV files that feed the ELA MIS datasets.

Core logic was originally written by **Shankararama Sharma**; startup checks, email alerts, crash-safe recovery, and the run() orchestration were added/improved by **Shubham**. A full run against the complete organization (122,000+ students) takes roughly **68-80 hours**, so the script is built to run for days inside a `tmux` session, survive interruptions, and resume automatically from a saved checkpoint rather than starting over.

## Folder layout

This folder is split into two subfolders:

- `scripts/` -- all source code and this pipeline's own settings:
  `edmingle_student_course_sync.py`, `edmingle_sync_config.json`,
  `notifications.yaml`. Run everything from inside `scripts/`. Also holds
  `edmingle_student_course_sync.py.bak_before_pathfix`, a pre-existing backup
  of the script taken before an earlier `SCRIPT_DIR` path-anchoring fix --
  kept only for reference/rollback, not used by anything, and its contents
  are untouched by this restructuring.
- `output/` -- everything the script generates at runtime:
  `edmingle_students.csv`, `edmingle_course_enrollments.csv`,
  `edmingle_sync_state.json` (checkpoint/resume state), `edmingle_sync.log`,
  and (transiently, mid-course-refresh) `edmingle_course_enrollments.in_progress.csv`,
  `edmingle_course_students_snapshot.csv`, plus any legacy-migration inputs
  (`students_data_2.csv`, `studentCoursesEnrolled.csv`, `PageNo.txt`) and a
  `SCRIPT_FAILED.txt` marker written on crash. All of these paths are built
  from `OUTPUT_DIR` (`scripts/../output`, resolved from the script's own
  `SCRIPT_DIR`), so they always land here regardless of the process's cwd.

Compiled bytecode (`__pycache__`) for every pipeline under `ela_datasets/`
is redirected to a single shared `ela_datasets/.pycache/` directory (via
`sys.pycache_prefix`, set at the top of `edmingle_student_course_sync.py`
before any local/third-party import) instead of a separate `__pycache__`
folder per pipeline.

## Data flow / workflow

The run has two halves, executed in order on a fresh run (`EdmingleSync.run()`):

1. **Student roster sync** (`sync_students`) -- paginates through `GET .../organization/students` (500 students per page by default), flattens each student record, and merges the results into `edmingle_students.csv`. To avoid missing students who register *while the sync is running*, each run does **not** simply resume from the next unfetched page -- `calculate_start_page()` rewinds `overlap_pages` (default 3) pages behind the last completed page before starting, so a small window of already-seen students is re-fetched and merged again. This is safe because the merge step is a de-dup by `user_id` (see Business Rules below), so re-fetching a page just overwrites the same rows with fresher data.

2. **Course/enrollment sync** (`sync_courses`) -- rebuilds the full course/attendance dataset from scratch every run (not incremental), one `GET .../admin/classes/attendance` call per eligible student. Because this rebuild can take days, the script first takes a **stable snapshot** of the eligible student list (`edmingle_course_students_snapshot.csv`) at the start of the course-refresh run. All 68-80 hours of course fetching then iterate over that frozen snapshot rather than the live `edmingle_students.csv` -- so if the roster sync updates the student list mid-run (new registrations, edits), it can't corrupt or desync the in-flight course pull. The snapshot is deleted once the course run completes successfully.

If a previous run left a course refresh `in_progress`, `run()` skips the student roster sync entirely and resumes the course pull directly from checkpoint.

## Edmingle endpoints used

- `GET https://vyoma-api.edmingle.com/nuSource/api/v1/organization/students`
  Params: `organization_id`, `is_archived=0`, `per_page`, `page`
- `GET https://vyoma-api.edmingle.com/nuSource/api/v1/admin/classes/attendance`
  Params: `user_id`, `response_type=1` -- one call per eligible student

Both calls authenticate via headers `apikey` and `ORGID` built from `../../credentials.yaml` (shared two directories up, at the root of `ela_datasets/`, since the script now lives in `scripts/`).

## Business rules that determine correct data

- **Custom fields are position-based, not name-based.** Edmingle returns each student's custom registration fields as a plain list (`customfield_data`) with no field names -- the meaning of each entry is only its position in that list. `extract_student()` maps them via a fixed `index_mapping`:
  - index `19` -> `PhoneNumber`
  - index `9` -> `Age`
  - index `6` -> `LastName`
  - index `0` -> `UserName`

  If Edmingle ever changes the order/count of custom fields returned for the organization, these indices would silently map to the wrong values -- there is no name-based fallback.

- **Student de-dup is last-write-wins by `user_id`.** `merge_students()` builds a dict keyed by `user_id`; existing rows are loaded first, then newly fetched rows are applied on top, so a freshly fetched row always overwrites an older stored row for the same `user_id`. Rows with an empty `user_id` are silently dropped.

- **Overlap-page rewind.** `calculate_start_page(last_completed_page, overlap_pages)` returns `max(1, last_completed_page - overlap_pages)`, so every roster sync re-walks the last few pages already fetched, to catch students who registered mid-run. This depends on the last-write-wins merge above.

- **Course-pull eligibility.** `_valid_course_users()` only includes students whose `user_id` is non-empty **and** not the literal string `"NA"`. Everyone else is excluded from the course/attendance pull entirely.

- **Atomic snapshot / in-progress-file crash safety.** The course pull writes rows to `edmingle_course_enrollments.in_progress.csv` (never directly to the final file), tracking a byte `output_offset` in the state file after every student. If the run crashes and resumes, `_prepare_progress_for_resume()` truncates the in-progress file back to that last confirmed safe offset before continuing, so a crash mid-write can't leave a half-written row. Only when every student in the snapshot has been processed does the script `os.replace()` the in-progress file onto the final `edmingle_course_enrollments.csv` and delete the snapshot -- so the final CSV either doesn't change or changes atomically as a whole file swap, never partially.

## Configuration

Credentials/notifications loading, email-sending mechanics, the rolling-window rate limiter, and the atomic-write/timestamp/duration helpers below now come from the shared `../../common.py` (see its docstring) rather than pipeline-local copies -- `edmingle_student_course_sync.py` keeps the same function/class names (`_load_credentials`, `_load_notifications_config`, `send_email_alert`, `RollingRateLimiter`, `atomic_write_json`, `atomic_write_csv`, `read_csv_rows`, `utc_now`, `format_duration`) for backward compatibility, just delegating internally.

- **`../../credentials.yaml`** (shared two directories up, at the root of `ela_datasets/`, used by every pipeline there): `edmingle.api_key`, `edmingle.organization_id`. Loaded via `common.load_credentials()`; a missing file or malformed YAML raises immediately (never silently proceeds with a bad/missing key), matching the original behavior -- important given the 68-80 hour unattended runtime.
- **`notifications.yaml`** (`scripts/`, not committed/shared): SMTP host/port, from-address, Gmail app password, alert recipient list, and `status_update_interval_hours` (how often, in hours, the periodic status-update email fires during the course pull) under `channels.email`. The file is still required to exist (a missing file is a hard failure, as before); once found, its contents are read via `common.load_notifications()`. Email sending itself (`send_email_alert`) now delegates its connect-and-send mechanics to `common.send_mail()`, which tolerates this pipeline's from-address-as-login-username convention and accepts `to_addresses` as a list; sending still never raises -- a disabled/incomplete email config just logs a warning and skips the send.
- **`edmingle_sync_config.json`** (`scripts/`): pagination (`students_per_page`, `overlap_pages`), rate limiting (`max_calls_per_minute`), retry/timeout behavior (`request_timeout_seconds`, `initial_retry_delay_seconds`, `maximum_retry_delay_seconds`, `rate_limit_block_seconds`), and the `files` section listing output/state/log filenames. These filenames are now always resolved against `OUTPUT_DIR` (`scripts/../output`, derived from this script's own `SCRIPT_DIR`) regardless of the process's current working directory at launch -- see "How to run" below.

## Reliability features

- **`RollingRateLimiter`** -- a true rolling 60-second window (a `deque` of call timestamps), not a fixed sleep-between-calls interval, so bursts are smoothed correctly against `max_calls_per_minute` rather than just spaced evenly.
- **Byte-offset crash-safe resume** for the course pull -- see the atomic snapshot / in-progress-file rule above.
- **Periodic status-update emails** during the course pull, sent every `status_update_interval_hours` (configurable in `notifications.yaml` -> `channels.email`, default `6` hours) of active processing time -- the script computes `STATUS_UPDATE_INTERVAL` (in seconds) from this config value at load time rather than hardcoding it. This tracks `active_elapsed_seconds`, actual processing time, not wall-clock time, so a paused/interrupted run doesn't fire a burst of stale status emails on resume.
- **Startup checks** (`run_startup_checks`) before the long run begins: Python version must be >= 3.8, at least 2 GB free disk space, and a lightweight 1-row API call to validate the API key -- each failure sends an email alert and exits immediately rather than letting a bad key or full disk surface 60+ hours into the run.
- **Email alerts** on start, completion, mid-run status, and failure (including 401 API-key-expired mid-run), all sent via `notifications.yaml` SMTP settings and silently skipped (logged, not crashed) if no app password is configured.
- **Automatic legacy-file migration** (`migrate_legacy_files`) -- one-time, runs silently only if the new-format files don't exist yet but old-format ones do.

## How to run

```
cd /home/projectdev/ela_datasets/ela_mis_datasets/scripts
python3 edmingle_student_course_sync.py
```

Optionally override the config file location with `--config /path/to/other_config.json`. Output, state, and log files always land in the sibling `output/` folder (`ela_mis_datasets/output/`) regardless of the directory the command is run from, thanks to the `SCRIPT_DIR`/`OUTPUT_DIR` path-anchoring.

## Output files produced

All written to `output/` (see "Folder layout" above):

- `edmingle_students.csv` -- full deduplicated student roster
- `edmingle_course_enrollments.csv` -- one row per class session per eligible student (attendance + enrollment detail)
- `edmingle_sync_state.json` -- checkpoint/resume state (last completed student page, course-refresh progress)
- `edmingle_sync.log` -- full run log

All are anchored to `output/` regardless of invocation cwd.

## Known limitations / things to watch for

- The 68-80 hour runtime for a full run is **inherent to the one-call-per-student course/attendance rebuild**, not a bug -- there is no batch/bulk attendance endpoint in use.
- The course/enrollment pull is a **full rebuild every run**, not incremental -- the entire student list snapshot is re-walked each time, even though the roster sync itself is incremental with overlap.
- The custom-field `index_mapping` (see Business Rules) is fragile: it depends on Edmingle always returning `customfield_data` in the same fixed order for this organization. There is no defensive check if the API response shape changes.
- Legacy migration logic (`legacy_student_master` / `students_data_2.csv`, `legacy_course_master` / `studentCoursesEnrolled.csv`, `legacy_page_state` / `PageNo.txt`) exists only to migrate one old script's output format into the new one on first run after upgrade -- it is not part of the ongoing pipeline and does nothing once the new-format files exist.
- The API-key startup validation call treats any non-200/400/401/403 response as a warning, not a failure, so it will proceed even if the key check response was unexpected (e.g. a 5xx) rather than blocking the run.
