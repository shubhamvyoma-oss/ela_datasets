# Attendance Pipeline

## 1. Overview & Purpose

`attendance.py` (v1.2.0, 1,459 lines, at `attendance/scripts/attendance.py`) pulls daily student
attendance from Edmingle (`report_type=55`), one HTTP call per day, and produces two CSVs: a
per-batch summary and a per-(batch, session) breakdown. Built to run unattended over long date
ranges (docstring cites a 546-day, ~68-minute historical run).

**Purpose:** a reliable, resumable, crash-safe attendance extraction, so no one has to babysit a
multi-day backfill or a daily run by hand.

## 2. High-Level Data Flow

```mermaid
flowchart TD
    A[CLI args: --from/--to/--date/--from-file/--dry-run/--retry-failed/--reset-checkpoint] --> B[load_config: config.yaml + ../../credentials.yaml + notifications.yaml]
    B --> C[setup_logging: TimedRotatingFileHandler, 30-day retention]
    C --> D[LockFile: PID-based, refuses concurrent run]
    D --> E[check_disk_space]
    E --> F{validate_on_startup?}
    F -->|yes| G[validate_api_connection: 1 real call for yesterday]
    F -->|no| H[build_date_list]
    G --> H
    H --> I[Checkpoint: skip dates already success, unless --retry-failed / --reset-checkpoint]
    I --> J[run_pull_loop: for each remaining date]
    J --> K[fetch_one_day: GET report/csv, report_type=55]
    K -->|200| L[write staging/raw_DATE.csv, mark checkpoint success]
    K -->|401/403/404| M[FatalAPIError -> abort run, critical email]
    K -->|429/5xx/timeout while online| N[exponential backoff + retry, circuit breaker counts]
    K -->|internet down| O[is_online/wait_for_connection: pause, probe, resume same date, no retry/breaker cost]
    L --> P{more dates?}
    P -->|yes| J
    P -->|no| Q[combine_staging_files -> raw DataFrame]
    Q --> R[clean_data: drop bad dates, dedupe, key-col check, filter_active_students]
    R --> S[resolve_session_id_column: attendance_id, fallback class_Id]
    S --> T[validate_present_value: abort run if 'P' never appears]
    T --> U[build_class_summary: per batch+session aggregates, session_number, is_conducted]
    U --> V[compute_batch_summary -> batch_attendance_summary_LABEL_TIMESTAMP.csv]
    U --> W[build_session_wise_output -> session_wise_attendance_LABEL_TIMESTAMP.csv]
    V --> X[EmailNotifier: HTML completion/critical/warning email]
    W --> X
    X --> Y[LockFile released, checkpoint left intact]
```

**Lineage:** Edmingle API → per-day staging CSVs → combined DataFrame → `clean_data()` →
`build_class_summary()` → the two output CSVs. No downstream system in this repo consumes them
automatically.

## 3. Repository Structure

| Path | Purpose |
|---|---|
| `scripts/attendance.py` | Entire pipeline — config, extraction, cleaning, summarization, email, CLI (`main()`). |
| `scripts/config.yaml` | Non-secret runtime config (API tuning, paths, behaviour flags). |
| `../notifications/attendance.yaml` | SMTP + recipients + alert-granularity toggles (repo-wide `notifications/` folder, not this pipeline's own `scripts/`). |
| `output/` | Summaries, `staging/`, `logs/`, checkpoint, lock file. |
| `../../credentials.yaml` | Shared Edmingle `api_key`/`organization_id`. |
| `../../common.py` | Shared credentials/notifications loader — not used for this pipeline's own SMTP/rate-limit code. |

## 4. Source System

| Source | Endpoint | Method | Auth | Parameters | Pagination | Rate Limit |
|---|---|---|---|---|---|---|
| Edmingle reporting API | `.../report/csv` (`config.yaml api.url`) | GET | `apikey`/`ORGID` query params from shared `credentials.yaml` | `report_type=55`, `organization_id`, `start_time`/`end_time` (one IST day per call), `response_type=1` | None — one call per calendar day | Client-side pacing only (`rate_limit_sleep_seconds`, 2.5s); reacts to server `429`/`Retry-After`. Real Edmingle-side ceiling: requires confirmation. |

**Upstream:** Edmingle LMS, plus the shared `credentials.yaml`/`common.py`.

## 5. Extraction Process

1. Parse CLI args.
2. `load_config()` — merges `config.yaml`, shared credentials, notifications; exits on missing required keys.
3. `setup_logging()` — daily-rotating file + console, IST timestamps.
4. `LockFile` — PID-based; refuses a second concurrent run, auto-clears a stale (dead-PID) lock.
5. `check_disk_space()` — aborts if free space is below `pipeline.min_free_disk_mb`.
6. If `validate_on_startup`, one real call for yesterday checks the API key/org id before the main loop.
7. `build_date_list()` — resolves dates from `--date`, `--from`/`--to`, or `default_lookback_days`.
8. Checkpoint skips already-`success` dates (unless `--retry-failed`/`--reset-checkpoint`).
9. `run_pull_loop()` calls `fetch_one_day()` per date — handles 200/429/401/403/404/400/5xx/Edmingle 6001/6002, network-outage detection, backoff, and a consecutive-error circuit breaker.
10. Each fetched day writes to `staging/raw_<date>.csv`; checkpoint updates immediately (crash-safe).
11. `combine_staging_files()` concatenates staging into one raw DataFrame (or `--from-file` skips extraction entirely).
12. `clean_data()` — parses dates, dedupes, drops rows missing key columns, flags (doesn't drop) conflicting rows, filters inactive students.
13. `resolve_session_id_column()` — `attendance_id`, falling back to `class_Id` with a warning.
14. `validate_present_value()` — aborts the run if `"P"` never appears.
15. `build_class_summary()` computes per-(batch, session) aggregates; `compute_batch_summary()`/`build_session_wise_output()` derive the two output CSVs.
16. `EmailNotifier` sends completion/critical/warning email per `notifications.yaml` — never raises on send failure.
17. Lock released on clean exit or SIGINT/SIGTERM.

## 6. Function Reference

### `load_config(config_path: str) -> dict`
Merges `config.yaml` over defaults, injects credentials/notifications, validates required keys
(exits with a clear message if missing), anchors relative paths to the script's own folder.

### `fetch_one_day(date_str, session, cfg, log, dry_run=False) -> pd.DataFrame`
One day of `report_type=55` data with retry/backoff. `200` → parsed; `429` → `Retry-After` or
backoff+jitter; `401/403/404`/Edmingle `6002` → `FatalAPIError`, no retry; `400`/`6001` → date
skipped, no retry; `5xx` → backoff+retry. On timeout/connection error, checks `is_online()` — if
the internet itself is down, waits and retries the same date for free; otherwise counts as a
normal retry. Raises `ValueError` once retries are exhausted.

### `resolve_session_id_column(df, cfg, log) -> str`
Returns `attendance_id` if present; else falls back to `class_Id` with a warning (undercounts
sessions — `class_Id` is a subject identifier, not a session).

### `filter_active_students(df, cfg, log) -> pd.DataFrame`
Allow-list filter on `studentBatchStatus` (default `["Active"]`), togglable; passes data through
unchanged (with a warning) if the toggle is off or the status column is missing.

### `clean_data(df, session_col, cfg, log) -> pd.DataFrame`
Parses `classDate`, drops unparseable/duplicate/key-incomplete rows, logs (keeps) conflicting
`(student_Id, session_col)` pairs, calls `filter_active_students()`, derives `_class_datetime` for
chronological session ordering.

### `validate_present_value(df, cfg)`
Raises `PipelineError` if the configured present-value marker ("P") never appears — guards
against a silently-all-zero run.

### `build_class_summary(df, session_col, cfg) -> pd.DataFrame`
Per-`(batch_Id, session_col)` present/absent/late/marked counts, `session_number` (chronological,
per batch), and `is_conducted` (`classDate <= TODAY`). Feeds both output builders below.

### `compute_batch_summary(df, session_col, cfg) -> pd.DataFrame`
Per-batch rollup: enrollment, planned/conducted/remaining class counts, first/last attendance,
attendance %, average/high/low class attendance, average rating, retention %, attendance drop.
Returns columns in the fixed `OUTPUT_COLUMNS` order.

### `build_session_wise_output(df, session_col, cfg) -> pd.DataFrame`
Same aggregation, reshaped to one row per (batch, session) in `SESSION_OUTPUT_COLUMNS` order.

## 7. Configuration & Parameters

| Source | Key(s) | Purpose |
|---|---|---|
| CLI | `--config` (cwd-relative, not script-relative), `--date`, `--from`/`--to`, `--from-file`, `--dry-run`, `--retry-failed`, `--reset-checkpoint`, `--verbose` | Which dates, which mode. |
| `../../credentials.yaml` | `edmingle.api_key`, `edmingle.organization_id` | Auth — fatal if missing. |
| `notifications.yaml` | SMTP host/port/user/password, recipients, per-severity toggles; Slack/Teams placeholders (unimplemented) | Alerting. |
| `config.yaml` | `api.*` (url/timeouts/retry tuning), `paths.*`, `pipeline.*` (lookback, present/absent/late values, session id column, date/time formats, active-student filtering) | Runtime behaviour. |
| Hardcoded | `VERSION`, IST offset, `OUTPUT_COLUMNS`, `SESSION_OUTPUT_COLUMNS`, extra "marked" codes `E`/`OL`/`NA` | Fixed schema/constants. |

## 8. Data Transformation, Output & Schema

**Transformations:** date parsing (drop unparseable) · exact-duplicate removal · drop rows missing
key columns · active-student allow-list · session-id resolution (`attendance_id`→`class_Id`
fallback) · zero-rating→NaN (togglable) · chronological session numbering · `is_conducted` date
flag · present-value abort check · per-session→per-batch aggregation.

**Outputs** (in `output/`; `<label>` = date or date range):

| Name | Notes |
|---|---|
| `batch_attendance_summary_<label>_<ts>.csv` | Direct `to_csv`, not atomic. |
| `session_wise_attendance_<label>_<ts>.csv` | Only if `write_session_wise_csv` (default true, unset here). |
| `attendance_raw_<label>_<ts>.csv` | Only if `save_combined_raw_csv` (true here). |
| `staging/raw_<date>.csv` | One per fetched day; enables resume. |
| `pipeline_checkpoint.json` | Per-date status, atomic write. |

**Confirmed state (2026-09-24):** `output/` has exactly one file — the batch summary (290 data
rows, 82,548 bytes) with a filesystem timestamp of today. `staging/`/`logs/` are empty, and no
checkpoint/lock/session-wise file exists — inconsistent with a normal run producing that file
(`cleanup_staging_after_combine` is `false`, so cleanup isn't the explanation). **Requires
confirmation** how/when it was actually produced.

**Database integration:** not applicable — CSV output only.

**`batch_attendance_summary_*.csv` schema** (verified against the live file):

| Column | Type | Description |
|---|---|---|
| `batch_Id`, `batchName`, `bundle_Id`, `bundleName`, `course_Id`, `courseName`, `teacher_Id`, `teacherName` | — | Source identity/name fields. |
| `total_students_enrolled` | int | Distinct students per batch. |
| `first_class_date` / `last_class_date` | date | Earliest / latest conducted session. |
| `first_class_attendance` / `last_class_attendance` | int | Present count, first/last conducted session. |
| `total_present_marks` / `total_absent_marks` | int | Summed per-session counts, conducted only. |
| `attendance_percentage` | float | Mean present ÷ enrolled × 100. |
| `average_class_attendance` / `highest_class_attendance` / `lowest_class_attendance` | float/int | Present-count stats, conducted sessions. |
| `average_rating` | float/blank | Mean rating (0 excluded if configured). |
| `retention_percentage` | float | `last / first attendance × 100`. |
| `attendance_drop` | int | `first − last`. |

`total_classes_planned/conducted/remaining` are computed but not in `OUTPUT_COLUMNS` — never reach the CSV.

**`session_wise_attendance_*.csv` schema** (per code; no live file to verify against):
`batch_Id`, `batchName`, `course_Id`, `courseName`, `session_number`, `session_id`, `classDate`,
`is_conducted`, `present_count`, `absent_count`, `late_count`, `total_marked`,
`session_attendance_percentage`.

## 9. Data Quality & Known Limitations

**Implemented checks:** unparseable-date drop, duplicate-row drop, missing-key-column drop,
conflicting-row flagging (not resolved), active-student allow-list, present-value sanity abort,
session-id fallback with warning, zero-rating handling, disk-space precheck, startup API
validation.

**Confirmed limitations:**
- Conflicting `(student, session)` rows are flagged but kept — can skew per-session counts.
- No schema validation beyond the fields the code reads directly.
- `total_classes_remaining` is effectively always 0 — `report_type=55` only reports past sessions.
- No range/outlier checks (e.g. `attendance_percentage` > 100 wouldn't be caught).
- `--config` default resolves against the caller's cwd, not the script folder — fails if run from elsewhere without an explicit path.
- `notifications.yaml` still has placeholder SMTP credentials/recipients with alerts enabled — no real email will arrive until filled in.
- The one file in `output/` has no supporting staging/log/checkpoint trail — provenance unconfirmed.
- `EmailNotifier` uses its own HTML-email code, separate from `common.send_mail()` (plain text) — routing this pipeline through the shared function later would break the HTML formatting unless that function is extended first.

**Requires confirmation:** the real Edmingle rate-limit ceiling; whether `exclude_inactive_students: false` here is intentional; how the one existing output file was produced.

## 10. Error Handling & Logging

`TimedRotatingFileHandler` (30-day retention) + console, IST timestamps. Custom exceptions:
`FatalAPIError` (401/403/404/6002, no retry) and `PipelineError` (present-value/startup-validation
failure). Exponential backoff+jitter for 429/5xx; a consecutive-error circuit breaker
(`max_consecutive_errors`) stops the run; `is_online()` distinguishes a real internet outage
(free retry) from an Edmingle-side failure. SIGINT/SIGTERM release the lock cleanly. Email sending
is always best-effort and never raises. No live log exists to quote; a reconstructed example:
`2026-09-24 00:19:43 IST | INFO     |   [2026-09-24] 42 rows fetched.` (not an actual captured line).

## 11. Dependencies

| Dependency | Purpose |
|---|---|
| `pandas` | DataFrame ops. |
| `requests` | HTTP calls. |
| `pyyaml` | Config parsing. |
| `common` (repo root) | Credentials/notifications loading. |
| stdlib (`argparse`, `logging`, `smtplib`, `socket`, `pathlib`, etc.) | CLI, logging, lock/PID, SMTP, timing. |

Docstring states: `pip install pandas requests pyyaml`.

## 12. Setup & How to Run

**Step by step:**
1. `source /home/projectdev/ela_datasets/.venv/bin/activate` — one time per shell session. Your
   prompt shows `(.venv)` when it's active; a plain `python3` after this already has `pandas`,
   `requests`, `pyyaml` installed, so no `pip install` step is needed.
2. Populate `../../credentials.yaml` (`edmingle.api_key`, `edmingle.organization_id`) — shared by
   every pipeline, so this is likely already done.
3. Populate `../notifications/attendance.yaml` if email alerts are wanted (currently placeholders).
4. Check `config.yaml` flags match intent (`exclude_inactive_students` is currently `false` here).
5. `cd /home/projectdev/ela_datasets/attendance/scripts` and run one of the commands below.
6. **For large date ranges, run inside `tmux`/`screen`.** At the pipeline's own documented rate
   (~7.5s/day, from its 546-day/~68-minute docstring example), a multi-year range — like the
   2020-01-01 to 2026-07-30 range behind the one output file currently in `output/` — takes
   roughly 5 hours. Running it in a detached session avoids losing the run to an SSH disconnect.
   A routine single-day/incremental run does not need this.

```bash
source /home/projectdev/ela_datasets/.venv/bin/activate
cd /home/projectdev/ela_datasets/attendance/scripts
python3 attendance.py --from 2026-01-01 --to 2026-01-31
python3 attendance.py --date 2026-06-15
python3 attendance.py                       # uses default_lookback_days
python3 attendance.py --from-file raw.csv   # summarize an existing raw CSV, skip extraction
python3 attendance.py --dry-run --from 2026-01-01 --to 2026-01-07
python3 attendance.py --retry-failed
python3 attendance.py --reset-checkpoint
```

## 13. Automation / Scheduling

None — triggered manually. If unattended auto-restart is ever wanted, a Linux-native wrapper
(e.g. a small shell script + `systemd`/`cron` retry) would need to be written; no such wrapper
exists today (a stale Windows `.bat` version from a different machine was removed as dead weight
during this doc's 2026-09-25 cleanup).

## 14. Important Business / Technical Rules

- `session_id_column` must be `attendance_id`, not `class_Id` — the fallback exists because using `class_Id` has previously undercounted sessions across most batches in a run.
- `studentRating == 0` means "not rated," not a real zero, when the zero-as-missing toggle is on (current default).
- Present-value abort is strict by design — a whole-run failure, not a warning, to avoid silently-all-zero output.
- `is_conducted` is a pure date check (`classDate <= TODAY`), not an Edmingle-provided flag.
- `exclude_inactive_students` is `false` on this installation despite the code's own `true` default — Archived students are currently included in attendance %.
- Duplicate (student, session) rows are never auto-resolved, only logged.
- Status handling is asymmetric by design: 401/403/404/6002 fatal, 400/6001 skip-this-date, 429/5xx/timeout retry.

## 15. Troubleshooting

| Symptom | Likely cause | Check |
|---|---|---|
| "Config file not found" on start | Wrong cwd, no `--config` passed | Run from `scripts/` or pass an absolute path |
| "Shared credentials file not found" | `../../credentials.yaml` missing/incomplete | Verify its `edmingle:` block |
| Refuses to start, mentions lock file | Prior run's PID still alive, or stale lock | Check `output/pipeline.lock` |
| Aborts with `PipelineError` re: `present_value` | Configured "P" marker never appeared | Check `pipeline.present_value` vs a raw API sample |
| Repeated "falling back to class_Id" warnings | `attendance_id` missing from response | Investigate — sessions will be undercounted |
| Circuit breaker trips | Consecutive days exhausted retries | Check Edmingle status / API key |
| No email despite a trigger firing | Placeholder SMTP credentials | Fill in real `smtp.*`/`to_addresses` |
| Session-wise CSV missing | `write_session_wise_csv: false`, or `--from-file` used | Check the config flag |

## 16. Maintenance Guide

- **Endpoint/params change** → `_day_params()` and `config.yaml api.url`.
- **New/renamed status codes** → `pipeline.present_value`/`absent_value`/`late_value`; the hardcoded `E`/`OL`/`NA` codes live in `build_class_summary()` itself.
- **Output schema change** → `OUTPUT_COLUMNS`/`SESSION_OUTPUT_COLUMNS` plus the summary-building functions.
- **Retry/backoff tuning** → `config.yaml api.*`, no code change needed.
- **New notification channel** → `notifications.yaml` has Slack/Teams placeholders, but `EmailNotifier` needs new send logic.

## 17. Security Considerations

`credentials.yaml`/`../notifications/attendance.yaml` hold secrets and are gitignored
(the `notifications/` folder is `chmod 700`, each file inside it `chmod 600`). The API key is masked in at least one log line (not exhaustively checked elsewhere).
Alert emails carry operational details only — no individual-student PII in the output CSVs.

## 18. Raw API Payload (Skeleton)

**Not a captured live response** — Edmingle credentials/session weren't used to make a fresh call
for this document. This is a skeleton built from the raw field names already confirmed elsewhere
in this doc (Sections 4, 6, 9, 14 — `attendance_id`, `class_Id`, `studentRating`,
`studentBatchStatus`, etc. are exact field names the code reads, not guesses). The response
envelope (top-level wrapper key, pagination fields) is **unconfirmed** — replace this whole block
with a real captured response the next time the pipeline runs.

```json
{
  "_comment": "TO CONFIRM: real top-level wrapper key/shape — this is a placeholder guess",
  "data": [
    {
      "batch_Id": "<TO CONFIRM>",
      "batchName": "<TO CONFIRM>",
      "bundle_Id": "<TO CONFIRM>",
      "bundleName": "<TO CONFIRM>",
      "course_Id": "<TO CONFIRM>",
      "courseName": "<TO CONFIRM>",
      "teacher_Id": "<TO CONFIRM>",
      "teacherName": "<TO CONFIRM>",
      "student_Id": "<TO CONFIRM>",
      "attendance_id": "<TO CONFIRM: preferred session-id field>",
      "class_Id": "<TO CONFIRM: subject/stream id, NOT a session id -- do not use as session_id_column>",
      "classDate": "<TO CONFIRM: format matches config.yaml pipeline.date_format, e.g. '03 Jan 2026'>",
      "studentBatchStatus": "<TO CONFIRM: e.g. 'Active' / 'Archived' / 'Cancelled'>",
      "studentRating": "<TO CONFIRM: 0 means 'not rated', not a real zero>",
      "markStatus": "<TO CONFIRM: maps to pipeline.present_value / absent_value / late_value>"
    }
  ]
}
```

## 19. Future Improvements

1. **Email the output on completion** — send a completion email that includes the run status *and* attaches the generated dataset file(s), not just a status notification.
2. **Scheduled automation** — run automatically on a defined schedule instead of a manual trigger.
3. **Data cleaning layer** — a dedicated cleaning step/script (nulls, duplicates, standardization) inside the pipeline, instead of leaving it to downstream consumers.

---
*Initial documentation: 2026-09-24. Project/technical owner: requires confirmation.*
