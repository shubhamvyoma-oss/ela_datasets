# Attendance Pipeline (report_type=55)

## What this pipeline does

`attendance.py` pulls daily student attendance data from Edmingle (Vyoma's LMS/CRM
platform) for a given date range, one HTTP call per calendar day, and turns it into
two analysis-ready CSVs:

- a **one-row-per-batch summary** (enrollment, attendance %, retention, ratings, etc.)
- a **one-row-per-session** breakdown (who was present/absent/late in every class
  that has actually happened, in chronological order per batch)

It is built to run unattended over long date ranges (the header comment cites a
546-day / ~68-minute historical run) and to survive crashes, network outages, and
accidental double-launches without corrupting output or silently losing data.

## Folder layout

This folder is split into two subfolders:

- `scripts/` -- all source code and configuration: `attendance.py`,
  `config.yaml`, `notifications.yaml`, `run_pipeline.bat`. Run everything
  from inside `scripts/`.
- `output/` -- everything the pipeline generates: the combined raw CSV,
  the per-batch and per-session summary CSVs, `staging/` (per-day staging
  CSVs), `pipeline_checkpoint.json`, `pipeline.lock`, and `logs/`.

Compiled bytecode (`__pycache__`) for every pipeline under `ela_datasets/`
is redirected to a single shared `ela_datasets/.pycache/` directory (via
`sys.pycache_prefix`, set at the top of `attendance.py` before any
third-party import) instead of a separate `__pycache__` folder per
pipeline.

## Data flow / workflow

1. **Extract** — For each calendar day in the requested range, call the Edmingle
   `report_type=55` CSV endpoint for that single day (IST midnight-to-midnight) and
   save the raw response as a per-day staging CSV
   (`staging/raw_<YYYY-MM-DD>.csv`). Progress is checkpointed after every day.
2. **Combine** — Once all days are fetched (or resumed from checkpoint), all
   staging CSVs are concatenated into one raw DataFrame, and optionally saved as
   `attendance_raw_<label>_<timestamp>.csv` in the output folder.
3. **Transform / clean** — The combined raw data is cleaned and validated (see
   *Business rules* below): bad dates dropped, exact duplicates removed, key
   columns checked, inactive students filtered out, and the configured "present"
   value is confirmed to actually appear in the data before any metrics are trusted.
4. **Summarize** — Per-session and per-batch aggregates are computed
   (`build_class_summary`, `compute_batch_summary`) and written out as two CSVs.
5. **Notify** — On completion (or on a critical failure / circuit-breaker trip /
   startup validation failure), an HTML email is sent per the rules in
   `notifications.yaml`. Email sending never raises — a failed send is logged and
   the pipeline's own success/failure is unaffected by it.

## Edmingle endpoint used

- `GET https://vyoma-api.edmingle.com/nuSource/api/v1/report/csv`
- Params sent per request (`_day_params` in `attendance.py`):
  - `apikey` — from shared credentials
  - `ORGID` — **case-sensitive**, must be uppercase in the query string
  - `report_type` = `55`
  - `organization_id` — same org id as `ORGID`, as an integer
  - `start_time` / `end_time` — Unix epoch seconds, one **IST** calendar day
    (`00:00:00` to `23:59:59` IST) per call
  - `response_type` = `1`
- **One API call per calendar day** in the requested date range — never a
  multi-day window in a single call.

## Business rules that determine correct data

These are enforced in `attendance.py` as of the current version (v1.2.0) — verify
against the code (`clean_data`, `filter_active_students`,
`resolve_session_id_column`, `validate_present_value`, `build_class_summary`) if
the pipeline is ever modified, since these rules directly determine whether the
output numbers are correct:

- **Student status allow-list** (`filter_active_students`): only rows whose
  `studentBatchStatus` is in `pipeline.active_status_values` (default:
  `["Active"]`) are kept. Everything else — e.g. `"Archived"` — is dropped. This
  is an **allow-list, not a block-list**, deliberately: any new/unexpected status
  Edmingle introduces in the future is excluded by default instead of silently
  being counted. Controlled by `pipeline.exclude_inactive_students` (default
  `true` in code; currently set to `false` in this folder's `config.yaml` —
  see *Configuration* below). If the `studentBatchStatus` column is missing
  entirely, the filter is skipped and a warning is logged (all rows kept).

- **`session_id_column` must be `"attendance_id"`, NOT `"class_Id"`**
  (`resolve_session_id_column`). `attendance_id` = one actual session
  occurrence; `class_Id` = a subject/stream identifier, not a session. Using
  `class_Id` caused a real historical bug that silently undercounted sessions
  in 23 of 28 batches. The code defaults to `attendance_id` via
  `pipeline.session_id_column` in `config.yaml`, and only falls back to
  `class_Id` — with a loud warning in the logs — if `attendance_id` is missing
  from the fetched data entirely.

- **`studentRating` of exactly `0` is Edmingle's "not rated" sentinel**, not a
  real score of zero. When `pipeline.treat_zero_rating_as_missing` is `true`
  (default), any `studentRating == 0` is converted to NaN before averaging, so
  it doesn't drag down `average_rating`.

- **Present-value sanity check** (`validate_present_value`): after cleaning, the
  pipeline confirms the configured `pipeline.present_value` (default `"P"`)
  appears at least once in the `studentAttendanceStatus` column of the cleaned
  data. If it doesn't, the **entire run is aborted** with a `PipelineError`
  (not just a warning) — because every attendance metric would silently compute
  as zero otherwise.

- **`session_number`** is calculated per `batch_Id`, ordered by the session's
  actual date+time (`_class_datetime`, i.e. `classDate` + parsed `startTime`),
  via `cumcount() + 1` within each batch group in `build_class_summary`.

- **`is_conducted`** is simply `classDate <= TODAY` (IST calendar date at run
  time) in `build_class_summary` — a session is "conducted" if its class date is
  today or earlier, regardless of what the API returned for it.

- **"Marked" vs "not marked"**: a row counts toward `total_marked` only if its
  `studentAttendanceStatus` is one of `P` / `A` / `L` / `E` / `OL` / `NA`
  (`pipeline.present_value` / `absent_value` / `late_value` plus the hardcoded
  extra codes). A `"-"` (or any other placeholder value) is treated as "not
  marked" and excluded from percentage denominators.

- **Duplicate handling**: exact full-row duplicates are dropped silently. Rows
  sharing the same `(student_Id, session_id_column)` pair that are *not* exact
  duplicates (i.e. genuinely conflicting records) are **not** auto-resolved —
  all such rows are kept and a warning is logged, so a downstream consumer of
  the summary should be aware duplicate-looking sessions are possible in rare
  cases.

- **Key-column completeness**: any row missing `batch_Id`, `student_Id`, or the
  resolved session id column is dropped before aggregation, with a warning
  logged with the count.

- **HTTP/application-level status semantics** (`fetch_one_day`):
  - `200` → parsed normally.
  - `429` → respects `Retry-After` header if present, otherwise exponential
    backoff with jitter; retried (consumes a retry attempt).
  - `401` / `403` / `404` → `FatalAPIError`, **never retried**, aborts the run
    immediately and sends a critical email (these mean a config problem, not a
    transient issue).
  - `400` → logged and that date is skipped (returns an empty DataFrame; not
    retried — treated as a bad-parameters response for that day only).
  - `>= 500` → exponential backoff + retry.
  - Edmingle application error code `6001` → invalid params, date is skipped.
  - Edmingle application error code `6002` → treated as an auth failure,
    raises `FatalAPIError` (same as 401/403).
  - Missing `"data"` key in an otherwise-200 JSON response → treated as
    retryable (logged, backoff, retry).
  - Empty `data` list → valid "quiet day" (0 sessions that day), not an error;
    resets the consecutive-failure counter.

## Configuration

- **`../../credentials.yaml`** (two levels up from `scripts/`, shared across
  every pipeline under `ela_datasets/`): `edmingle.api_key`,
  `edmingle.organization_id`. Loaded by `load_config()`; the run exits
  immediately if this file or either value is missing.
- **`notifications.yaml`** (in `scripts/`, per-pipeline, not shared): SMTP host/
  port/username/app-password/from-address/timeout, `to_addresses`, and the
  three notification toggles: `notify_on_critical`, `notify_on_warning`,
  `notify_on_completion` (under `channels.email`). `channels.slack` /
  `channels.teams` exist as placeholders (`enabled: false`) for future use but
  are not implemented in `attendance.py` yet.
- **`config.yaml`** (in `scripts/`): everything else —
  - `api:` — endpoint URL, timeout, rate-limit sleep between day-calls
    (`rate_limit_sleep_seconds`, currently 2.5s to stay under a 25 req/min
    ceiling), retry/backoff tuning (`max_retries`, `retry_backoff_base_seconds`,
    `retry_backoff_max_seconds`, `retry_jitter_seconds`), the circuit-breaker
    threshold (`max_consecutive_errors`), and `validate_on_startup`.
  - `paths:` — `output_folder` (`../output`) and `log_folder`
    (`../output/logs`), plus optional overrides for `staging_folder` /
    `checkpoint_file` / `lock_file`, all resolved relative to `scripts/`
    (this script's own folder) so generated files land in `attendance/output/`
    regardless of the caller's cwd — see *Known limitations* for history.
  - `pipeline:` — lookback window, whether to save the combined raw CSV,
    whether to clean up staging files after combining, minimum free disk space,
    the attendance status codes (`present_value` / `absent_value` /
    `late_value`), `session_id_column`, date/time formats,
    `treat_zero_rating_as_missing`, and the student-status allow-list
    (`exclude_inactive_students`, `active_status_values`).

Both `credentials.yaml` and `notifications.yaml` contain secrets and must stay
out of version control (`notifications.yaml` is already `chmod 600` on this
server).

## Reliability features

- **Circuit breaker**: after `api.max_consecutive_errors` (default 3)
  consecutive days each exhaust all their retries, the run stops entirely with
  a critical email, rather than grinding through a broken endpoint
  date-by-date. A single successful or empty-but-valid day resets the counter.
- **Network-outage detection vs. genuine API failure** (`is_online`,
  `wait_for_connection`): when a request raises a timeout/connection error,
  the pipeline checks whether the *internet itself* is down (raw TCP connect
  to `8.8.8.8:53` plus DNS resolution of the Edmingle host) before deciding
  how to react.
  - If the internet is down, the pipeline **pauses and probes every
    `connectivity_check_interval_seconds`** until it comes back, then resumes
    the *same* date. This waiting time does **not** consume a retry attempt
    and does **not** count toward the circuit breaker — it's not Edmingle's
    or that date's fault. `api.max_offline_wait_minutes` caps the total wait
    (`0` = wait forever).
  - If the internet is up but the specific call still failed (Edmingle itself
    is having trouble), normal exponential backoff + retry applies, and the
    circuit breaker is in play as usual.
- **Retry/backoff**: exponential backoff with random jitter
  (`retry_backoff_base_seconds`, capped at `retry_backoff_max_seconds`) for
  429s (unless `Retry-After` is given, in which case that's honored instead)
  and 5xx responses.
- **Resumable checkpoint** (`Checkpoint` class, JSON file): every date's
  outcome (`success` / `failed` / `skipped`) is recorded after it's processed.
  Re-running the same command skips dates already marked `success` (as long as
  their staging file still exists) and only fetches what's left. `--retry-failed`
  re-processes only dates marked `failed`. `--reset-checkpoint` wipes it and
  starts clean. The checkpoint file is written atomically (write to `.tmp`,
  then `Path.replace()`) so a crash mid-write can't corrupt it.
- **Lock file** (`LockFile` class): before doing anything, the pipeline writes
  its PID to a lock file. A second concurrent invocation detects the running
  PID (cross-platform: `OpenProcess` on Windows, `os.kill(pid, 0)` elsewhere)
  and refuses to start rather than double-fetching or corrupting shared
  output. A stale lock (PID no longer running) is detected and removed
  automatically.
- **SIGINT/SIGTERM handling**: Ctrl+C or a `kill` releases the lock cleanly and
  exits 0 — the checkpoint is already safe from the per-day marking, so a
  re-run resumes exactly where it left off.
- **Startup validation** (`validate_on_startup`, default `true`): before the
  main loop, one lightweight real call is made (for yesterday's date) to catch
  a bad API key, wrong org id, or endpoint problem immediately rather than
  partway through a long run.
- **Disk space check**: refuses to start if free space in `output_folder` is
  below `pipeline.min_free_disk_mb`.
- **Dry-run mode** (`--dry-run`): simulates the entire pipeline — random
  simulated row counts per day, no real HTTP calls — for testing config,
  logging, checkpoint, and output-shape behavior safely.

## How to run

Run all commands from inside `scripts/`:

```bash
python3 attendance.py --from 2026-01-01 --to 2026-01-31
python3 attendance.py --date 2026-06-15
python3 attendance.py                       # uses pipeline.default_lookback_days
python3 attendance.py --from-file raw.csv   # skip the API, summarize an existing raw CSV
python3 attendance.py --dry-run --from 2026-01-01 --to 2026-01-07   # no real API calls
python3 attendance.py --retry-failed        # re-run only dates the checkpoint marked failed
python3 attendance.py --reset-checkpoint    # wipe checkpoint, start fresh
python3 attendance.py --config /other/path/config.yaml ...
```

As of this update, `output_folder` (`../output`) and `log_folder`
(`../output/logs`) in `config.yaml` are resolved relative to **this script's
own folder** (`attendance/scripts/`), not the caller's current working
directory — `attendance.py` anchors any relative path in the `paths:`
section to `Path(__file__).resolve().parent` at config-load time. So all
output, logs, staging files, the checkpoint, and the lock file land in the
sibling `attendance/output/` folder regardless of where the script is
invoked from (a cron job, Task Scheduler, a wrapper `.bat`/shell script with
a different `cd`, etc.). Absolute paths are still honored as-is if you ever
want output on a different drive/folder.

Note: the default for `--config` is the literal string `"config.yaml"`, which
*is* resolved against the current working directory (not the script folder) —
so if you invoke the script from somewhere other than `attendance/scripts/`
without `--config`, pass `--config /full/path/to/attendance/scripts/config.yaml`
explicitly, or `cd` into `attendance/scripts/` first.

## Output files produced

All written into `output_folder` (`attendance/output/`, by default), timestamped
`YYYYMMDD_HHMMSS` at the moment each file is written:

- `staging/raw_<YYYY-MM-DD>.csv` — one per successfully fetched day (under
  `staging_folder`, default `<output_folder>/staging`)
- `attendance_raw_<label>_<timestamp>.csv` — the combined raw data for the
  whole run, only if `pipeline.save_combined_raw_csv: true` (default)
- `batch_attendance_summary_<label>_<timestamp>.csv` — one row per batch
- `session_wise_attendance_<label>_<timestamp>.csv` — one row per
  (batch, session), only if `pipeline.write_session_wise_csv: true` (default)
- `pipeline_checkpoint.json` — per-date run status (default
  `<output_folder>/pipeline_checkpoint.json`)
- `pipeline.lock` — concurrency guard, removed on clean exit (default
  `<output_folder>/pipeline.lock`)
- `logs/pipeline.log` (+ rotated `pipeline.log.YYYY-MM-DD`, 30-day retention)
  under `log_folder`

`<label>` is either the single requested date, or `<from>_to_<to>` for a range.

## Known limitations / things to watch for

- **`session_id_column` gotcha**: always confirm `pipeline.session_id_column`
  is `"attendance_id"` in `config.yaml`. If Edmingle ever renames/drops that
  field, `resolve_session_id_column()` will silently fall back to `class_Id`
  (with a warning logged, not a hard failure) and session counts will be
  undercounted again, the same way the historical 23/28-batch bug happened.
  Treat that specific warning line in the logs as a stop-and-investigate
  signal, not routine noise.
- **`exclude_inactive_students` is currently `false`** in this folder's
  `config.yaml`, even though the code's own default (and the module docstring)
  is `true`. Confirm this is intentional before relying on batch-level
  attendance percentages for anything where Archived students should be
  excluded — right now they are **not** being filtered out on this
  installation.
- **Present-value abort is strict by design**: if `pipeline.present_value`
  ("P") doesn't literally appear in a run's `studentAttendanceStatus` values,
  the whole run aborts rather than producing all-zero metrics. Don't relax
  this without a good reason — it's the one guard against a silently wrong
  full-history run.
- **`total_classes_remaining` is always 0 in practice**: it's computed as
  `planned - conducted`, but with `report_type=55`, "planned" only reflects
  sessions actually present in the report data (i.e. already conducted or
  past-dated), so this field won't reflect real future/remaining sessions
  without a different endpoint (the batch schedule endpoint) — noted directly
  in the code as a future enhancement.
- **Conflicting (student, session) rows are kept, not resolved**: if the same
  student has more than one row for the same session after exact-duplicate
  removal, all of them are kept and only a warning is logged. This can
  slightly distort per-session present/absent/late counts in rare cases;
  investigate the warning if it appears with a non-trivial count.
- **`run_pipeline.bat`** in `scripts/` is a stale artifact from a different
  machine (references `master_attendance_pipeline.py`, a `D:\Shubham\...`
  Windows path, and a hardcoded 2018–2026 date range). It does not match the
  current `attendance.py` filename or this server's environment and should be
  rewritten (or removed) before being relied on for auto-restart / Task
  Scheduler use on this server.
- **`--config` default is cwd-relative, not script-relative** (see *How to
  run* above) — this is a different code path from the `paths:` fix in this
  update and was intentionally left as-is; pass `--config` explicitly if
  invoking from outside `attendance/scripts/`.
- Slack/Teams notification channels are configured as placeholders in
  `notifications.yaml` but have no corresponding send logic in
  `attendance.py` yet — only email actually sends.
