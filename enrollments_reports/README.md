# Enrollment Reports -- Historical Enrollment Export

## What this pipeline does
Pulls row-level student enrollment records from Edmingle's enrollment report
endpoint over an arbitrary historical date range, and writes them to a single
CSV file. Because Edmingle rejects large single-shot date ranges, the full
range is split into fixed-size day windows ("chunks") that are fetched one at
a time, page by page. Every page written is checkpointed, so if the process
is killed, crashes, or the VPS reboots mid-run, it picks back up exactly
where it left off instead of re-fetching data or duplicating rows.

## Folder layout

This folder is split into two subfolders:

- `scripts/` -- all source code and config: `edmingle_export.py` (the
  orchestrator/entry point -- also holds checkpoint load/save and log setup,
  inlined from the former `edmingle_checkpoint.py`/`edmingle_logger.py`,
  each of which was a few lines wrapping a single stdlib/helper call, and
  now also holds the settings that used to live in `edmingle_config.json`/
  `edmingle_config.py`, both removed 2026-09-23 -- see "Configuration"),
  `edmingle_constants.py`, `edmingle_chunker.py`, `edmingle_io_utils.py`,
  `edmingle_rate_limiter.py`, `edmingle_api.py`, and `notifications.yaml`.
  Run everything from inside `scripts/`.
- `output/` -- everything generated at runtime: the enrollment CSVs, their
  matching `.csv.checkpoint.json` and `.csv.chunks.json` files, and the
  `.log` files. `edmingle_export.py` creates this folder automatically if
  it doesn't already exist.

Compiled bytecode (`__pycache__`) for every pipeline under `ela_datasets/`
is redirected to a single shared `ela_datasets/.pycache/` directory (via
`sys.pycache_prefix`, set at the top of `edmingle_export.py` before any
local import) instead of a separate `__pycache__` folder per pipeline.

## Data flow / workflow
1. `edmingle_export.py` is the orchestrator. It loads config/credentials,
   sets up logging and the rate limiter, and drives the run.
2. `edmingle_chunker.py`'s `load_or_create_chunk_plan()` splits the
   `[start_date, end_date]` range into `chunk_days`-sized windows. This plan
   is written once to a `.chunks.json` file and reused on every subsequent
   run/resume rather than being recomputed from CLI flags each time -- so
   the chunk boundaries can never silently shift between runs.
3. For each chunk, the run pages through results by calling
   `edmingle_api.py`'s `fetch_page()` (page 1, 2, 3... until
   `has_more_page` is false).
4. Each page's rows are written to the output CSV immediately, then the file
   is flushed and `fsync`'d to disk.
5. After every page write, `save_checkpoint()` (in `edmingle_export.py`)
   saves progress (`.checkpoint.json`): which chunk/page was last completed,
   total rows written so far, and the exact CSV byte offset at that point.
6. If interrupted and restarted, `edmingle_io_utils.py`'s
   `truncate_to_offset()` cuts the CSV back to the last checkpointed byte
   offset before resuming, discarding any partially-written (torn) row a
   crash might have left dangling. The run then continues appending from
   there.

## Edmingle endpoint used
- `GET https://vyoma-api.edmingle.com/nuSource/api/v1/reports/enrollment`
- Params: `start_date`, `end_date` (DD-MM-YYYY, one chunk window at a time),
  `time_step=1`, `report_details_type=3`, `page`, `per_page`, `sort_order=D`,
  `sort_by=date_of_enrolment`, `currency_id=1`
- Auth headers: `apikey`, `orgid`

## Business rules that determine correct data
- **Chunking**: `build_chunks()` splits an inclusive DD-MM-YYYY date range
  into windows of at most `chunk_days` days (default 30). `start_date` must
  not be after `end_date`.
- **Chunk plan persistence**: the chunk plan is computed once and persisted
  to `.chunks.json`. On every later run for the *same* start/end/chunk_days
  it is loaded back from disk rather than recomputed -- if start/end/
  chunk_days differ from what's on disk, the plan is regenerated. This keeps
  date-window boundaries stable and inspectable across resumes.
- **Resume logic**: on startup the run compares the saved checkpoint's
  `start_date`, `end_date`, `chunk_days`, and `per_page` against the current
  invocation's values:
  - identical params + not yet completed -> resumes from the exact
    chunk/page in the checkpoint.
  - identical params + already completed -> refuses to run again (logs a
    message telling you to delete the `.checkpoint.json` to force a rerun).
  - different params -> starts completely fresh and **overwrites** the
    existing output CSV.
- **Null handling**: any field Edmingle returns as `None` is written to the
  CSV as an empty string, not the literal text "None".
- **Unknown/missing fields**: the CSV column order is fixed by `FIELDS` in
  `edmingle_constants.py`. Extra fields Edmingle returns are dropped;
  fields Edmingle omits are written blank -- neither case crashes the run.
- **Error handling**: HTTP 400/401/403/404 are treated as permanent (bad
  API key, bad org id, bad endpoint) and stop the run immediately with no
  retry. HTTP 429 triggers a long cooldown (`rate_limit_block_seconds`, or
  the response's `Retry-After` if longer) and resets the rate limiter.
  Everything else (408/429-follow-up/5xx, network errors, invalid JSON,
  unexpected response shape) is retried forever with exponential backoff
  capped at `maximum_retry_delay_seconds` -- there's no internal retry-count
  limit. A permanent error emails immediately and stops (see "How to run" --
  there is no external watchdog/auto-restart; a stuck or crashed process is
  handled by manually re-running the same command, which resumes from
  checkpoint, the same pattern `ela_mis_datasets` uses).

## Configuration

Credentials/notifications loading, the rate limiter, and the atomic-write
helpers all come from the shared `../../common.py` (see its docstring)
rather than pipeline-local copies.

- **`../../credentials.yaml`** (shared across all `ela_datasets/`
  pipelines): `edmingle.api_key`, `edmingle.organization_id`. Loaded
  directly via `common.load_credentials()` in `edmingle_export.py`'s
  `EdmingleExportRun.__init__`; the script exits with a clear error if the
  file or either key is missing.
- **`notifications.yaml`** (in `scripts/`, permissions `600`): SMTP settings
  and recipients under `channels.email` (`enabled`, `smtp.host`,
  `smtp.port`, `smtp.username`, `smtp.password`, `smtp.from_address`,
  `to_addresses`). If email is disabled or SMTP isn't fully configured, the
  run logs a warning and skips sending -- it does not fail the run. (The
  file also has `channels.slack` / `channels.teams` blocks, both currently
  disabled/unused by this script.)
- **Tunables** (removed 2026-09-23: there is no longer a
  `edmingle_config.json`/`.json.example`/`edmingle_config.py` -- that file's
  own content was always literally `{}`, so every run already used these
  same values; they're now the `DEFAULTS` dict inlined directly at the top
  of `edmingle_export.py`). There is no config file or CLI flag for these --
  edit the `DEFAULTS` dict in the script if a value genuinely needs to
  change:
  - `chunk_days` (default 30)
  - `per_page` (default 200)
  - `max_calls_per_minute` (default 30)
  - `request_timeout_seconds` (default 30)
  - `initial_retry_delay_seconds` (default 2)
  - `maximum_retry_delay_seconds` (default 60)
  - `rate_limit_block_seconds` (default 300)

## Reliability features
- **RollingRateLimiter** (`edmingle_rate_limiter.py`): a deque-based *true*
  rolling window (not a flat per-call delay) -- `acquire()` tracks call
  timestamps and blocks only long enough to keep the count within
  `max_calls_per_minute` calls per 60s window. `reset()` clears it after a
  429 cooldown so resuming doesn't immediately re-trip the limit.
- **Byte-offset crash-safe CSV resume**: the checkpoint records the exact
  CSV file size at the moment right after a page's rows were written,
  flushed, and `fsync`'d. On resume, `truncate_to_offset()` cuts the file
  back to that exact byte count first, so any torn/partial write from a
  mid-write crash is discarded before appending continues.
- **Atomic JSON writes** (`edmingle_io_utils.atomic_write_json`): the
  checkpoint and chunk-plan files are written to a temp file, `fsync`'d,
  then swapped into place with `os.replace()` -- a crash mid-write can
  never leave a half-written/corrupt `.checkpoint.json` or `.chunks.json`.
- **No external watchdog/auto-restart (changed 2026-09-23).** This pipeline
  used to run under `edmingle_watchdog.sh`, a shell wrapper that restarted
  the script on any non-zero exit and only emailed failure after burning
  through 30 restart attempts -- which meant a genuinely permanent error
  (e.g. an expired key) still wasted up to ~2.5 hours of growing backoff
  before anyone was told. The watchdog is removed; `main()` now emails
  failure directly from its own exception handling the moment a permanent
  error or unexpected crash happens, matching the pattern already used in
  `ela_mis_datasets` (that pipeline has never used an external watchdog --
  it catches its own crash and emails from inside the `except` block). A
  crash is recovered the same way as before: just re-run the same command;
  the checkpoint makes it resume rather than start over.

## How to run
Run from inside `scripts/`, in `tmux` for a long historical range so it
survives an SSH disconnect:
```bash
cd scripts
python3 edmingle_export.py --start-date 01-01-2010 --end-date 06-08-2026
```
Dates are `DD-MM-YYYY`. There is no watchdog/auto-restart wrapper -- if the
process crashes or the server reboots, re-run the exact same command; the
checkpoint resumes it from the last confirmed byte offset rather than
starting over. A permanent error (bad key/org id/endpoint) or an unexpected
crash sends a failure email immediately from inside the script itself
before it exits (see "Reliability features"). `--output` is optional -- if omitted, the output
filename is auto-derived from the date range (see below) and, regardless of
the current working directory the script is invoked from, always lands in
the `output/` folder next to `scripts/`. This is because `output_path`
defaults to `SCRIPT_DIR.parent / "output" / <auto-name>` -- `SCRIPT_DIR` is
`scripts/` itself (resolved from `Path(__file__).resolve().parent`, i.e.
the script's own folder, not the caller's cwd), so the `.parent` step back
up lands on the pipeline root before descending into `output/`. The
`output/` folder is created automatically if it doesn't already exist.

`--api-key` / `--org-id` can override the credentials file per-run if
needed.

## Output files produced
All written into the `output/` folder (next to `scripts/`, not inside it),
under a **fixed filename that every run overwrites** (changed from the
original per-date-range naming -- see "Fixed output filename" below):
- `edmingle_enrollment_report.csv` -- the data
- `edmingle_enrollment_report.csv.checkpoint.json` -- resume pointer
  (chunk/page progress, rows written, byte offset, completed flag)
- `edmingle_enrollment_report.csv.chunks.json` -- the persisted
  chunk plan (list of date windows) for this exact start/end/chunk_days
- `edmingle_enrollment_report.log` -- full run log (also mirrored to
  stdout, so it's visible live if run inside tmux)

All three companion paths are derived directly from the CSV's own
`output_path` via `Path.with_suffix(...)`, so they always sit alongside the
CSV rather than depending on a separately-computed relative path.

### Fixed output filename (why, and how resume still works)

`build_default_output_name()` used to embed the requested `--start-date`/
`--end-date` in the filename (e.g. `edmingle_enrollment_01012010_06082026.csv`),
so every differently-scoped run left behind a brand new, permanent CSV --
each one can be 100MB+, so repeated runs grew `output/` unbounded. It now
always returns the fixed name `edmingle_enrollment_report.csv`, and every
run overwrites it.

This is safe with the existing resume logic without any other change,
because `_resolve_resume_state()` already compares the *checkpoint's own*
recorded `start_date`/`end_date`/`chunk_days`/`per_page` against the current
run's arguments, not the filename:
- Rerunning the **same** date range after a crash (just re-run the same
  command by hand -- there is no watchdog) still matches the checkpoint
  and resumes by truncating back to the last confirmed byte offset,
  exactly as before.
- Running a **different** date range already logged "starting fresh
  (existing output file will be overwritten)" and used write mode -- that
  behavior is unchanged, it just now always targets the same filename
  instead of creating a new one.

If you need to keep a specific run's output permanently, pass
`--output /path/to/a/name/you/choose.csv` explicitly.

## Known limitations / things to watch for
- A checkpoint marked `completed: true` will not be re-run for the same
  start/end/chunk_days/per_page -- you must manually delete the
  `.checkpoint.json` file to force a full rerun of that exact range.
- Running with different `chunk_days` or `per_page` for the same date range
  silently starts fresh and **overwrites** the existing output CSV for that
  range -- there's no separate confirmation prompt.
- Retries for transient errors are unbounded in `edmingle_api.py` itself
  (backoff capped, but no retry-count ceiling) -- since there is no
  external watchdog anymore, a transient issue that never clears (e.g. a
  sustained network outage) will retry forever rather than eventually
  giving up and notifying anyone. Check the `.log` file, or the process
  list, if a run seems to be running much longer than expected.
- Email notifications silently no-op (with a logged warning) if
  `channels.email.enabled` is false or SMTP fields are incomplete in
  `notifications.yaml` -- a run can succeed or fail without you being
  notified if that file isn't fully configured.
- Tunables (`chunk_days`, `per_page`, etc.) are fixed defaults in the
  `DEFAULTS` dict at the top of `edmingle_export.py` -- there is no config
  file or CLI flag for them (see Configuration above).
- `output/` used to accumulate one CSV (+checkpoint/chunks/log) per
  distinct date range ever run -- as of the fixed-filename change above,
  only the single most recent run's files exist, under
  `edmingle_enrollment_report.*`. The older per-range files (ranges ending
  06-08-2026, 24-08-2026, and 25-08-2026) were deleted; the most recent one
  at the time (ending 31-08-2026) was kept and renamed to the new fixed
  name.
