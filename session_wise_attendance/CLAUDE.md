# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A pipeline that pulls course/batch/attendance data from Edmingle's API (Vyoma's LMS) and stitches it into one dataset answering "how many students has Vyoma served, and how well did they attend?" See `PIPELINE.md` for the full data model, flow, and operational rationale, and `RULES.md` for the exact per-stage inclusion/exclusion/derivation rules — read both before touching any stage's logic, they are the source of truth and more detailed than this file.

## Commands

This folder is split into `scripts/` (all source code + config) and `output/`
(everything the scripts generate) — see README.md's "Folder layout" section.
All commands below are run from inside `scripts/`.

```bash
cd scripts

# Tests — pure logic only, no network calls, no real CSVs touched, ~1.5s
python -m pytest tests/
python -m pytest tests/ -v
python -m pytest tests/test_build_course_catalog.py
python -m pytest tests/test_build_course_catalog.py -v -k test_name   # single test

# Pipeline stages, run in order (all read config.yaml for api_key/org_id/institute_id)
python build_course_catalog.py                                       # Stage 1 -> ../output/course_catalog.csv
python resolve_class_ids.py                                          # Stage 2 -> ../output/class_id_lookup.csv
python build_session_attendance.py --start YYYY-MM-DD --end YYYY-MM-DD    # Stage 3 -> ../output/session_wise_attendance_data.csv

# Resume after a crash/429/Ctrl+C: rerun the exact same command (Stages 2 & 3 auto-skip
# already-fetched batch_id/class_id rows by reading the output CSV). Add --restart to wipe and start clean.
# Stage 1 has no row-level resume (see RULES.md § Stage 1) — a crash means rerunning from scratch.

# Spot-check one class_id against the Edmingle UI (Stage 3's sibling tool)
python attendance_crossvalidation.py --class_id <id> --start YYYY-MM-DD --end YYYY-MM-DD
```

No `requirements.txt`/`pyproject.toml` — dependencies (`pandas`, `requests`, `pyyaml`) are just expected to be present in the environment.

## Architecture

Three-stage funnel, each stage's output CSV is the next stage's input — always run them in order after any upstream change:

1. **`build_course_catalog.py`** (Stage 1) — merges Edmingle's catalogue + masterbatch endpoints into one row per batch, applying business rules (latest-batch-per-bundle selection, status derivation, exclusion list, enrollment rollup — see RULES.md § Stage 1). `build_course_catalog_alt.py` is a simpler backup implementation of the same job — **not** the one feeding Stage 2; the two can drift.
2. **`resolve_class_ids.py`** (Stage 2) — for every batch, calls `/masterbatch/<batchId>` to resolve the hidden `class_id`(s) attendance is actually queried against (a `batch_id` alone cannot fetch attendance). The real response shape (`class.courses_array[]`) contradicts Edmingle's own docs.
3. **`build_session_attendance.py`** (Stage 3) — bulk-pulls session-level attendance per `class_id`. Imports `fetch_org_attendances`, `sessions_to_dataframe`, and `SESSION_BASE_COLUMNS` from **`attendance_crossvalidation.py`** directly (not duplicated) so the two scripts can't drift on session-shaping/status-classification logic. `attendance_crossvalidation.py` also works standalone as a single-`class_id` spot-check tool.

Stages 4 (catalog-anchored join, adding a `has_attendance_data` flag) and 5 (cohort retention against lifetime enrollment) are planned, not yet built.

**`pipeline_common.py`** centralizes what every network-calling script above needs, so they can't drift into slightly different copies of the same logic: `load_config()`, `parse_retry_after_seconds()` (Edmingle 429 backoff), `resolve_output_folder()`, `RateLimiter` (30/min-safe call spacing), `PipelineRunLogger` (tees a run's console output into a timestamped file under `logs/<stage>/`), and `send_run_report()` (best-effort end-of-run email summary via `config.yaml`'s `smtp:` block — never raises, so a bad/placeholder SMTP config can't crash a pipeline run).

**Shared conventions across all 5 scripts** — apply these when editing or extending any stage:
- Rate limiting: default 24 calls/min against Edmingle's hard cap of 30/min (`--calls_per_minute`); on HTTP 429, parse Edmingle's `"Try after X minutes"` message and sleep that long + 15s buffer. Stages 2 & 3 also get full row-level checkpoint/resume via the output CSV; Stage 1 does not (global aggregates need the complete dataset before any row can be written — see RULES.md § Stage 1).
- All 5 force `sys.stdout.reconfigure(encoding="utf-8", errors="replace")` at the top — required on Windows, where non-ASCII batch/subject names (em-dashes, curly quotes) otherwise crash the run with `UnicodeEncodeError`.
- Timestamps are converted to IST (`+5:30` manual offset, no `pytz`/`zoneinfo`) before being written; raw UTC unix fields from Edmingle never reach the CSV.
- Every script's `main()` runs inside `PipelineRunLogger` and ends with `send_run_report()` — a run's full console output always lands in `logs/`, and a summary always attempts to email out, regardless of success/failure.

**`config.yaml`** is the single source of truth for `api_key`, `org_id`, `institute_id`, `output_folder`, and the `smtp:` block, read by every script via `pipeline_common.load_config()` — no script hardcodes its own copy of the API key. The `api_key` still rotates ~every 30 days; update it in `config.yaml` only.

## Tests

`tests/` covers only deterministic, pure logic (timezone math, status-code classification, resume/dedup, catalog business rules like latest-batch tie-breaking) — never anything that makes a live HTTP call, since that would burn Edmingle's rate limit and depends on Edmingle's actual response shape rather than this code. Every test builds fabricated input; none read the real CSVs in this folder or touch the network, and CSV-writing tests use pytest's `tmp_path` fixture. Run the suite after any change to shared logic (`sessions_to_dataframe`, `to_unix`/`unix_to_ist`, or the catalog functions) before trusting a real pipeline run.

Pandas gotcha worth remembering when adding tests: a column mixing `None` with floats becomes `NaN` (check with `pd.isna(...)`, not `is None`); boolean columns hold `numpy.bool_` (check with `bool(...)`, not `is True`/`is False`).
