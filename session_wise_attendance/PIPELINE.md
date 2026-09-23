# Vyoma Attendance Data Pipeline

## Goal

Build one reliable dataset that answers: **how many students has Vyoma served, and how well did they attend?**

This requires stitching together three layers of Edmingle data that don't naturally connect:

1. **Catalog** — what courses/batches exist
2. **Subject IDs** — the hidden identifier needed to query attendance
3. **Attendance** — session-by-session records, only queryable per subject ID

The critical, hard-won fact: **you cannot pull attendance with a `batch_id`.** You need the `class_id`, and Edmingle has no accurate direct "give me the class_id for this batch" documentation — the real response shape had to be reverse-engineered.

For the exact inclusion/exclusion/derivation rules each stage applies, see **[RULES.md](RULES.md)** — this file covers flow and operations, RULES.md covers "what data counts."

---

## Data model

| Term | What it means |
|---|---|
| **Bundle** | A permanent course, e.g. "Vishnu Sahasranama" |
| **Batch** (`batch_id`) | One time-limited run of that course |
| **Class / subject** (`class_id`) | A subject/stream within a batch — this is what attendance is actually tracked against |
| **Session** | One individual live class meeting |

One `class_id` can be shared/broadcast across multiple `batch_id`s (surfaced via `associated_masterbatches`) — relevant to avoiding cross-batch overcounting.

---

## Credentials & config (`config.yaml`)

Every script reads this file (via the shared `pipeline_common.load_config()`) for: `api_key` (rotates ~every 30 days — update in `config.yaml` **only**; no script hardcodes a copy anymore), `org_id` (683), `institute_id` (483), and `output_folder` (where every output CSV is written).

`checkpoint_folder` and `log_folder` are defined in the config but **not actually used** by any script — checkpointing works by re-reading the output CSV itself (Stages 2 & 3) or writing atomically at the end (Stage 1); actual per-run logs live in `logs/` (see **Logging** below), not the configured `log_folder`. Treat those two keys as dead config.

---

## Pipeline stages

### Stage 1 — Build the catalog

**Script:** `build_course_catalog.py` (primary) — merges `/institute/{id}/courses/catalogue` + `/short/masterbatch`
**Output:** `course_catalog.csv` — one row per batch (plus catalogue-only rows for bundles with zero batches)
Batch status filtering, the exclusion list, `Is_Latest_Batch`/`Final_Status` derivation, and the enrollment rollup are all documented in **[RULES.md § Stage 1](RULES.md#stage-1--course-catalog)**.

### Stage 2 — Resolve class_ids

**Script:** `resolve_class_ids.py`
**Input:** `course_catalog.csv` → **Output:** `class_id_lookup.csv`

For every `batch_id`, calls `GET /masterbatch/<batchId>` and extracts the real subject-level `class_id`(s). Response-shape quirks and the multi-class_id/zero-class_id rules are in **[RULES.md § Stage 2](RULES.md#stage-2--class-id-resolution)**.

### Stage 3 — Pull session attendance

**Script:** `build_session_attendance.py`
**Input:** `class_id_lookup.csv` → **Output:** `session_wise_attendance_data.csv`
**Supporting script:** `attendance_crossvalidation.py` — pulls attendance for **one** `class_id` at a time, for manually spot-checking a batch against the Edmingle UI (output: `attendance_spotcheck.csv`). `build_session_attendance.py` imports `fetch_org_attendances`, `sessions_to_dataframe`, and `SESSION_BASE_COLUMNS` from it directly, so the two scripts can never drift on session-shaping logic.

The session-status→conducted mapping, IST timestamp handling, `session_number` numbering, and the columns deliberately excluded from the output are documented in **[RULES.md § Stage 3](RULES.md#stage-3--session-attendance)**. Final output column order:

```
session_id, class_id, class_name, master_batch_id, master_batch_name,
bundle_id, bundle_name, class_date, total_enrolled_at_session, present,
not_marked, attendance_pct, taken_by_name, individual_batch_attendance,
session_start_ist, session_end_ist, session_duration_min,
session_conducted, session_number
```

### Rate limiting & resume rules (Stages 2 & 3, identical in both)

Edmingle allows a hard max of **30 calls/minute**. Both bulk scripts (via `pipeline_common.RateLimiter`):
- Default to **24 calls/min** (`--calls_per_minute` to tune), spacing requests and accounting for time already spent on each request.
- On HTTP 429, parse Edmingle's own `"Try after X minutes"` message and sleep that long (+15s buffer) rather than retrying quickly — a fixed 31-minute fallback is used if the message can't be parsed.
- **Checkpointing is the output CSV itself** — every batch/class_id's rows are appended and flushed immediately after a successful call, never held in memory until the end. A crash, 429 block, or Ctrl+C loses nothing already pulled.
- **Resuming is automatic** — on startup, each script reads whatever's already in the output CSV and skips any `batch_id` / `class_id` already present. Just rerun the same command. Use `--restart` to wipe prior progress and start clean.
- **Known Windows gotcha (fixed):** printing a batch/subject name containing non-ASCII characters (em-dashes, curly quotes) used to crash the whole run with `UnicodeEncodeError`, because Windows defaults stdout to cp1252 even when output is redirected to a file. All network-calling scripts now force `sys.stdout.reconfigure(encoding="utf-8", errors="replace")` at the top, so this can't happen again.

Stage 1 does **not** have row-level resume (see RULES.md § Stage 1 for why) but does apply the same rate-limit spacing and 429 backoff.

### Stage 4 — Catalog-anchored join 📋 planned

Merge `session_wise_attendance_data.csv` back onto `course_catalog.csv`, adding a `has_attendance_data` flag — live-session attendance can only ever cover batches with actual scheduled classes, not self-paced/pre-recorded content, so this flag is what lets later analysis exclude those correctly instead of treating "no sessions returned" as zero attendance.

### Stage 5 — Cohort analysis 📋 planned

Join against `edmingle_course_enrollments.csv` (lifetime cumulative enrollment, 101K+ students back to 2010) to get true cohort retention curves and finally answer "how many students has Vyoma served."

---

## Logging

Every network-calling script's `main()` runs inside a `pipeline_common.PipelineRunLogger` context manager, which mirrors everything printed to the console into a timestamped file at:

```
logs/<stage_name>/<stage_name>_<YYYYMMDD_HHMMSS>.log
```

e.g. `logs/build_session_attendance/build_session_attendance_20260825_170000.log`. Each log carries a `[RUN START]`/`[RUN END]` header/footer (start time, args, end time, duration) plus everything the script already prints during the run — row counts, skipped/error counts, rate-limit waits, and the final `[SUMMARY]`/`[RESULT]` block — so a failed or slow run can be debugged from the log file alone, without needing to have watched the console live.

## Email reports

At the end of every script's `main()`, `pipeline_common.send_run_report()` emails a plaintext summary (stage name, start/end time, duration, rows processed/written, skipped/error counts, output file path) using the `smtp:` block in `config.yaml`. This is **best-effort and non-fatal** — if `config.yaml`'s SMTP credentials are missing or still the placeholder `app_password`, it logs a `[WARN] Email report failed: ...` line and the pipeline run itself is unaffected. Fill in a real Gmail App Password in `config.yaml`'s `smtp:` block to start actually receiving these.

---

## File structure

```
session_wise_attendance/
│
├── README.md                         pipeline overview & how to run
├── PIPELINE.md                       this file — flow & operations
├── RULES.md                          per-layer inclusion/exclusion/derivation rules
├── CLAUDE.md                         guidance for Claude Code
│
├── scripts/                          all source code + config — run everything from here
│   ├── pipeline_common.py            shared config/rate-limit/logging/email helpers
│   ├── config.yaml                   org_id, output paths, behavior toggles (shared by all scripts)
│   ├── notifications.yaml            this pipeline's SMTP/notification settings
│   │
│   ├── build_course_catalog.py       STAGE 1 — builds the catalog
│   ├── resolve_class_ids.py          STAGE 2 — resolves class_id per batch
│   ├── build_session_attendance.py   STAGE 3 — bulk attendance pull
│   ├── attendance_crossvalidation.py spot-check tool (1 class_id at a time)
│   │                                  + shared fetch/session functions used by Stage 3
│   │
│   └── tests/                        unit tests for the pure logic in every script above
│
├── output/                           everything the scripts generate
│   ├── course_catalog.csv            → Stage 1 output: one row per batch (+ catalogue-only rows)
│   ├── class_id_lookup.csv           → Stage 2 output: one row per resolved class_id
│   ├── session_wise_attendance_data.csv → Stage 3 output: session-level attendance, all batches
│   ├── attendance_spotcheck.csv      → output of the standalone spot-check tool
│   └── logs/                         per-run, per-stage timestamped log files
│
└── (planned)
    ├── catalog_attendance_join.py    STAGE 4
    └── student_cohort_analysis.py    STAGE 5
```

Compiled bytecode (`__pycache__`) for every pipeline under `ela_datasets/` is
redirected to a single shared `ela_datasets/.pycache/` directory (via
`sys.pycache_prefix`, set at the top of each of the 5 entry-point scripts in
`scripts/` before any local import), instead of a separate `__pycache__`
folder per pipeline. `../../credentials.yaml` (two levels up from `scripts/`,
shared across all `ela_datasets/` pipelines) supplies `api_key`/
`organization_id`/`institute_id`, merged into `config.yaml`'s settings
automatically by `pipeline_common.load_config()`.

---

## Known open items

- `num_users` (Stage 2) is enrollment, not attendance — don't confuse with `present` (Stage 3).
- `checkpoint_folder` / `log_folder` in `config.yaml` are unused dead config.
- Stage 1 has no row-level resume (see RULES.md § Stage 1) — a crash mid-fetch means the whole run restarts, though it's still rate-limit-safe.
- `config.yaml`'s `smtp.app_password` is still a placeholder — email reports will log a warning on every run until it's filled in with a real Gmail App Password.

---

## Tests (`tests/` folder)

### What's tested and why

The pipeline's risk isn't crashes — the scripts fail loudly (an exception, a 429) and are safe to rerun. The real risk is **silent correctness bugs**: a timezone off-by-one, a status code misclassified as "conducted," a tie-break rule that picks the wrong "latest batch" — all of which show up as a slightly-wrong number in a CSV with no error message. `tests/` covers exactly the deterministic logic where that kind of bug can hide: date/timezone math, status classification, resume/dedup logic, and the catalog's business rules (latest-batch selection, exclusion list, status derivation, enrollment aggregation).

**Deliberately not tested:** anything that makes a live HTTP call. Those need real credentials, would burn Edmingle's 30-calls/min quota just to run the suite, and their correctness depends on Edmingle's actual response shape rather than our code — that's what `attendance_crossvalidation.py`'s spot-check mode is for, not unit tests.

### Rules

- Every test builds its own fabricated input (dicts, small DataFrames) — no test depends on the real CSVs in this folder or on network access. The suite runs the same way with an empty repo.
- CSV-writing tests use pytest's `tmp_path` fixture, never the real output files, so running tests can never corrupt `class_id_lookup.csv` or `session_wise_attendance_data.csv`.
- Pandas gotcha to remember when adding tests: a column that mixes `None` with floats becomes `NaN` (use `pd.isna(...)`, not `is None`), and boolean columns hold `numpy.bool_` (use `bool(...)`, not `is True`/`is False`).

### Test files and what each covers

| File | Covers | Key cases |
|---|---|---|
| `test_resolve_class_ids.py` | `resolve_class_ids.py` | 429 message parsing (with and without a fallback), `courses_array` → record mapping (incl. joining `associated_masterbatches` into a comma string), empty results, resume-set loading from an existing CSV, checkpoint CSV append (header written once, then appended) |
| `test_attendance_crossvalidation.py` | `attendance_crossvalidation.py` (and by extension the session shape `build_session_attendance.py` relies on) | IST midnight conversion both directions, 429 message parsing, empty session list, `session_conducted` for Cancelled/Postponed vs. everything else (incl. unknown status codes), `attendance_pct` division-by-zero safety, per-`master_batch_id` chronological `session_number`, whitespace-stripped batch names |
| `test_build_session_attendance.py` | `build_session_attendance.py` | resume-set loading keyed on `class_id`, checkpoint CSV append |
| `test_build_course_catalog.py` | `build_course_catalog.py` | exclusion-list matching (incl. non-numeric input), latest-batch selection per bundle with date tie-breaking, `Final_Status` rules for latest vs. non-latest batches and valid vs. invalid catalogue status, bundle enrollment summation and broadcast back to every row |

### Running

```bash
cd scripts
python -m pytest tests/          # all tests, ~1.5s, no network calls
python -m pytest tests/ -v       # verbose, one line per test
python -m pytest tests/test_build_course_catalog.py   # just one file
```

Currently: **29 tests, all passing.** Run this after any change to the shared logic (especially `sessions_to_dataframe`, `to_unix`/`unix_to_ist`, or the catalog business-logic functions) before trusting a new full pipeline run.
