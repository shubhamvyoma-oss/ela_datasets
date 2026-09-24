# ela_datasets

Eight independent pipelines that pull data from Vyoma's Edmingle LMS API and produce CSV
datasets — attendance, enrollments, course catalogues, and student records. Each pipeline is a
self-contained script (or small set of scripts) run manually by an operator; there is no
orchestration layer or scheduler.

## Contents

- [Pipelines](#pipelines)
- [Quick start](#quick-start)
- [Repository layout](#repository-layout)
- [Documentation map](#documentation-map)
- [Shared infrastructure](#shared-infrastructure)
- [Configuration & secrets](#configuration--secrets)
- [What's intentionally not shared](#whats-intentionally-not-shared)
- [Conventions for changes](#conventions-for-changes)

## Pipelines

| Pipeline | What it does | Docs |
|---|---|---|
| `attendance/` | Daily attendance (`report_type=55`) → batch + session summaries | [ATTENDANCE.md](attendance/ATTENDANCE.md) |
| `country_wise_data/` | 3-stage: geo-IP + dial-code country signals, merged | [COUNTRY_WISE_DATA.md](country_wise_data/COUNTRY_WISE_DATA.md) |
| `course_batch_merge/` | Catalogue merged with batches (all statuses) | [COURSE_BATCH_MERGE.md](course_batch_merge/COURSE_BATCH_MERGE.md) |
| `course_catalogue_data/` | Raw, unfiltered flatten of the course catalogue | [COURSE_CATALOGUE_DATA.md](course_catalogue_data/COURSE_CATALOGUE_DATA.md) |
| `edmingle_api_key_generator/` | Rotates the shared Edmingle API key (manual, occasional) | [EDMINGLE_API_KEY_GENERATOR.md](edmingle_api_key_generator/EDMINGLE_API_KEY_GENERATOR.md) |
| `ela_mis_datasets/` | Full student roster + course/attendance sync (~68–80h full run) | [ELA_MIS_DATASETS.md](ela_mis_datasets/ELA_MIS_DATASETS.md) |
| `enrollments_reports/` | Row-level enrollment export over a date range | [ENROLLMENTS_REPORTS.md](enrollments_reports/ENROLLMENTS_REPORTS.md) |
| `session_wise_attendance/` | 3-stage funnel: catalogue → class_id → per-session attendance | [SESSION_WISE_ATTENDANCE.md](session_wise_attendance/SESSION_WISE_ATTENDANCE.md) |

Each linked doc is the source of truth for that pipeline: exact endpoints and why they're used,
business rules, output schema, known limitations, and current status. This README stays
intentionally high-level so it doesn't go stale the way per-pipeline detail would.

## Quick start

```bash
source /home/projectdev/ela_datasets/.venv/bin/activate   # once per shell session
cd <pipeline>/scripts
python3 <script>.py [args]
```

The venv already has every pipeline's dependencies installed. See **[RUN_GUIDE.md](RUN_GUIDE.md)**
for the exact command for each of the 8 pipelines, tmux guidance for the long-running ones, and
how to recreate the venv if it's ever lost.

## Repository layout

```
ela_datasets/
├── credentials.yaml        # shared Edmingle API key/org id/institute id (gitignored)
├── common.py                # shared helpers: credentials/notifications loading, SMTP, rate limiter
├── .venv/                   # shared Python environment (gitignored)
├── docker/                  # alternative containerized runtime
├── RUN_GUIDE.md              # how to run each pipeline
├── NOTIFICATIONS.md           # local-only index of who gets emailed (gitignored)
└── <pipeline>/
    ├── <PIPELINE>.md         # that pipeline's full technical documentation
    ├── notifications.yaml    # that pipeline's own SMTP/recipient config (gitignored)
    ├── scripts/              # the pipeline's code (+ its own config.yaml/*.json, if any)
    └── output/               # everything generated at runtime (gitignored)
```

## Documentation map

| Doc | Purpose |
|---|---|
| `README.md` (this file) | Project overview, setup, conventions |
| `RUN_GUIDE.md` | Exact run command for every pipeline |
| `<pipeline>/<PIPELINE>.md` | Deep technical doc per pipeline — endpoints, schema, rules, limitations, raw payload skeleton |
| `NOTIFICATIONS.md` | Local-only index of which pipeline emails whom (gitignored, not on GitHub) |

## Shared infrastructure

- **`credentials.yaml`** (repo root, gitignored) — the one Edmingle API key/org id/institute
  id/tutor login every pipeline reads. Only `edmingle_api_key_generator` writes to it.
- **`common.py`** (repo root) — shared code for credentials/notification loading, SMTP sending,
  a rolling-window rate limiter, and crash-safe atomic file writes, consolidated from what used
  to be 3–7 near-identical copies of each across pipelines. See its own docstring for the full
  interface.
- **`.pycache/`** (repo root, gitignored) — shared bytecode cache for every pipeline.
- **`.venv/`** (repo root, gitignored) — shared virtual environment; see RUN_GUIDE.md.

### Working on a pipeline that uses `common.py`

Any entry-point script that imports it does:
```python
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import common
```
right after setting `sys.pycache_prefix` and before any other local import. All 8 pipelines use
it for credentials/notifications loading. `enrollments_reports`, `country_wise_data`, and
`ela_mis_datasets` also use its `RollingRateLimiter`; `edmingle_student_course_sync.py` and
`enrollments_reports/edmingle_export.py` also use its atomic-write/`format_duration`/`utc_now`
helpers. `attendance.py` and `session_wise_attendance/pipeline_common.py` deliberately keep their
own rate-limiter/email-sending mechanics — see each one's own doc for exactly why (a different
email format for `attendance.py`; a flat delay instead of a rolling window for
`pipeline_common.py`, since forcing them together would change how those pipelines actually pace
requests).

## Configuration & secrets

| File | Scope | Committed? |
|---|---|---|
| `credentials.yaml` | Shared Edmingle API key/org id/institute id/tutor login | No — gitignored |
| `<pipeline>/notifications.yaml` | That pipeline's own SMTP settings + recipients | No — gitignored, `chmod 600` |
| `<pipeline>/scripts/config.yaml` or `*.json` | That pipeline's own runtime tuning (only where genuinely needed) | No — gitignored |
| `NOTIFICATIONS.md` | Local index mirroring real recipient addresses | No — gitignored |

No pipeline hardcodes a secret in its own source — every credential is loaded from one of the
files above at runtime. Never commit any of them; `.gitignore` already excludes all of the above
by pattern (`**/notifications.yaml`, `**/input/`, etc.).

## What's intentionally not shared

- `course_batch_merge` and `session_wise_attendance/build_course_catalog.py` both build a
  course/batch catalogue, but use different inclusion rules (Archived batches, exclusion
  mechanism, latest-batch tie-break) — documented as intentionally divergent in each pipeline's
  own doc, not a duplication to merge.
- Each pipeline's `notifications.yaml` is a separate file with its own recipients/thresholds —
  intentionally not centralized, since who gets alerted genuinely differs per pipeline.
- `experiments/` (out of scope for this repo; see `.gitignore`).

## Conventions for changes

- Prefer stdlib/pandas-native operations over hand-rolled loops (vectorized filters/joins over
  `while i < len(...)` index tracking) — several pipelines were cleaned up from the latter pattern
  and stayed behaviorally identical, verified by diffing live output before/after.
- Comments explain *why*, not *what* — the code should read clearly enough that a comment
  restating the next line isn't needed. Keep the ones that capture a non-obvious fact (a
  confirmed API quirk, a bug that was fixed, a deliberate tradeoff).
- Never invent data, row counts, or endpoint behavior in documentation — mark anything unverified
  as `[TO CONFIRM]` rather than guessing.
- Don't add a config file, notification channel, or abstraction a pipeline doesn't actually need
  — several already have zero config beyond the shared `credentials.yaml`, which is correct for
  what they do, not an inconsistency to "fix."
