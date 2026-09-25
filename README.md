# ela_datasets

Eight independent pipelines that pull data from Vyoma's Edmingle LMS API into CSV datasets (attendance, enrollments, course catalogues, student records). Each is a self-contained script or small set of scripts run by an operator; there is no orchestration layer (only the monthly key rotation is scheduled).

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
| `edmingle_api_key_generator/` | Rotates the shared Edmingle API key (monthly, 25th 09:00 IST, or by hand) | [EDMINGLE_API_KEY_GENERATOR.md](edmingle_api_key_generator/EDMINGLE_API_KEY_GENERATOR.md) |
| `ela_mis_datasets/` | Full student roster + course/attendance sync (~68–80h full run) | [ELA_MIS_DATASETS.md](ela_mis_datasets/ELA_MIS_DATASETS.md) |
| `enrollments_reports/` | Row-level enrollment export over a date range | [ENROLLMENTS_REPORTS.md](enrollments_reports/ENROLLMENTS_REPORTS.md) |
| `session_wise_attendance/` | 3-stage funnel: catalogue → class_id → per-session attendance | [SESSION_WISE_ATTENDANCE.md](session_wise_attendance/SESSION_WISE_ATTENDANCE.md) |

Each linked doc is the source of truth for its pipeline (endpoints, business rules, schema, limitations, status); this README stays high-level.

## Quick start

```bash
source /home/projectdev/ela_datasets/.venv/bin/activate   # once per shell session
cd <pipeline>/scripts
python3 <script>.py [args]
```

The venv already has every dependency. See **[RUN_GUIDE.md](RUN_GUIDE.md)** for each pipeline's command, tmux usage and how to recreate the venv; every pipeline folder also has its own step-by-step `RUN_GUIDE.md`.

## Repository layout

```
ela_datasets/
├── credentials.yaml        # shared Edmingle API key/org id/institute id (gitignored)
├── common.py                # shared helpers: Edmingle settings, credentials/notifications, SMTP, rate limiter
├── .venv/                   # shared Python environment (gitignored)
├── docker/                  # alternative containerized runtime
├── RUN_GUIDE.md              # how to run each pipeline (index)
├── NOTIFICATIONS.md           # local-only index of who gets emailed (gitignored)
└── <pipeline>/
    ├── RUN_GUIDE.md          # step-by-step run guide (+ tmux for long runs)
    ├── <PIPELINE>.md         # that pipeline's full technical documentation
    ├── notifications.yaml    # that pipeline's own SMTP/recipient config (gitignored)
    ├── scripts/              # the pipeline's code (+ its own config.yaml/*.json, if any)
    └── output/               # everything generated at runtime (gitignored)
```

## Documentation map

| Doc | Purpose |
|---|---|
| `README.md` (this file) | Project overview, setup, conventions |
| `RUN_GUIDE.md` | Run index + environment + tmux cheat-sheet |
| `<pipeline>/RUN_GUIDE.md` | Step-by-step run guide for that pipeline |
| `<pipeline>/<PIPELINE>.md` | Deep technical doc per pipeline — endpoints, schema, rules, limitations, raw payload skeleton |
| `NOTIFICATIONS.md` | Local-only index of which pipeline emails whom (gitignored, not on GitHub) |

## Shared infrastructure

- **`credentials.yaml`** (repo root, gitignored) — the one Edmingle API key/org id/institute
  id/tutor login every pipeline reads. Only `edmingle_api_key_generator` writes to it.
- **`common.py`** (repo root) — `edmingle_settings()` (the one place the API key, organization id, institute id and base URL are read, from `credentials.yaml`; nothing is hardcoded in scripts), `auth_headers()` (the key always goes in headers, never in a URL), `get_json()` (the one HTTP GET-with-retries loop every pipeline uses: permanent statuses fail at once, a 429 waits and resets the rate limiter, everything else backs off), plus shared notification loading, SMTP sending, a rolling-window rate limiter and crash-safe atomic writes (replacing 3–7 near-identical copies of each). Entry-point scripts import it with `sys.path.insert(0, str(Path(__file__).resolve().parents[2]))` + `import common`, after setting `sys.pycache_prefix`.
- **`.pycache/`** and **`.venv/`** (repo root, gitignored) — shared bytecode cache and virtual environment. `docker/requirements.txt` pins the same package versions as the venv.

`attendance.py` and `session_wise_attendance/pipeline_common.py` deliberately keep their own rate-limiter/email mechanics (an HTML email format; a flat delay instead of a rolling window) — forcing them onto `common.py` would change how they pace requests.

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

- Prefer stdlib/pandas-native operations over hand-rolled index loops; verify behaviour-preserving cleanups by diffing live output before and after.
- Comments explain *why*, not *what*; keep the ones that record a non-obvious fact (an API quirk, a fixed bug, a deliberate tradeoff).
- Never invent data, row counts or endpoint behaviour in documentation — mark anything unverified `[TO CONFIRM]`.
- Don't add a config file, notification channel or abstraction a pipeline doesn't need; zero config beyond `credentials.yaml` is correct for several of them.
