# ela_datasets

Eight independent pipelines that pull data from Vyoma's Edmingle LMS API.
Each pipeline lives in its own folder, split into `scripts/` (code +
config) and `output/` (everything generated at runtime) -- see that
pipeline's own README for what it does, its exact endpoint(s), and its
business rules.

| Folder | What it does |
|---|---|
| `enrollments_reports/` | Row-level enrollment export over a date range |
| `attendance/` | Daily attendance (report_type=55) -> batch + session summaries |
| `course_catalogue_data/` | Raw, unfiltered flatten of the course catalogue |
| `course_batch_merge/` | Catalogue merged with batches (all statuses) |
| `session_wise_attendance/` | 3-stage funnel: catalogue -> class_id -> per-session attendance |
| `ela_mis_datasets/` | Full student roster + course/attendance sync (68-80h run) |
| `country_wise_data/` | 3-stage: ip-driven + dial-code country signals, merged |
| `edmingle_api_key_generator/` | Rotates the shared Edmingle API key |

## Shared infrastructure

- **`credentials.yaml`** (repo root, gitignored) -- the one Edmingle API
  key/org id/institute id/tutor login every pipeline reads. Only
  `edmingle_api_key_generator` writes to it.
- **`common.py`** (repo root) -- shared code for credentials/notification
  loading, SMTP sending, a rolling-window rate limiter, and crash-safe
  atomic file writes, extracted from what used to be 3-7 near-identical
  copies of each across pipelines. Each pipeline still keeps its own
  `notifications.yaml` and `config.yaml`/`*.json` -- only the loading code
  is shared. See the docstring in `common.py` for the full interface.
- **`.pycache/`** (repo root, gitignored) -- shared bytecode cache for
  every pipeline (see `edmingle_api_key_generator/README.md` for how).
- **`NOTIFICATIONS.md`** (repo root, gitignored -- not pushed to GitHub) --
  a one-page index of which pipeline emails whom, regenerated from the
  live `notifications.yaml` files. Not read by any script.

## Working on a pipeline that uses `common.py`

Any entry-point script that imports it does:
```python
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import common
```
right after setting `sys.pycache_prefix` (see that pipeline's own script
for the exact placement) and before any other local import. All 8
pipelines now use it for credentials/notifications loading.
`enrollments_reports`, `country_wise_data`, and `ela_mis_datasets` also
use its `RollingRateLimiter`; `edmingle_student_course_sync.py` and
`enrollments_reports/edmingle_export.py` also use its atomic-write/
`format_duration`/`utc_now` helpers. `attendance.py` and
`session_wise_attendance/pipeline_common.py` deliberately keep their own
rate limiter/email-sending mechanics -- see each one's own `## Configuration`
section for exactly why (different email format for attendance.py, a flat
delay instead of a rolling window for pipeline_common.py).

## What's intentionally NOT shared

- `course_batch_merge` and `session_wise_attendance/build_course_catalog.py`
  both build a course/batch catalogue, but use different inclusion rules
  (Archived batches, exclusion mechanism, latest-batch tie-break) --
  documented as intentionally divergent in each pipeline's own README, not
  a duplication to merge.
- `experiments/` (out of scope for this repo; see `.gitignore`).
