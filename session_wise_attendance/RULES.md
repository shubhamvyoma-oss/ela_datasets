# Pipeline Rules

The definitive reference for **what data gets included, excluded, or derived** at every layer of the pipeline. `PIPELINE.md` covers the narrative/architecture (why the pipeline is shaped this way, how to run it); this file covers the concrete business rules each script enforces. If the two ever disagree, this file wins for rule detail — `PIPELINE.md` only summarizes and links here.

---

## Stage 1 — Course Catalog

**Scripts:** `build_course_catalog.py`
**Output:** `course_catalog.csv`

- **Batch status inclusion:** only batches with status **Active** or **Completed** are fetched. **Archived batches are never fetched at all** — not filtered out after the fact, simply never requested.
- **Explicit exclusion list:** any `batch_id` in the hardcoded `BATCH_IDS_TO_EXCLUDE` (primary) / `EXCLUDED_BATCH_IDS` (alt) set is dropped — matched **only** by exact ID, never by name. This is a manually curated list of test courses/duplicates.
- **Content-keyword exclusion (alt script only):** rows whose `bundle_name`/`batch_name` contains `test`, `flipbook`, or `audio` (case-insensitive substring match) are dropped and logged with a reason in `removed_rows_log.csv`. The primary script does not apply this filter — another point where the two scripts can diverge.
- **`Is_Latest_Batch`:** per `bundle_id`, the batch with the highest `start_date` wins; ties are broken by the highest `batch_id`.
- **`Final_Status`:** the latest batch (per bundle) takes its `Status` from the catalogue if it's one of `Completed`/`Ongoing`/`Upcoming`, else blank. **Every non-latest batch is forced to `"Completed"` regardless of its own catalogue status.**
- **`bundle_enrollment_count`** = sum of `batch_enrollment_count` (`admitted_students`) across all batches in that bundle, broadcast back onto every row of the bundle.
- **Catalogue-only bundles:** bundles with no batch at all still get a row (`Has_Batch = 0`), so the catalogue's full course list is preserved even for courses with nothing currently scheduled.
- **No row-level resume:** `Is_Latest_Batch` and `bundle_enrollment_count` are *global* aggregates that need the complete fetched dataset before any row can be written — so unlike Stages 2 & 3, this stage has no `--restart`/checkpoint mechanic. "Rollback safety" here means retries survive 429s/transient failures without losing already-fetched pages, and the output CSV is written once, atomically, only after every rule above has been applied — a crash never leaves a partial/corrupt `course_catalog.csv`.

## Stage 2 — Class ID Resolution

**Script:** `resolve_class_ids.py`
**Input:** `course_catalog.csv` → **Output:** `class_id_lookup.csv`

- For every `batch_id`, calls `GET /masterbatch/<batchId>` to resolve the real subject-level `class_id`(s) — **a `batch_id` alone cannot be used to query attendance**, this is the whole reason this stage exists.
- The real response shape nests dicts under `class.courses_array[]` — Edmingle's own docs describe a different (wrong) array-indexed shape. Trust the live response, not the docs.
- A batch can resolve to **more than one** `class_id` (multiple subjects/streams in one batch) — every one becomes its own output row.
- If a batch resolves to **zero** classes, it still emits one row with `class_id` blank, so it isn't silently dropped from the output — the row count in `class_id_lookup.csv` is never a subset of the batches without an explicit reason visible.
- Full row-level checkpoint/resume (see "Rate limiting & resume rules" in `PIPELINE.md`).

## Stage 3 — Session Attendance

**Scripts:** `build_session_attendance.py` (bulk pull) / `attendance_crossvalidation.py` (shared session-shaping logic + single-`class_id` spot-check tool)
**Input:** `class_id_lookup.csv` → **Output:** `session_wise_attendance_data.csv`

- Rows with no resolved `class_id` (self-paced/archived content with nothing attendance-trackable) are **skipped**, and duplicate `class_id`s are deduplicated before pulling.
- Every session row is tagged with `bundle_id` / `bundle_name` from the lookup file, so the output can be joined back to the catalog.
- **Session status → `session_conducted` mapping** (raw Edmingle `status` codes, confirmed against the classroom UI):

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

  Every row Edmingle returns is a *planned* slot — only codes `2` (Postponed) and `3` (Cancelled) are marked `session_conducted = False`. Every other status means the class happened; attendance-marking behavior (signed in, missed, absent, etc.) is a separate concern from whether the slot occurred. The raw status code/label are **not** written to the output CSV — only the derived `session_conducted` boolean is.
- **Timestamps:** all dates/times are converted to IST (`+5:30`, manual offset — no `pytz`/`zoneinfo` dependency) before being written. Raw UTC unix fields from Edmingle are never written to the CSV.
- **`session_number`**: assigned per `master_batch_id`, in chronological order of `session_start_ist` — not per `class_id` (a broadcast class_id spanning batches gets independent numbering in each batch).
- **`num_users`** (Stage 2's field) is an *enrollment* count, not attendance — never confuse it with `present`, which only comes from this stage's `/organization/attendances` call.
- Pre-recorded/self-paced content can still return session rows with `present = 0` across the board (`NotSignedIn` on every row) — that's expected, not a fetch error; it means live attendance was never tracked for that subject. This is exactly why Stage 4 needs a `has_attendance_data` flag rather than assuming every row in the output represents "real" attendance.
- **Output columns removed on purpose:** `signin_by_name`, `signout_by_name`, `class_status_code`, `class_status_label` are fetched from the API but not written out (not useful for downstream analysis; `session_conducted` already captures what matters from the status code). `catalog_batch_name` is likewise dropped — `bundle_name` already covers it.
- Full row-level checkpoint/resume (see "Rate limiting & resume rules" in `PIPELINE.md`).

## Stage 4 — Catalog-anchored join (planned)

Merge `session_wise_attendance_data.csv` back onto `course_catalog.csv`, adding a `has_attendance_data` flag — live-session attendance can only ever cover batches with actual scheduled classes, not self-paced/pre-recorded content, so this flag is what lets later analysis exclude those correctly instead of treating "no sessions returned" as zero attendance.

## Stage 5 — Cohort analysis (planned)

Join against `edmingle_course_enrollments.csv` (lifetime cumulative enrollment, 101K+ students back to 2010) to get true cohort retention curves.
