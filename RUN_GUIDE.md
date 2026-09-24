# ela_datasets — Script Run Guide

Quick reference for running each pipeline on the VPS. For deep technical detail on any pipeline,
see its own `<PIPELINE_NAME>.md` file in that pipeline's folder — this guide only covers *how to
run it*.

## Runtime environment — read this first

The VPS's system Python does **not** have `pandas`/`phonenumbers`/`pycountry` installed, and
neither `python3-venv` nor sudo is available on this server to fix that directly. All scripts
that need those packages must run inside the project's own Docker image (`docker/Dockerfile`).

**Build the image once** (rebuild only if `docker/requirements.txt` or the Dockerfile changes):
```bash
cd /home/projectdev/ela_datasets
docker build -t ela_datasets -f docker/Dockerfile .
```

**Enter the container** (mounts the whole repo at `/app`, so file changes/output persist on the
VPS after you exit):
```bash
docker run --rm -it -v /home/projectdev/ela_datasets:/app ela_datasets bash
```
Once inside, every command below is identical to running Python natively — `cd` to the pipeline's
`scripts/` folder and run it. Type `exit` to leave the container.

General prerequisites for all pipelines: shared `credentials.yaml` at the repo root must have a
valid `edmingle.api_key`/`organization_id` (rotate via Section 5 below if expired).

---

## 1. Attendance

**Status: blocked — has never produced output. Confirm with the project owner before relying on it.**

Inside the container:
```bash
cd attendance/scripts
python3 attendance.py --from <YYYY-MM-DD> --to <YYYY-MM-DD>
```
- `--from`/`--to`: date range. Omit both to use the configured default lookback.
- Other flags: `--date <YYYY-MM-DD>` (single day), `--dry-run`, `--retry-failed`, `--reset-checkpoint`.
- **Check after running:** new file in `../output/` named `batch_attendance_summary_*.csv`; `../output/logs/pipeline.log` has no `ERROR`/`CRITICAL`.

---

## 2. Country_Wise_Data

Run all three stages in order — Stage 3 needs both prior outputs.

Inside the container:
```bash
cd country_wise_data/scripts
python3 ip_driven_country_data.py --config ip_driven_country_data_config.json   # Stage 1
python3 dial_code_to_country.py                                                # Stage 2
python3 merge_country_data.py                                                  # Stage 3
```
- Stage 2 needs a fresh `Student-Export*.csv` dropped into `../input/` beforehand (manual Edmingle admin-panel export) — do this on the VPS before entering the container.
- **Check after running:** `../output/merged_country_data.csv` row count matches `../input/Student-Export.csv`'s row count.

---

## 3. Course_Batch_Merge

Inside the container:
```bash
cd course_batch_merge/scripts
python3 Course_Batch_Merge.py
```
- No arguments. Takes well under a minute.
- **Check after running:** console prints `SUCCESS! Saved N rows to file.`; `../output/course_batch_merge.csv` modification time updated.

---

## 4. Course_Catalogue_Data

Inside the container:
```bash
cd course_catalogue_data/scripts
python3 course_catalogue_data.py
```
- No arguments.
- **Check after running:** console prints `Success! Data saved successfully.` with a record/column count. **Use this printed count, or a proper CSV-aware count (e.g. Python's `csv` module) — `wc -l` overcounts this specific file, since several catalogue fields contain embedded newlines inside quoted values.**

---

## 5. Edmingle_API_Key_Generator (utility — rotates the shared API key)

**Never run automatically or on a schedule.** Run only when a real key rotation is intended. This
one may prompt interactively (tutor login / email password) — make sure you used `-it` when
entering the container.

Inside the container:
```bash
cd edmingle_api_key_generator/scripts
python3 edmingle_generate_api_key.py --check-config   # validate config only, no live call
python3 edmingle_generate_api_key.py                  # full run: rotates the live key + emails it
```
- **Check after running:** confirmation email received. If stderr says the credentials file **was** updated but a later step failed, the key is already rotated — do not re-run, just fix the email config.

---

## 6. ELA_MIS_Datasets

**Run inside `tmux` — a full run takes 68–80 hours.** Start `tmux` on the VPS *before* entering
the container, so the container keeps running even if your SSH session drops.

```bash
tmux new -s vyoma
docker run --rm -it -v /home/projectdev/ela_datasets:/app ela_datasets bash
cd ela_mis_datasets/scripts
python3 edmingle_student_course_sync.py
# detach from tmux: Ctrl+B then D — reattach later with: tmux attach -t vyoma
```
- Resumable — if it crashes or the session is lost, re-enter the container and re-run the same command; it picks up from checkpoint.
- **Check after running:** `../output/edmingle_sync.log` ends with `Edmingle sync run completed`; no `SCRIPT_FAILED.txt` in `../output/`.

---

## 7. Enrollments_Reports

**Status: currently blocked by a permanent API error, and has a known code defect (`send_mail` call bug) that will crash the next run's emails. Fix both before relying on this.**

```bash
tmux new -s enrollments   # recommended for long ranges, start before entering the container
docker run --rm -it -v /home/projectdev/ela_datasets:/app ela_datasets bash
cd enrollments_reports/scripts
python3 edmingle_export.py --start-date <DD-MM-YYYY> --end-date <DD-MM-YYYY>
```
- Dates are `DD-MM-YYYY` (not `YYYY-MM-DD` — different from every other pipeline).
- Resumable — re-running the same command after a crash continues from checkpoint. Re-running the *same* already-completed range is refused; delete `../output/*.checkpoint.json` to force a redo.
- **Check after running:** log shows `Done`; `.checkpoint.json` shows `"completed": true`.

---

## 8. Session_Wise_Attendance

Run all three stages in order — each needs the previous stage's output.

Inside the container:
```bash
cd session_wise_attendance/scripts
python3 build_course_catalog.py                                   # Stage 1
python3 resolve_class_ids.py                                      # Stage 2
python3 build_session_attendance.py --start <YYYY-MM-DD> --end <YYYY-MM-DD>   # Stage 3
```
- Stages 2 and 3 auto-resume (skip already-processed rows) if re-run after a crash; pass `--restart` to force a full re-pull. Stage 1 has no resume — a crash means starting over.
- **Check after running:** each stage's log (`../output/logs/<stage>/`) ends with a `[RESULT]`/`[TOTAL]` line, no `[ERROR]`.

---

## Standalone spot-check tool (not part of the ordered run)

Inside the container:
```bash
cd session_wise_attendance/scripts
python3 attendance_crossvalidation.py --class_id <id> --start <YYYY-MM-DD> --end <YYYY-MM-DD>
```
Compares one class_id's attendance against a second Edmingle endpoint as a manual sanity check.
