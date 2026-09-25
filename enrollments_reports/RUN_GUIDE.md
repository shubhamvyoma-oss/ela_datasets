# enrollments_reports — Run Guide

Exports enrollment rows for a date range in 30-day chunks into one CSV; long ranges take hours. Details: [ENROLLMENTS_REPORTS.md](ENROLLMENTS_REPORTS.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid key; `notifications.yaml` (this folder) is optional.
- **Dates are `DD-MM-YYYY`** (every other dataset uses `YYYY-MM-DD`).
- Output is always `output/edmingle_enrollment_report.csv` and is **overwritten** by a run over a different range — copy it away first if needed.
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.

## Run (inside tmux — see below)
```bash
python3 edmingle_export.py --start-date 01-09-2026 --end-date 30-09-2026
```
Progress lines look like `[chunk 1/2 p1] wrote 200 rows …`; it ends with `Done. Wrote N rows total`.

## Check
`output/edmingle_enrollment_report.csv.checkpoint.json` shows `"completed": true`; `output/edmingle_enrollment_report.log` has no errors.

## If it stops
Re-run the same command; it resumes from the checkpoint. A range that already completed is refused — delete `output/*.checkpoint.json` to force a redo.

## tmux
```
step 1: tmux new -s enrollments_reports          start the session (name = folder name)
step 2: activate the venv, open the directory, run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/enrollments_reports/scripts
        python3 edmingle_export.py --start-date 01-09-2026 --end-date 30-09-2026
Ctrl+B then D                detach (the script keeps running)
tmux ls                      list active sessions
tmux attach -t enrollments_reports      return to the session
```
