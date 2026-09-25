# enrollments_reports — Run Guide

Exports row-level enrollment data from Edmingle for a date range, in 30-day chunks, into one CSV. Long ranges take hours.
Full documentation: [ENROLLMENTS_REPORTS.md](ENROLLMENTS_REPORTS.md).

## Before you start
- **Known defect — read this first.** The "started" and "completed" emails call `send_mail` wrongly, so the next run is
  expected to crash with `TypeError: send_mail() got multiple values for argument 'subject'` right at the start. This has not
  been fixed yet. The API side was working in a recent check, so only this defect is expected to stop a run.
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid Edmingle key. `notifications.yaml` (this folder) is optional.
- **Dates are `DD-MM-YYYY`** — the only dataset that differs from the `YYYY-MM-DD` used everywhere else.
- The output file has a fixed name (`output/edmingle_enrollment_report.csv`) and is **overwritten** by a run over a
  different date range. Copy it away first if you need to keep it.
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.

## Step by step
1. Start a tmux session named after this folder: `tmux new -s enrollments_reports`
2. Inside it: `source /home/projectdev/ela_datasets/.venv/bin/activate`
3. `cd /home/projectdev/ela_datasets/enrollments_reports/scripts`
4. Run it with the range you want:
   ```bash
   python3 edmingle_export.py --start-date 01-09-2026 --end-date 30-09-2026
   ```
5. Watch the log lines `[chunk 1/2 p1] wrote 200 rows ...`; it ends with `Done. Wrote N rows total`.
6. Detach and leave it: `Ctrl+B` then `D`. Come back with `tmux attach -t enrollments_reports`.

## If it stops or crashes
- Re-run **the same command**: it resumes from the saved checkpoint (it cuts the file back to the last confirmed byte first).
- Re-running a range that already **completed** is refused. To force a redo, delete `output/*.checkpoint.json` first.
- `output/edmingle_enrollment_report.log` has the reason.

## Check the result
- The log ends with `Done`; `output/edmingle_enrollment_report.csv.checkpoint.json` shows `"completed": true`.
- The row count in the log matches the CSV (count rows with a CSV-aware tool, not `wc -l`).

## Run it in tmux (session name = folder name)
```
step 1: tmux new -s enrollments_reports
        (starts the session -- the session name is the dataset folder name)
step 2: activate the environment, open the directory and run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/enrollments_reports/scripts
        python3 edmingle_export.py --start-date 01-09-2026 --end-date 30-09-2026
Ctrl+B then D                to detach / come out of the session (the script keeps running)
tmux ls                      to see the list of active sessions
tmux attach -t enrollments_reports      to return to / open the session
exit                         (inside the session, when the run has finished) to close it
```
