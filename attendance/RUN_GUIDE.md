# attendance — Run Guide

Daily student attendance (`report_type=55`), one API call per day → per-batch summary + per-session CSV. Details: [ATTENDANCE.md](ATTENDANCE.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid Edmingle key; `scripts/config.yaml` flags are right.
- Free disk ≈ staging size + 0.5× (2020→2026 ≈ 5.6 GB + 2.8 GB). The script checks this and stops if short.
- Notification emails go nowhere until `notifications.yaml` (this folder) has real addresses.
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.
- A standalone copy lives in `/home/projectdev/attendance_dataset/` (own venv, `./run_attendance.sh`); they don't share progress.

## Run (dates `YYYY-MM-DD`)
```bash
source /home/projectdev/ela_datasets/.venv/bin/activate
cd /home/projectdev/ela_datasets/attendance/scripts
python3 attendance.py --from 2026-08-01 --to 2026-08-31   # range
python3 attendance.py --date 2026-08-15                   # one day
python3 attendance.py                                     # last 30 days
python3 attendance.py --dry-run --from 2026-08-01 --to 2026-08-07   # no API calls
```
Finished with `Pipeline complete.` in the log.

## Check
- New `output/batch_attendance_summary_*.csv` and `output/session_wise_attendance_*.csv`; no `ERROR` in `output/logs/pipeline.log`.

## If it stops
Re-run the same command: finished days (files in `output/staging/`) are skipped, failed days are retried. To start over, delete `output/staging/`. After a long internet outage the pull stops after 3 failed days in a row — just re-run.
"Another instance already running" → check `tmux ls`; the lock frees itself when that run ends.

## tmux (use for any long range)
```
step 1: tmux new -s attendance          start the session (name = folder name)
step 2: activate the venv, open the directory, run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/attendance/scripts
        python3 attendance.py --from 2026-08-01 --to 2026-08-31
Ctrl+B then D                detach (the script keeps running)
tmux ls                      list active sessions
tmux attach -t attendance      return to the session
```
