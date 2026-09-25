# session_wise_attendance — Run Guide

One row per class session, built in three stages: (1) course/batch catalogue, (2) class ids per batch, (3) attendance per class. Details: [SESSION_WISE_ATTENDANCE.md](SESSION_WISE_ATTENDANCE.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has `api_key`, `organization_id`, `institute_id`.
- Run stages **in order** — each reads the previous stage's file. Stage 3 dates are `YYYY-MM-DD`.
- `notifications.yaml` (this folder) holds run-report email settings; check the recipient.
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.

## Run
```bash
source /home/projectdev/ela_datasets/.venv/bin/activate
cd /home/projectdev/ela_datasets/session_wise_attendance/scripts
python3 build_course_catalog.py                                              # → output/course_catalog.csv
python3 resolve_class_ids.py                                                 # → output/class_id_lookup.csv
python3 build_session_attendance.py --start 2026-01-01 --end 2026-08-31      # → output/session_wise_attendance_data.csv
```
`attendance_crossvalidation.py` is a manual spot-check, not a stage; it currently gets HTTP 404 (sends only the `apikey` header), so don't rely on it.

## Check
Each stage's log in `output/logs/<stage>/` ends with a `[RESULT]`/`[TOTAL]` line and has no `[ERROR]`.

## If it stops
Re-run the same command. Stages 2 and 3 skip finished rows (`--restart` discards progress); Stage 1 is short, just run it again. On a 429 the script waits out Edmingle's cool-down — let it.

## tmux (recommended)
```
step 1: tmux new -s session_wise_attendance          start the session (name = folder name)
step 2: activate the venv, open the directory, run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/session_wise_attendance/scripts
        python3 build_course_catalog.py
        python3 resolve_class_ids.py
        python3 build_session_attendance.py --start 2026-01-01 --end 2026-08-31
Ctrl+B then D                detach (the script keeps running)
tmux ls                      list active sessions
tmux attach -t session_wise_attendance      return to the session
```
