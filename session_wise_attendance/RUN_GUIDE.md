# session_wise_attendance — Run Guide

A three-stage funnel that ends with one row per class session: Stage 1 builds the course/batch catalogue, Stage 2 finds each
batch's class ids, Stage 3 pulls the attendance for every class. Full documentation:
[SESSION_WISE_ATTENDANCE.md](SESSION_WISE_ATTENDANCE.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has `api_key`, `organization_id` and `institute_id`.
- `notifications.yaml` (this folder) holds the run-report email settings; check the recipient before relying on it.
- Run the stages **in order** — each one reads the previous stage's output file.
- Dates for Stage 3 are **YYYY-MM-DD**.
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.

## Step by step
1. Activate the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
2. Go to the scripts folder: `cd /home/projectdev/ela_datasets/session_wise_attendance/scripts`
3. **Stage 1** — course/batch catalogue (`output/course_catalog.csv`):
   ```bash
   python3 build_course_catalog.py
   ```
4. **Stage 2** — class ids per batch (`output/class_id_lookup.csv`):
   ```bash
   python3 resolve_class_ids.py
   ```
5. **Stage 3** — session attendance for a date range (`output/session_wise_attendance_data.csv`):
   ```bash
   python3 build_session_attendance.py --start 2026-01-01 --end 2026-08-31
   ```
6. `attendance_crossvalidation.py` is a manual spot-check, not a stage. It currently has a known defect (it sends only the
   `apikey` header and gets HTTP 404), so do not rely on it until that is fixed.

## If it stops (Ctrl+C, a rate-limit wait, a crash)
- Re-run the same command. Stages 2 and 3 skip rows they already finished; `--restart` throws that progress away.
- Stage 1 is the shortest stage; if it stops, just run it again.
- On a 429 (rate limited) the script waits out Edmingle's own cool-down; let it wait.

## Check the result
- Each stage's log in `output/logs/<stage>/` ends with a `[RESULT]` / `[TOTAL]` line and has no `[ERROR]`.
- The three CSVs above have new modification times.

## Run it in tmux (recommended)
```
step 1: tmux new -s session_wise_attendance
        (starts the session -- the session name is the dataset folder name)
step 2: activate the environment, open the directory and run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/session_wise_attendance/scripts
        python3 build_course_catalog.py
        python3 resolve_class_ids.py
        python3 build_session_attendance.py --start 2026-01-01 --end 2026-08-31
Ctrl+B then D                to detach / come out of the session (the script keeps running)
tmux ls                      to see the list of active sessions
tmux attach -t session_wise_attendance      to return to / open the session
exit                         (inside the session, when the run has finished) to close it
```
