# ela_mis_datasets — Run Guide

Phase 1 refreshes the student roster; phase 2 fetches enrollments for **every student**, one call each. Phase 2 takes **~72 hours** (~131,000 students), so **always use tmux**. Details: [ELA_MIS_DATASETS.md](ELA_MIS_DATASETS.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid key; `notifications.yaml` (this folder) **must exist** with SMTP settings.
- `scripts/edmingle_sync_config.json` has all required keys; ≥2 GB free disk (checked at start).
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.
- The API key is rotated automatically on the 25th at 09:00 IST, but only if no pipeline is running, so a run in progress is never broken by it.
- Don't press Ctrl+C: it discards an unfinished roster refresh and stops phase 2. Detach with `Ctrl+B` then `D`.

## Run (inside tmux — see below)
```bash
python3 edmingle_student_course_sync.py
```
1. Startup checks (disk, key, real student count) → `STARTED` email.
2. Phase 1 (~4 min): log says `Published student master with N unique students`.
3. Phase 2: log says `Started full course refresh for N students`.
4. Done when the log says `Edmingle sync run completed`.

## Check
`output/edmingle_sync.log` ends with that line, there is no `output/SCRIPT_FAILED.txt`, and both CSVs in `output/` are freshly modified.

## If it stops
Start a session and re-run the same command; it resumes from its checkpoint (phase 2 does not restart). The failure email and `output/edmingle_sync.log` give the reason.

## tmux
```
step 1: tmux new -s ela_mis_datasets          start the session (name = folder name)
step 2: activate the venv, open the directory, run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/ela_mis_datasets/scripts
        python3 edmingle_student_course_sync.py
Ctrl+B then D                detach (the script keeps running)
tmux ls                      list active sessions
tmux attach -t ela_mis_datasets      return to the session
```
