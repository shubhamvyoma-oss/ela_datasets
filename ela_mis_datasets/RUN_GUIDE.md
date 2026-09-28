# ela_mis_datasets — Run Guide

Refreshes the student list, then pulls the full course/enrollment history for **every single student**, one at a time. The second part is why this takes so long — around **72 hours** for the full run. Full technical detail: [ELA_MIS_DATASETS.md](ELA_MIS_DATASETS.md).

**Always run this inside tmux** — it will not fit in a normal terminal session.

## Before you start
- Make sure `credentials.yaml` (shared) has a working key.
- `notifications.yaml` (this folder) **must** exist with real email settings — the script won't start without it.
- Make sure there's at least 2 GB of free disk space (the script checks this itself and stops if there isn't).
- Only run one pipeline at a time.
- **Never press Ctrl+C** — it throws away an unfinished student-list refresh. To step away, detach instead: `Ctrl+B` then `D`.
- The monthly key rotation (25th, 09:00 IST) checks if this is running first, so it won't interrupt you.

## Steps
1. Start a background session: `tmux new -s ela_mis_datasets`
2. Turn on the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
3. Go to the folder: `cd /home/projectdev/ela_datasets/ela_mis_datasets/scripts`
4. Run it: `python3 edmingle_student_course_sync.py`
5. Detach and let it run: press `Ctrl+B`, then `D`.

What happens, in order:
1. A few quick checks (disk space, API key, student count) — you get a "started" email.
2. The student list refreshes (~4 minutes) — log line: `Published student master with N unique students`.
3. The long part starts — log line: `Started full course refresh for N students`.
4. Done — log line: `Edmingle sync run completed`.

## How to tell it worked
- `output/edmingle_sync.log` ends with `Edmingle sync run completed`.
- There is **no** `output/SCRIPT_FAILED.txt` file.
- Both output CSVs in `output/` show a recent modified time.

## If something goes wrong
- Just start a tmux session and run the same command again — it picks up where it stopped, it doesn't start the whole 72 hours over.
- Check the failure email and `output/edmingle_sync.log` for the actual reason.

## Coming back to check on it
- See it running: `tmux ls`
- Reattach: `tmux attach -t ela_mis_datasets`
