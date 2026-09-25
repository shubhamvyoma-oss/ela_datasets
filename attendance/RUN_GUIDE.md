# attendance — Run Guide

Pulls daily student attendance from Edmingle (`report_type=55`), one API call per day, and writes a per-batch summary
and a per-session CSV. Full documentation: [ATTENDANCE.md](ATTENDANCE.md).

## Before you start
- `../credentials.yaml` (repo root, i.e. `/home/projectdev/ela_datasets/credentials.yaml`) has a valid Edmingle `api_key` and `organization_id`.
- `notifications.yaml` (this folder) — still has placeholder addresses (`your_email@gmail.com`), so no email arrives until it is filled in.
- `scripts/config.yaml` — check the flags (`exclude_inactive_students` is `false` here).
- Disk: the run keeps one staging file per day, and the summarising step then needs roughly **another 0.5× the staging size** free
  (a 2020 → 2026 range is ~5.6 GB of staging, so ~2.8 GB extra). The script checks this up front and stops if there is not enough.
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.
- A standalone copy of this dataset also exists at `/home/projectdev/attendance_dataset/` (own environment, run with
  `./run_attendance.sh`). Use one or the other — they do not share progress.

## Step by step
1. Activate the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate` — your prompt now starts with `(.venv)`.
2. Go to the scripts folder: `cd /home/projectdev/ela_datasets/attendance/scripts`
3. Run it. Dates are **YYYY-MM-DD**:
   ```bash
   python3 attendance.py --from 2026-08-01 --to 2026-08-31    # a date range
   python3 attendance.py --date 2026-08-15                    # one day
   python3 attendance.py                                      # last 30 days (default_lookback_days)
   python3 attendance.py --dry-run --from 2026-08-01 --to 2026-08-07   # no API calls, checks the flow
   ```
4. Wait for `Pipeline complete.` in the log. It makes one API call per day, so a multi-year range takes many hours — use tmux.
5. If it stops part-way, run **the same command again** — finished days are skipped. `--retry-failed` re-does only failed
   days; `--reset-checkpoint` starts over.

## Check the result
- New files in `output/`: `batch_attendance_summary_<range>_<time>.csv` and `session_wise_attendance_<range>_<time>.csv`.
- `output/logs/pipeline.log` has no `ERROR` / `CRITICAL` lines.

## If something goes wrong
| Symptom | What to do |
|---|---|
| `ModuleNotFoundError: pandas` | You forgot step 1 (activate the environment). |
| "Another instance already running" | A run is active (check `tmux ls`); if not, delete `output/pipeline.lock`. |
| Stops with a 401/403 | The API key is wrong or was just rotated — check `credentials.yaml`. |

## Run it in tmux (recommended for anything longer than a few minutes)
A tmux session keeps the script running if your SSH connection drops or you close the terminal.

```
step 1: tmux new -s attendance
        (starts the session -- the session name is the dataset folder name)
step 2: activate the environment, open the directory and run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/attendance/scripts
        python3 attendance.py --from 2026-08-01 --to 2026-08-31
Ctrl+B then D                to detach / come out of the session (the script keeps running)
tmux ls                      to see the list of active sessions
tmux attach -t attendance      to return to / open the session
exit                         (inside the session, when the run has finished) to close it
```
