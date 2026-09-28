# attendance — Run Guide

Pulls daily attendance from Edmingle (one call per day) and saves two files: one summary per batch, one per session. Full technical detail: [ATTENDANCE.md](ATTENDANCE.md).

## Before you start
- Make sure `credentials.yaml` (shared) has a working API key.
- Check `scripts/attendance_config.yaml` looks right — it holds the tuning settings.
- Make sure `notifications.yaml` (this folder) has a real email address, or you won't get alerts.
- Only run one pipeline at a time.
- There's a separate standalone copy of this pipeline at `/home/projectdev/attendance_dataset/` — it has its own environment and doesn't share progress with this one.

## Steps
1. Turn on the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
2. Go to the folder: `cd /home/projectdev/ela_datasets/attendance/scripts`
3. Run it, picking one:
   ```
   python3 attendance.py --from 2026-08-01 --to 2026-08-31   # a date range
   python3 attendance.py --date 2026-08-15                   # just one day
   python3 attendance.py                                     # last 30 days, if you give no dates
   python3 attendance.py --dry-run --from 2026-08-01 --to 2026-08-07   # test run, no real API calls
   ```
4. Wait for `Pipeline complete.` in the log.

## How to tell it worked
- New files appear: `output/batch_attendance_summary_*.csv` and `output/session_wise_attendance_*.csv`.
- `output/logs/pipeline.log` has no `ERROR` in it.

## If something goes wrong
- **It stopped partway** — just run the same command again. Days it already finished are skipped; only the rest are retried.
- **Want to start completely fresh?** Delete the `output/staging/` folder first.
- **"Another instance already running"** — check `tmux ls` to see if it's already going somewhere else. It clears itself once that run finishes.
- **Long internet outage** — the pull stops after 3 failed days in a row. Just run it again once the connection is back.

## Running it in the background (for anything longer than a few minutes)
1. `tmux new -s attendance`
2. `source /home/projectdev/ela_datasets/.venv/bin/activate`
3. `cd /home/projectdev/ela_datasets/attendance/scripts`
4. Run the command from Step 3 above.
5. Detach and leave it running: press `Ctrl+B`, then `D`.
6. Come back later: `tmux attach -t attendance`
