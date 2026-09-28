# enrollments_reports — Run Guide

Downloads individual enrollment records for any date range you choose, in 30-day pieces (Edmingle rejects one huge request), then joins them into one file. Long ranges can take hours. Full technical detail: [ENROLLMENTS_REPORTS.md](ENROLLMENTS_REPORTS.md).

## Before you start
- Make sure `credentials.yaml` (shared) has a working key. the central `notifications.yaml` (repo root) is optional (no email is sent if it is missing).
- `scripts/enrollments_reports_config.json` holds chunk size, speed, and retry settings — the defaults are fine.
- **Dates here are `DD-MM-YYYY`** — every other pipeline uses `YYYY-MM-DD`, so watch out.
- The result is **always saved as `output/edmingle_enrollment_report.csv`** and gets **overwritten** every time a run finishes. If you want to keep an older run's file, copy or rename it first.
- Only run one pipeline at a time.

## Steps
1. Start a background session: `tmux new -s enrollments_reports`
2. Turn on the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
3. Go to the folder: `cd /home/projectdev/ela_datasets/enrollments_reports/scripts`
4. Run it with your dates: `python3 edmingle_export.py --start-date 01-09-2026 --end-date 30-09-2026`
5. Detach and let it run: press `Ctrl+B`, then `D`.

While it runs you'll see lines like `[chunk 1/2 p1] wrote 200 rows …`. It's finished when you see `Done. Wrote N rows total`.

## How to tell it worked
- The log ends with `Done. Wrote N rows total`.
- `output/edmingle_enrollment_report.csv` was just replaced.
- The temporary `output/edmingle_enrollment_report.chunks/` folder is gone.

## If something goes wrong
- **It stopped partway** — run the exact same command again. It skips the 30-day pieces it already finished (at most one piece gets redone).
- Running a range that already finished will just download it again from scratch.

## Coming back to check on it
- See it running: `tmux ls`
- Reattach: `tmux attach -t enrollments_reports`
