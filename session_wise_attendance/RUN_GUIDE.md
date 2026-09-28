# session_wise_attendance — Run Guide

Attendance for every individual class session. Edmingle can't hand this over directly, so it's built in 3 stages: (1) get the course/batch list, (2) look up each batch's class id, (3) pull attendance for each class. Full technical detail: [SESSION_WISE_ATTENDANCE.md](SESSION_WISE_ATTENDANCE.md).

## Before you start
- Make sure `credentials.yaml` (shared) has `api_key`, `organization_id`, and `institute_id`.
- `scripts/session_wise_attendance_config.json` holds the speed limit and request timeout, shared by all 3 stages — the defaults are fine.
- Check `notifications.yaml` (this folder) has the right email address for the run report.
- Always run the 3 stages **in order** — each one reads the file the last one made.
- Stage 3 dates are `YYYY-MM-DD`.
- Only run one pipeline at a time.

## Steps
1. Start a background session: `tmux new -s session_wise_attendance`
2. Turn on the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
3. Go to the folder: `cd /home/projectdev/ela_datasets/session_wise_attendance/scripts`
4. Run the 3 stages, one after another:
   ```
   python3 build_course_catalog.py                                            # Stage 1 → output/course_catalog.csv
   python3 resolve_class_ids.py                                               # Stage 2 → output/class_id_lookup.csv
   python3 build_session_attendance.py --start 2026-01-01 --end 2026-08-31    # Stage 3 → output/session_wise_attendance_data.csv
   ```
5. Detach and let it run: press `Ctrl+B`, then `D`.

## How to tell it worked
- Each stage has a log in `output/logs/<stage name>/`. It should end with a `[RESULT]` or `[TOTAL]` line and contain no `[ERROR]`.

## If something goes wrong
- **It stopped partway** — run the same command again. Stages 2 and 3 skip rows they already finished (`--restart` throws that progress away and starts over). Stage 1 is short, so just run it again.
- **Edmingle says "too many requests" (429)** — the script waits out the cool-down by itself. Let it wait, don't interrupt.

## Coming back to check on it
- See it running: `tmux ls`
- Reattach: `tmux attach -t session_wise_attendance`
