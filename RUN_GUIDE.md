# ela_datasets — Run Guide

This is the index. Each folder below has its own short run guide with the exact steps for that one.

## First time on the server

1. Log in: `ssh projectdev@195.35.6.99`
2. Turn on the Python environment (do this every time, in every new terminal):
   ```
   source /home/projectdev/ela_datasets/.venv/bin/activate
   ```
   Your prompt now starts with `(.venv)`. If you ever see errors about missing `pandas` or similar, you forgot this step.
3. Every pipeline needs a working Edmingle API key in `credentials.yaml` (shared by all of them, kept up to date automatically).

**Only run one pipeline at a time** — they all use the same API key and the same rate limit.

If the `.venv` folder is ever missing, rebuild it:
```
cd /home/projectdev/ela_datasets
curl -sL https://bootstrap.pypa.io/virtualenv.pyz -o /tmp/virtualenv.pyz
python3 /tmp/virtualenv.pyz .venv
source .venv/bin/activate
pip install pandas requests pyyaml phonenumbers pycountry
```

## Running something that takes a long time (tmux)

A few of these take hours. If you close your terminal, a normal command stops — tmux keeps it running.

1. Start a session, named after the folder: `tmux new -s <folder name>`
2. Turn on the environment and run the command as usual.
3. Leave it running and disconnect: press `Ctrl+B`, then `D`.
4. Come back later: `tmux attach -t <folder name>`
5. See what's running: `tmux ls`

## Which pipeline does what

| Folder | What it does | Takes about | Needs tmux? |
|---|---|---|---|
| [attendance](attendance/RUN_GUIDE.md) | Daily attendance per batch/session | minutes to hours, depends on the date range | for long ranges |
| [country_wise_data](country_wise_data/RUN_GUIDE.md) | Each learner's country | 15–20 minutes | yes |
| [course_batch_merge](course_batch_merge/RUN_GUIDE.md) | Courses + batches in one file | under a minute | no |
| [course_catalogue_data](course_catalogue_data/RUN_GUIDE.md) | Raw course catalogue | seconds | no |
| [edmingle_api_key_generator](edmingle_api_key_generator/RUN_GUIDE.md) | Gets a new API key | under a minute | no |
| [ela_mis_datasets](ela_mis_datasets/RUN_GUIDE.md) | Student list + full enrollment history | ~3 days | **always** |
| [enrollments_reports](enrollments_reports/RUN_GUIDE.md) | Enrollments for a date range | minutes to hours, depends on the range | yes |
| [session_wise_attendance](session_wise_attendance/RUN_GUIDE.md) | Attendance per class session | hours | yes |

## A few things worth knowing

- Most of these pick up where they left off if they stop partway — just run the same command again.
- `enrollments_reports` wants dates as `DD-MM-YYYY`. Everything else wants `YYYY-MM-DD`.
- `country_wise_data` and `session_wise_attendance` each run in **stages, in order** — don't skip ahead.
- If the API key ever expires (you'll see 401/403 errors), run `edmingle_api_key_generator` first, then retry.
- Emails: who receives them and the sender login are in `notifications.yaml` (repo root; it holds a password, so it is not on GitHub). What each email says is in `notification_messages.yaml` inside that pipeline's own folder — plain text you can edit.
