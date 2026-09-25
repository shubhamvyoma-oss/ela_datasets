# ela_datasets — Run Guide

Index of how to run each pipeline on the VPS. Each dataset folder has its own step-by-step `RUN_GUIDE.md`; deep technical detail is in its `<PIPELINE>.md`.

## Environment (read first)

The VPS's system Python has no `pandas`/`phonenumbers`/`pycountry`, and there is no `python3-venv` or sudo. A working virtualenv already exists at `/home/projectdev/ela_datasets/.venv` (built with the `virtualenv.pyz` zipapp). Activate it once per shell (your prompt gets a `(.venv)` prefix; `deactivate` leaves it):

```bash
source /home/projectdev/ela_datasets/.venv/bin/activate
```

**Recreate it** if `.venv/` is ever lost:
```bash
cd /home/projectdev/ela_datasets
curl -sL https://bootstrap.pypa.io/virtualenv.pyz -o /tmp/virtualenv.pyz
python3 /tmp/virtualenv.pyz .venv
source .venv/bin/activate
pip install pandas requests pyyaml phonenumbers pycountry
```

**Docker alternative:** `docker build -t ela_datasets -f docker/Dockerfile .`, then `docker run --rm -it -v /home/projectdev/ela_datasets:/app ela_datasets bash`.

Every pipeline needs a valid `edmingle.api_key`/`organization_id` in the shared `credentials.yaml` (rotated monthly by the key generator). Run **one pipeline at a time** — they share one API key and rate limit.

## tmux (for anything that runs longer than a few minutes)

The tmux session name is always the dataset folder name.

```
step 1: tmux new -s <folder>          start the session
step 2: source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/<folder>/scripts
        python3 <script> <args>
Ctrl+B then D                detach (the script keeps running)
tmux ls                      list active sessions
tmux attach -t <folder>      return to the session
```

## Datasets

| Folder | Command (from `<folder>/scripts`) | tmux? | Guide |
|---|---|---|---|
| `attendance` | `python3 attendance.py --from YYYY-MM-DD --to YYYY-MM-DD` | yes (hours for long ranges) | [guide](attendance/RUN_GUIDE.md) |
| `country_wise_data` | `python3 ip_driven_country_data.py --config ip_driven_country_data_config.json`, then `dial_code_to_country.py`, then `merge_country_data.py` | yes (Stage 1) | [guide](country_wise_data/RUN_GUIDE.md) |
| `course_batch_merge` | `python3 Course_Batch_Merge.py` (capitalised) | optional | [guide](course_batch_merge/RUN_GUIDE.md) |
| `course_catalogue_data` | `python3 course_catalogue_data.py` | no | [guide](course_catalogue_data/RUN_GUIDE.md) |
| `edmingle_api_key_generator` | `python3 edmingle_generate_api_key.py` (also runs itself on the 25th, 09:00 IST) | no | [guide](edmingle_api_key_generator/RUN_GUIDE.md) |
| `ela_mis_datasets` | `python3 edmingle_student_course_sync.py` (~72 h) | **always** | [guide](ela_mis_datasets/RUN_GUIDE.md) |
| `enrollments_reports` | `python3 edmingle_export.py --start-date DD-MM-YYYY --end-date DD-MM-YYYY` | yes | [guide](enrollments_reports/RUN_GUIDE.md) |
| `session_wise_attendance` | `python3 build_course_catalog.py`, then `resolve_class_ids.py`, then `build_session_attendance.py --start YYYY-MM-DD --end YYYY-MM-DD` | yes | [guide](session_wise_attendance/RUN_GUIDE.md) |

Notes:
- `enrollments_reports` takes **`DD-MM-YYYY`** dates (every other dataset uses `YYYY-MM-DD`) and has a known `send_mail` defect that crashes the next run's "started" email — fix it first (see its guide).
- `country_wise_data` Stage 2 needs a fresh `Student-Export*.csv` in `input/`; run the stages in order.
- `session_wise_attendance` stages must run in order; `attendance_crossvalidation.py` is a manual spot-check that currently gets HTTP 404.
- `edmingle_api_key_generator` refuses to rotate while any pipeline is running (exit code 2); rotating revokes the old key at once. Flags: `--check-config`, `--verify-only`, `--force`.
- After every rotation, update the key copy in `/home/projectdev/attendance_dataset/credentials.yaml` (the standalone attendance copy).
- Long runs resume: re-run the same command after a crash and it continues from its checkpoint.
