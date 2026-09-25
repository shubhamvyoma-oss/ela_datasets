# country_wise_data — Run Guide

Country per student, three stages: (1) Edmingle geo-IP, (2) phone dial code, (3) merge. Details: [COUNTRY_WISE_DATA.md](COUNTRY_WISE_DATA.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid key (Stage 1 only).
- Stage 2 needs a fresh `Student-Export*.csv` (Edmingle admin panel) in `input/`.
- `start_date` is in `scripts/ip_driven_country_data_config.json`; `end_date` is automatic (end of today) unless you add it there.
- To **re-pull Stage 1** (it never refreshes a finished pull by itself): delete `output/user_country_list.csv` and `output/user_country_list_checkpoint.json`, then run it — a few minutes.
- Run stages **in order** — Stage 3 needs 1 and 2.
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.

## Run
```bash
source /home/projectdev/ela_datasets/.venv/bin/activate
cd /home/projectdev/ela_datasets/country_wise_data/scripts
python3 ip_driven_country_data.py --config ip_driven_country_data_config.json   # Stage 1 (long, resumable)
python3 dial_code_to_country.py                                                 # Stage 2 (seconds)
python3 merge_country_data.py                                                   # Stage 3
```

## Check
- `output/user_country_list_checkpoint.json` shows the page reached and the `end_date` window. Last seen: page 124, 61,722 rows — **complete for a window ending 19 Aug 2026; today's window has 65,391 users**, so re-pull Stage 1 (see above).
- `output/Student-Export_with_country.csv` and `output/merged_country_data.csv` were rewritten. Merged rows ≠ export rows (it also holds Stage-1-only users).

## If it stops
Re-run the same command; Stage 1 continues from its checkpoint (same date window). If it says the checkpoint has no saved end date, delete the two Stage 1 files as above. Stage 2 "No --input given…" → put a `Student-Export*.csv` in `input/`. Stage 3 "not found" → run Stages 1–2 first.

## tmux (Stage 1 is long)
```
step 1: tmux new -s country_wise_data          start the session (name = folder name)
step 2: activate the venv, open the directory, run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/country_wise_data/scripts
        python3 ip_driven_country_data.py --config ip_driven_country_data_config.json
        python3 dial_code_to_country.py
        python3 merge_country_data.py
Ctrl+B then D                detach (the script keeps running)
tmux ls                      list active sessions
tmux attach -t country_wise_data      return to the session
```
