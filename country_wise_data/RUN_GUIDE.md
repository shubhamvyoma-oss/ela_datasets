# country_wise_data — Run Guide

Country per student, two stages: (1) Edmingle geo-IP pull, (2) phone dial code + join. Details: [COUNTRY_WISE_DATA.md](COUNTRY_WISE_DATA.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid key (Stage 1 only).
- Stage 2 needs a fresh `Student-Export*.csv` (Edmingle admin panel) in `input/`.
- `start_date` is in `scripts/ip_driven_country_data_config.json`; `end_date` is automatic (end of today) unless you add it there.
- Run the stages **in order** — Stage 2 needs Stage 1's file. Every Stage 1 run re-pulls everything and replaces the file (about 5 minutes).
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.

## Run
```bash
source /home/projectdev/ela_datasets/.venv/bin/activate
cd /home/projectdev/ela_datasets/country_wise_data/scripts
python3 ip_driven_country_data.py --config ip_driven_country_data_config.json   # Stage 1 (~5 min)
python3 merge_country_data.py                                                   # Stage 2 (seconds)
```

## Check
- `output/user_country_list.csv` (Stage 1) was just rewritten. Last file on disk: 61,722 rows for a window ending 19 Aug 2026 — today's window has 65,391 users, so run Stage 1 again.
- `output/merged_country_data.csv` (Stage 2) was rewritten; the last lines printed give the match counts. Merged rows ≠ Stage 1 rows (it has one row per student in the export).

## If it stops
Stage 1: nothing was replaced — run it again. Stage 2 "No --input given…" → put a `Student-Export*.csv` in `input/`; "Column '…' not found" → check the export's first two lines and `--dial-code-column`.

## tmux (Stage 1 can be slow after a rate limit)
```
step 1: tmux new -s country_wise_data          start the session (name = folder name)
step 2: activate the venv, open the directory, run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/country_wise_data/scripts
        python3 ip_driven_country_data.py --config ip_driven_country_data_config.json
        python3 merge_country_data.py
Ctrl+B then D                detach (the script keeps running)
tmux ls                      list active sessions
tmux attach -t country_wise_data      return to the session
```
