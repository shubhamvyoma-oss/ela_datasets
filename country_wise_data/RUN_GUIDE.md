# country_wise_data — Run Guide

Works out each student's country three ways and merges them: Stage 1 asks Edmingle for its geo-IP country per user,
Stage 2 derives a country from each phone dial code, Stage 3 joins them. Full documentation:
[COUNTRY_WISE_DATA.md](COUNTRY_WISE_DATA.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid Edmingle key (Stage 1 only).
- **Stage 2 needs a fresh student export:** download `Student-Export*.csv` from the Edmingle admin panel and put it in
  `input/` (this folder). Stage 2 uses the newest matching file.
- `scripts/ip_driven_country_data_config.json` — check the `start_date` / `end_date` window covers what you want.
- Run the stages **in order**: Stage 3 needs the output of both Stage 1 and Stage 2.
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.

## Step by step
1. Activate the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
2. Go to the scripts folder: `cd /home/projectdev/ela_datasets/country_wise_data/scripts`
3. **Stage 1** — geo-IP country per user from the API (the long one; safe to stop and re-run, it resumes):
   ```bash
   python3 ip_driven_country_data.py --config ip_driven_country_data_config.json
   ```
4. **Stage 2** — dial-code country from the export in `../input/` (no API calls, takes seconds):
   ```bash
   python3 dial_code_to_country.py
   ```
5. **Stage 3** — merge the two:
   ```bash
   python3 merge_country_data.py
   ```

## Check the result
- `output/user_country_list.csv` (Stage 1) has rows. Its checkpoint `output/user_country_list_checkpoint.json` shows how far it got —
  at the last check it stood at page 124 with 61,722 rows, i.e. Stage 1 was **partly done, not finished**.
- `output/Student-Export_with_country.csv` (Stage 2) and `output/merged_country_data.csv` (Stage 3) were rewritten.
  The merged file is *not* expected to have the same row count as the export (it also holds users found only by Stage 1).

## If something goes wrong
| Symptom | What to do |
|---|---|
| Stage 1 stops with a permanent error | API key or config problem; fix it and re-run the same command (it continues from its checkpoint). |
| Stage 2: "No --input given and no file matching" | Put a `Student-Export*.csv` in `input/`. |
| Stage 3 says a file is missing | Run Stage 1 and Stage 2 first. |

## Run it in tmux (Stage 1 is long-running)
```
step 1: tmux new -s country_wise_data
        (starts the session -- the session name is the dataset folder name)
step 2: activate the environment, open the directory and run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/country_wise_data/scripts
        python3 ip_driven_country_data.py --config ip_driven_country_data_config.json
        python3 dial_code_to_country.py
        python3 merge_country_data.py
Ctrl+B then D                to detach / come out of the session (the script keeps running)
tmux ls                      to see the list of active sessions
tmux attach -t country_wise_data      to return to / open the session
exit                         (inside the session, when the run has finished) to close it
```
