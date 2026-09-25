# course_batch_merge — Run Guide

Merges the course catalogue with all-status batches into one CSV; under a minute. Details: [COURSE_BATCH_MERGE.md](COURSE_BATCH_MERGE.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid key. No config or input needed.
- The script name is **capitalised** (`Course_Batch_Merge.py`); Linux is case-sensitive.
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.

## Run
```bash
source /home/projectdev/ela_datasets/.venv/bin/activate
cd /home/projectdev/ela_datasets/course_batch_merge/scripts
python3 Course_Batch_Merge.py
```
Finished with `SUCCESS! Saved N rows to file.`

## Check
`output/course_batch_merge.csv` was just rewritten and has N rows.

## If it fails
`ERROR occurred during execution!` → read the message under it (usually API key or network). The script still exits normally, so read the output.

## tmux (optional — it finishes in seconds)
```
step 1: tmux new -s course_batch_merge          start the session (name = folder name)
step 2: activate the venv, open the directory, run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/course_batch_merge/scripts
        python3 Course_Batch_Merge.py
Ctrl+B then D                detach (the script keeps running)
tmux ls                      list active sessions
tmux attach -t course_batch_merge      return to the session
```
