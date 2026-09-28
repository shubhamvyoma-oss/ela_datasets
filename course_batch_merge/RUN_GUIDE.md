# course_batch_merge — Run Guide

Combines the course catalogue with every batch (Active, Completed, and Archived) into one file. Finishes in under a minute. Full technical detail: [COURSE_BATCH_MERGE.md](COURSE_BATCH_MERGE.md).

## Before you start
- Make sure `credentials.yaml` (shared) has a working key.
- Check `scripts/course_batch_merge_config.json` if you ever need to change the request timeout — the default is fine.
- The script's file name has capital letters: `Course_Batch_Merge.py`. Type it exactly.
- Only run one pipeline at a time.

## Steps
1. Turn on the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
2. Go to the folder: `cd /home/projectdev/ela_datasets/course_batch_merge/scripts`
3. Run it: `python3 Course_Batch_Merge.py`
4. Wait for `SUCCESS! Saved N rows to file.`

## How to tell it worked
- `output/course_batch_merge.csv` was just rewritten, with the row count the script printed.

## If something goes wrong
- **`ERROR occurred during execution!`** — read the line right under it, it explains why (usually the API key or the network). Note: the script still exits normally even when this happens, so don't assume no error message means success — check the log text itself.

## Running it in the background (optional — it only takes seconds)
1. `tmux new -s course_batch_merge`
2. `source /home/projectdev/ela_datasets/.venv/bin/activate`
3. `cd /home/projectdev/ela_datasets/course_batch_merge/scripts`
4. `python3 Course_Batch_Merge.py`
5. Detach: press `Ctrl+B`, then `D`. Come back later: `tmux attach -t course_batch_merge`
