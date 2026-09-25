# course_batch_merge — Run Guide

Merges the course catalogue with the batch list (all statuses) into one CSV. Takes well under a minute. Full
documentation: [COURSE_BATCH_MERGE.md](COURSE_BATCH_MERGE.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid Edmingle key. No config file, input folder or notification setup is needed.
- **The script name is capitalised: `Course_Batch_Merge.py`.** Linux is case-sensitive, so `course_batch_merge.py` fails with
  "No such file or directory".
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.

## Step by step
1. Activate the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
2. Go to the scripts folder: `cd /home/projectdev/ela_datasets/course_batch_merge/scripts`
3. Run it (no arguments):
   ```bash
   python3 Course_Batch_Merge.py
   ```
4. Wait for `SUCCESS! Saved N rows to file.`

## Check the result
- `output/course_batch_merge.csv` was just rewritten, and `N` matches the row count the script printed.
- The row count changes with live data (835 active, 40 archived, 12 completed batches at the last check).

## If something goes wrong
| Symptom | What to do |
|---|---|
| `ModuleNotFoundError: pandas` | Activate the environment (step 1). |
| `ERROR occurred during execution!` | Read the message under it; usually the API key or a network problem. The script prints the error and still exits normally, so read the output. |

## Run it in tmux
It finishes in seconds, so tmux is optional here — the format is the same as for the long-running datasets.

```
step 1: tmux new -s course_batch_merge
        (starts the session -- the session name is the dataset folder name)
step 2: activate the environment, open the directory and run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/course_batch_merge/scripts
        python3 Course_Batch_Merge.py
Ctrl+B then D                to detach / come out of the session (the script keeps running)
tmux ls                      to see the list of active sessions
tmux attach -t course_batch_merge      to return to / open the session
exit                         (inside the session, when the run has finished) to close it
```
