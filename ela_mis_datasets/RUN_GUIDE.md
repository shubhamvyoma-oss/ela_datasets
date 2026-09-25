# ela_mis_datasets — Run Guide

Two phases in one run: **(1)** it refreshes the student roster (`edmingle_students.csv`), **then (2)** it fetches the class
enrollments for **every student**, one API call each (`edmingle_course_enrollments.csv`). Phase 1 takes a few minutes;
phase 2 takes about **72 hours** for ~131,000 students, so **always run it in tmux**. Full documentation:
[ELA_MIS_DATASETS.md](ELA_MIS_DATASETS.md).

## Before you start
- `/home/projectdev/ela_datasets/credentials.yaml` has a valid Edmingle key.
- `notifications.yaml` (this folder) **must exist** with SMTP settings — the script refuses to start without it.
- `scripts/edmingle_sync_config.json` has all its required keys.
- At least **2 GB** of free disk (the script checks and stops if not).
- Run **one pipeline at a time**: they all share one Edmingle API key and one rate limit, and the server has little spare memory.
- The API key is rotated automatically on the 25th at 09:00 IST, but only if no pipeline is running, so a run in progress is never broken by it.
- **Do not press Ctrl+C in the window.** Detach with `Ctrl+B` then `D` instead (see tmux below). Ctrl+C in phase 1 discards
  the roster refresh (nothing is saved until its last page); in phase 2 it stops the run (it resumes from its checkpoint).

## Step by step
1. Start a tmux session named after this folder: `tmux new -s ela_mis_datasets`
2. Inside it: `source /home/projectdev/ela_datasets/.venv/bin/activate`
3. `cd /home/projectdev/ela_datasets/ela_mis_datasets/scripts`
4. Run it:
   ```bash
   python3 edmingle_student_course_sync.py
   ```
5. Startup checks run (disk, API key, and the real student count from the API), then you get a `STARTED` email.
6. **Phase 1 — roster:** it re-reads the last few roster pages plus everything new (about 12 pages, ~4 minutes; each page
   takes ~19 seconds) and then saves the roster. The log says `Published student master with N unique students`.
7. **Phase 2 — enrollments:** the log says `Started full course refresh for N students`, then one call per student.
8. Detach and leave it: `Ctrl+B` then `D`. Come back with `tmux attach -t ela_mis_datasets`.
9. It is finished when the log says `Edmingle sync run completed`.

## If it stops or crashes
- Re-run the same command (steps 1–4). It resumes from its checkpoint, student by student — phase 2 does **not** start over.
- If it fails you get a failure email;  `output/edmingle_sync.log` has the reason.

## Check the result
- `output/edmingle_sync.log` ends with `Edmingle sync run completed`; there is no `output/SCRIPT_FAILED.txt`.
- `output/edmingle_students.csv` and `output/edmingle_course_enrollments.csv` have new modification times.

## Run it in tmux (session name = folder name)
```
step 1: tmux new -s ela_mis_datasets
        (starts the session -- the session name is the dataset folder name)
step 2: activate the environment, open the directory and run the script
        source /home/projectdev/ela_datasets/.venv/bin/activate
        cd /home/projectdev/ela_datasets/ela_mis_datasets/scripts
        python3 edmingle_student_course_sync.py
Ctrl+B then D                to detach / come out of the session (the script keeps running)
tmux ls                      to see the list of active sessions
tmux attach -t ela_mis_datasets      to return to / open the session
exit                         (inside the session, when the run has finished) to close it
```
