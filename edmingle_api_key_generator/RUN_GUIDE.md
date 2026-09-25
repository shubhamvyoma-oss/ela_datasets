# edmingle_api_key_generator — Run Guide

Logs into Edmingle, gets a new API key, saves it into `/home/projectdev/ela_datasets/credentials.yaml`, checks that the new key works, and
emails it. **This changes the key every other pipeline uses.** Full documentation:
[EDMINGLE_API_KEY_GENERATOR.md](EDMINGLE_API_KEY_GENERATOR.md).

## It already runs by itself
It runs automatically on the **25th of every month at 09:00 IST** (cron on the server). You only run it by hand if you need a
new key at another time.

## Before you start
- Rotating **revokes the old key immediately.** The script refuses to rotate (exit code 2, emails a "SKIPPED" notice) while
  any other pipeline is running, so nothing in progress is broken.
- `../credentials.yaml` needs the Edmingle `tutor_login` block; `notifications.yaml` (this folder) needs SMTP settings and at
  least one recipient.
- **A copy of the key also lives in `/home/projectdev/attendance_dataset/credentials.yaml`.** That copy is not updated
  automatically — after every rotation, paste the new key (from the email) into it.

## Step by step
1. Activate the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
2. Go to the scripts folder: `cd /home/projectdev/ela_datasets/edmingle_api_key_generator/scripts`
3. Check the settings first (no network, changes nothing):
   ```bash
   python3 edmingle_generate_api_key.py --check-config
   ```
4. Optional — check that the key currently in `credentials.yaml` still works (one read-only call):
   ```bash
   python3 edmingle_generate_api_key.py --verify-only
   ```
5. Rotate the key:
   ```bash
   python3 edmingle_generate_api_key.py
   ```
6. Wait for "New Edmingle API key generated, saved to credentials.yaml, checked against Edmingle, and emailed successfully."
   Then update the copy in `attendance_dataset` (see above).

## Check the result
- You received the email `[Vyoma Edmingle] New API Key Generated`, and it says the key was verified.
- `python3 edmingle_generate_api_key.py --verify-only` now succeeds.

## If something goes wrong
| Symptom | What to do |
|---|---|
| Exit code 2, "Not rotating: these pipelines are running" | Wait for them to finish, then run again. `--force` overrides (they will fail with invalid credentials). |
| "credentials.yaml WAS updated ... but Edmingle did not accept it" | The old key is already revoked; check the tutor login and `credentials.yaml`, then `--verify-only`. |
| "WAS updated ... but a later step failed" (email) | The key is already changed and works; fix the SMTP settings. **Do not rotate again.** |

This is a short, manual utility, so it has no tmux section.
