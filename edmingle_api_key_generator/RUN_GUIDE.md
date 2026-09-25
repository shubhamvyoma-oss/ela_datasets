# edmingle_api_key_generator — Run Guide

Logs into Edmingle, gets a new API key, saves it to `/home/projectdev/ela_datasets/credentials.yaml`, verifies it and emails it. **This changes the key every pipeline uses.** Details: [EDMINGLE_API_KEY_GENERATOR.md](EDMINGLE_API_KEY_GENERATOR.md).

Runs automatically on the **25th at 09:00 IST** (cron). Run by hand only when you need a key at another time.

## Before you start
- Rotating revokes the old key at once. The script refuses (exit 2, "SKIPPED" email) while any pipeline is running.
- `credentials.yaml` needs the `tutor_login` block; `notifications.yaml` (this folder) needs SMTP settings and a recipient.
- After each rotation, paste the new key into `/home/projectdev/attendance_dataset/credentials.yaml` (not updated automatically).

## Run
```bash
source /home/projectdev/ela_datasets/.venv/bin/activate
cd /home/projectdev/ela_datasets/edmingle_api_key_generator/scripts
python3 edmingle_generate_api_key.py --check-config   # settings only, no network
python3 edmingle_generate_api_key.py --verify-only    # does the current key still work?
python3 edmingle_generate_api_key.py                  # rotate
```

## Check
- Email `[Vyoma Edmingle] New API Key Generated` says the key was verified; `--verify-only` succeeds.

## If it fails
- Exit 2, "Not rotating: these pipelines are running" → wait, then re-run (`--force` overrides; running pipelines then fail).
- "WAS updated … Edmingle did not accept it" → old key is already revoked; check the tutor login, then `--verify-only`.
- "WAS updated … a later step failed" → key is changed and works; fix SMTP. **Do not rotate again.**

No tmux section: short manual utility.
