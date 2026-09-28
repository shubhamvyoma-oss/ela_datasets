# edmingle_api_key_generator — Run Guide

Logs into Edmingle, gets a brand new API key, saves it into the shared `credentials.yaml`, checks it works, and emails you. **Every other pipeline uses this same key**, so rotating it affects all of them. Full technical detail: [EDMINGLE_API_KEY_GENERATOR.md](EDMINGLE_API_KEY_GENERATOR.md).

This already runs by itself, automatically, on the **25th of each month at 09:00 IST**. You only need to run it by hand if you need a new key at some other time (for example, the current one stopped working).

## Before you start
- The old key stops working the moment you rotate — there's no overlap.
- It will **refuse to run** (and email you that it skipped) if any other pipeline is currently running. Check `tmux ls` first if you're not sure.
- `credentials.yaml` needs the `tutor_login` details filled in; `notifications.yaml` (this folder) needs a working email address.
- After rotating, you also need to manually copy the new key into `/home/projectdev/attendance_dataset/credentials.yaml` — that's a separate standalone copy that isn't updated automatically.

## Steps
1. Turn on the environment: `source /home/projectdev/ela_datasets/.venv/bin/activate`
2. Go to the folder: `cd /home/projectdev/ela_datasets/edmingle_api_key_generator/scripts`
3. (Optional) Check your settings are right, without changing anything: `python3 edmingle_generate_api_key.py --check-config`
4. (Optional) Check the current key still works: `python3 edmingle_generate_api_key.py --verify-only`
5. Actually rotate the key: `python3 edmingle_generate_api_key.py`

## How to tell it worked
- You get an email titled `[Vyoma Edmingle] New API Key Generated`, confirming the new key was checked and works.

## If something goes wrong
- **"Not rotating: these pipelines are running" (exit code 2)** — wait for them to finish, then try again. (`--force` will rotate anyway, but any pipeline still running will then fail.)
- **"WAS updated … Edmingle did not accept it"** — the old key is already gone. Check the tutor login details, then run `--verify-only`.
- **"WAS updated … a later step failed"** — the key itself is fine and already changed; something else (usually email) failed. **Do not rotate again** — just fix the email settings.

No need for tmux here — it's a short, manual task.
