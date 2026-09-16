# Edmingle API Key Generator Package

This package contains only the standalone Edmingle API-key rotation tool. It
does not contain pipeline data-extraction code, student data, state files,
or logs. See `README.md` for the full workflow and configuration details.

## Files

- `edmingle_generate_api_key.py` — entry point; orchestrates login, key
  extraction/validation, the `credentials.yaml` write, and the notification
  email.
- `edmingle_api_key_settings.py` — loads non-secret and secret settings from
  `../credentials.yaml` (shared) and `notifications.yaml` (local).
- `edmingle_credentials_writer.py` — rewrites `edmingle.api_key` in place
  inside `../credentials.yaml` via a targeted regex replace (preserves
  comments/formatting). The only file in this package that touches a file
  outside this folder.
- `edmingle_api_key_email.py` — builds and sends the notification email over
  SMTP/TLS.
- `notifications.yaml` — this folder's own SMTP/recipient configuration.
  Contains real secrets (an app password); not committed to version control.
- `run_generate_and_email_api_key.bat` — Windows double-click launcher for
  the full, real run.
- `test_edmingle_api_key_generator.py`, `test_credentials_writer.py` —
  offline unit tests; make no real network/file/email calls.
- `requirements.txt` — Python dependencies (`requests`, `PyYAML`).

The generated API key is written into `../credentials.yaml` and delivered by
email; it is never printed, logged, or saved anywhere else.
