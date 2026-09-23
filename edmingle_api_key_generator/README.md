# Edmingle API Key Generator

## What this does

This folder is different from every other pipeline under `ela_datasets/`: it
doesn't extract or transform Edmingle data at all. Its only job is to rotate
the single Edmingle API key that every other pipeline in `ela_datasets/`
depends on.

It does three things, in order, on each run:

1. Logs in to Edmingle with a real tutor username/password to obtain a fresh
   API key (a real network call, not a dry run).
2. Writes that new key into the shared `../../credentials.yaml` (`edmingle.api_key`),
   in place, replacing the previous key.
3. Emails a notification containing the new key to the configured recipients.

**This is the only script in `ela_datasets/` that writes to `credentials.yaml`.**
Every other pipeline in this repo only *reads* `edmingle.api_key` from that
file; none of them ever modify it. That's why this folder is kept separate
and self-contained, and why running it for real is a deliberate, disruptive
action — it changes the credential every other pipeline is currently using.

## Folder layout

This folder is split into two subfolders:

- `scripts/` -- all source code: `edmingle_generate_api_key.py`,
  `edmingle_api_key_settings.py`, `edmingle_credentials_writer.py`,
  `edmingle_api_key_email.py`, `notifications.yaml`, `requirements.txt`,
  `run_generate_and_email_api_key.bat`, and both test files. Run everything
  from inside `scripts/`.
- `output/` -- reserved for file output. This pipeline currently produces
  none (it only writes to the shared `../../credentials.yaml` and sends an
  email), so this folder just holds a short `README.md` explaining that.

Compiled bytecode (`__pycache__`) for every pipeline under `ela_datasets/`
is redirected to a single shared `ela_datasets/.pycache/` directory (via
`sys.pycache_prefix`, set at the top of `edmingle_generate_api_key.py`
before any local import) instead of a separate `__pycache__` folder per
pipeline.

## Edmingle endpoint used

- `POST <tutor_login.login_url>` (from `../../credentials.yaml`, currently
  `.../tutor/login`)
- Sent as **multipart form-data**, not a JSON body: a single `JSONString`
  field whose value is `{"username": ..., "password": ...}` serialized to a
  JSON string. This mirrors the exact shape Edmingle's API expects (matching
  Postman's form-data equivalent), not a standard `application/json` POST.
- Exactly **one** request is ever made per run. There is no retry logic: a
  login failure, a non-2xx status, or an HTTP 429 (rate limit) is all treated
  as an immediate, terminal error — never retried automatically.

## Business rules / validation

`extract_api_key()` in `edmingle_generate_api_key.py` enforces:

- The response JSON must have `code == 200`, and `user.apikey` must be a
  non-empty string. Anything else (bad credentials, missing key, unexpected
  shape) raises `ApiKeyGenerationError` immediately.
- The extracted key must be **16–256 characters with no whitespace**. A key
  outside that range, or containing whitespace, is rejected as malformed
  before it is ever written anywhere.

**Critical rule: the generated key value is never logged, printed, or
persisted anywhere except (a) the single line written into
`credentials.yaml`, and (b) the body of the outgoing notification email.**
This is enforced structurally, not just by convention — read through
`edmingle_generate_api_key.py`, `edmingle_credentials_writer.py`, and
`edmingle_api_key_email.py` and note that no `print()`, `logging`, exception
message, or `sys.stderr` write ever receives the `api_key` variable itself.
Error paths only ever include *non-secret* details (HTTP status codes,
Edmingle's own `message` field truncated to 200 chars, exception type names)
— never the key. The only two places the raw key value flows to are the
`update_shared_api_key(...)` call (file write) and `send_api_key_email(...)`
(email body).

## Workflow (in order)

1. **Validate settings** (`validate_settings()`) — confirms
   `EDMINGLE_LOGIN_URL` uses HTTPS, the request timeout is positive, and an
   email sender + at least one recipient are configured. Fails fast, before
   touching the network, if anything is missing.
2. **Read tutor credentials** — username/password come from
   `../../credentials.yaml` (`edmingle.tutor_login`). If either is left blank
   in that file, the script falls back to an interactive prompt (password
   input is hidden via `getpass`).
3. **Make the one login request** — `generate_api_key()` posts the
   multipart request described above, then `extract_api_key()` validates
   and returns the key.
4. **Write the new key into `../../credentials.yaml`** — via
   `update_shared_api_key()` in `edmingle_credentials_writer.py`. This
   happens **before** the email is sent, deliberately: the credential
   rotation is the operationally important step, and its success must not
   depend on email delivery working.
5. **Email a notification** with the new key value, via
   `send_api_key_email()` (TLS/STARTTLS through the SMTP settings in
   `notifications.yaml`).
6. **Failure handling**: if the `credentials.yaml` write in step 4 succeeds
   but the email in step 5 fails, the script does **not** report a plain
   failure. It prints a warning to stderr making clear that
   `credentials.yaml` **was already updated** and the new key is already
   live for every pipeline — only the notification failed. Any failure
   *before* the write (bad login, malformed key, etc.) is reported as a
   plain "failed safely" error, since nothing was changed.

## Configuration

`edmingle_api_key_settings.py`'s credentials/notifications loading now comes from the shared `../../common.py`. `edmingle_credentials_writer.py` (the one script allowed to *write* to `credentials.yaml`) is untouched -- it does a targeted regex replace on the raw file text, not a full load/dump, specifically to preserve comments/formatting.


- **`../../credentials.yaml`** (shared across all of `ela_datasets/`; this is
  the only script that writes to it, everyone else only reads it):
  - `edmingle.tutor_login.login_url`
  - `edmingle.tutor_login.username`
  - `edmingle.tutor_login.password`
  - `edmingle.api_key` — the field this script rewrites after each
    successful rotation.
- **`notifications.yaml`** (local to this folder only — delivery settings,
  not Edmingle auth):
  - `channels.email.smtp.host` / `port` / `from_address` / `app_password` /
    `timeout_seconds`
  - `channels.email.to_addresses`
  - `channels.slack` / `channels.teams` — present but `enabled: false`
    placeholders reserved for future notification channels; not used today.

Neither file is committed to version control.

## How to run

- `python3 edmingle_generate_api_key.py` — runs the full flow **for real**:
  makes a live Edmingle login call, rewrites the shared
  `../../credentials.yaml`, and sends a real email. Only run this deliberately,
  since it rotates a credential every other pipeline depends on.
- `python3 edmingle_generate_api_key.py --check-config` — validates
  configuration structure only (`validate_settings()`). Makes **no**
  network request, **no** file write, and sends **no** email. Safe to run
  at any time, including on the shared server.
- `run_generate_and_email_api_key.bat` — Windows double-click launcher that
  runs the same script with no arguments (i.e. the full, real flow).

## Tests

- `test_edmingle_api_key_generator.py` (5 tests) — covers the exact
  multipart request shape (`JSONString` field, no `data=` kwarg), API key
  extraction from a successful response, rejection of a bad/failed login,
  and the email's content (full key present, recipients from settings, no
  leftover login password in the body).
- `test_credentials_writer.py` (6 tests, added alongside today's
  `credentials.yaml`/writer changes) — covers `update_shared_api_key()` in
  isolation: it replaces only the `api_key` line, preserves surrounding
  comments/formatting/other fields byte-for-byte, leaves no leftover
  `.tmp` file, and raises `CredentialsUpdateError` cleanly for an empty
  key, a key containing whitespace, a missing credentials file, or a file
  with no matching `api_key:` line.

Run with `python3 -m unittest discover` (or run each file directly) from
inside `scripts/`. Both test suites are fully offline — no real network call,
file write to the real `credentials.yaml`, or email is made by the tests.

## Known limitations / things to watch for

- **Never run this automatically or on a schedule.** Unlike every other
  script in `ela_datasets/`, which only ever *uses* an API key, this script
  *authenticates with a real tutor username and password* and rewrites a
  credential that every other pipeline depends on. It should only be run
  deliberately, by a person, when a key rotation is actually intended.
- **`edmingle_credentials_writer.py`'s replace is regex-based, not a YAML
  round-trip**, specifically so it doesn't reformat or lose comments in
  `../../credentials.yaml`. It matches a line shaped like
  `api_key: "..."` (optionally indented, with an optional trailing
  comment). If `credentials.yaml`'s structure is ever changed — e.g. the
  `api_key` line's quoting or formatting — the writer will find zero or
  more than one match and **refuse to write** (`CredentialsUpdateError`)
  rather than risk corrupting the file. If you ever restructure
  `credentials.yaml`, re-check this regex still matches, and re-run
  `test_credentials_writer.py`.
- The generated key is emailed as plaintext in the message body to every
  address in `notifications.yaml`'s `to_addresses` — keep that recipient
  list tight.
