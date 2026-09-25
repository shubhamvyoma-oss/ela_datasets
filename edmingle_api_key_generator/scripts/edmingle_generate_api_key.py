#!/usr/bin/env python3
"""Generate one Edmingle API key, save it into the shared credentials.yaml, check that it works,
and email a notification -- without ever printing/logging the key itself.

Exit codes: 0 done, 1 failed, 2 skipped because another pipeline is running (nothing changed).
Scheduled monthly (25th, 09:00 IST) by cron; see ../EDMINGLE_API_KEY_GENERATOR.md.
Settings: the tutor login and base URL come from ../../credentials.yaml, the SMTP settings and
recipients from ../notifications.yaml (this pipeline's own folder).
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

# Shared bytecode cache across every ela_datasets/ pipeline -- must be set before any local import.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import common
import requests
from edmingle_credentials_writer import CredentialsUpdateError, update_shared_api_key
from edmingle_rotation_guard import RotationBlockedError, ensure_no_pipeline_running

CREDENTIALS_PATH = str(Path(__file__).resolve().parents[2] / "credentials.yaml")
REQUEST_TIMEOUT_SECONDS = 30

_edmingle = common.load_credentials(CREDENTIALS_PATH)
_tutor_login = _edmingle.get("tutor_login", {})
NOTIFICATIONS = common.load_notifications("edmingle_api_key_generator")
_email_channel = (NOTIFICATIONS.get("channels") or {}).get("email") or {}
_smtp = _email_channel.get("smtp") or {}

EDMINGLE_LOGIN_URL = _tutor_login.get("login_url", "")
EDMINGLE_USERNAME = _tutor_login.get("username", "")
EDMINGLE_PASSWORD = _tutor_login.get("password", "")
# Used to check that a freshly generated key actually works, and by --verify-only.
EDMINGLE_BASE_URL = str(_edmingle.get("base_url", "")).rstrip("/")
EDMINGLE_ORGANIZATION_ID = str(_edmingle.get("organization_id", ""))
EDMINGLE_CURRENT_API_KEY = str(_edmingle.get("api_key", ""))
EMAIL_TO = tuple(_email_channel.get("to_addresses", []))


class ApiKeyGenerationError(RuntimeError):
    """Edmingle did not return a usable API key."""


class ApiKeyVerificationError(RuntimeError):
    """Edmingle did not accept a key on a real (read-only) API call."""


class EmailDeliveryError(RuntimeError):
    """The generated API key could not be delivered."""


def validate_settings() -> None:
    if not EDMINGLE_LOGIN_URL.startswith("https://"):
        raise ValueError("EDMINGLE_LOGIN_URL must use HTTPS")
    if not EDMINGLE_BASE_URL.startswith("https://") or not EDMINGLE_ORGANIZATION_ID:
        raise ValueError("credentials.yaml needs edmingle.base_url (https) and edmingle.organization_id "
                         "to check a new key")
    if not (_smtp.get("from_address") and EMAIL_TO and _smtp.get("host") and _smtp.get("app_password")):
        raise ValueError("notifications.yaml needs an SMTP host, sender, app password and at least one recipient")


def read_credentials() -> tuple[str, str]:
    username = EDMINGLE_USERNAME.strip()
    password = EDMINGLE_PASSWORD
    if not username:
        username = input("Edmingle username: ").strip()
    if not password:
        password = getpass.getpass("Edmingle password (input hidden): ")
    if not username or not password:
        raise ValueError("Edmingle username and password are required")
    return username, password


def extract_api_key(payload: Any) -> str:
    if not isinstance(payload, dict):
        raise ApiKeyGenerationError("Edmingle returned an invalid JSON structure")
    user = payload.get("user")
    api_key = user.get("apikey") if isinstance(user, dict) else None
    if payload.get("code") != 200 or not isinstance(api_key, str) or not api_key.strip():
        message = payload.get("message")
        safe_message = str(message)[:200] if message else "login was not successful"
        raise ApiKeyGenerationError(f"Edmingle rejected the request: {safe_message}")
    api_key = api_key.strip()
    if len(api_key) < 16 or len(api_key) > 256 or any(character.isspace() for character in api_key):
        raise ApiKeyGenerationError("Edmingle returned an API key with an unexpected format")
    return api_key


def generate_api_key(
    username: str,
    password: str,
    session: requests.Session | Any | None = None,
) -> str:
    """Make exactly one login request using Postman's form-data equivalent."""
    client = session or requests.Session()
    login_json = json.dumps({"username": username, "password": password}, separators=(",", ":"))
    try:
        try:
            response = client.post(
                EDMINGLE_LOGIN_URL,
                files={"JSONString": (None, login_json)},
                headers={"Accept": "application/json", "User-Agent": "vyoma-edmingle-api-key-generator/1.0"},
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as error:
            raise ApiKeyGenerationError(f"Could not contact Edmingle: {type(error).__name__}") from error
        try:
            payload = response.json()
        except (ValueError, json.JSONDecodeError) as error:
            raise ApiKeyGenerationError(f"Edmingle returned HTTP {response.status_code} with invalid JSON") from error
        if response.status_code == 429:
            raise ApiKeyGenerationError("Edmingle rate-limited the login request; try again later")
        if not 200 <= response.status_code < 300:
            message = payload.get("message") if isinstance(payload, dict) else None
            safe_message = str(message)[:200] if message else "request failed"
            raise ApiKeyGenerationError(f"Edmingle returned HTTP {response.status_code}: {safe_message}")
        return extract_api_key(payload)
    finally:
        if session is None:
            client.close()


def verify_api_key(api_key: str, session: requests.Session | Any | None = None, attempts: int = 3) -> None:
    """One read-only GET (a 1-row batch list) with `api_key`; raises unless Edmingle accepts it. Retries a
    few times in case a brand-new key takes a moment to become active (so 400 is retried too)."""
    org = EDMINGLE_ORGANIZATION_ID
    try:
        common.get_json(
            f"{EDMINGLE_BASE_URL}/short/masterbatch",
            headers={**common.auth_headers(api_key, org), "Accept": "application/json"},
            params={"status": 3, "page": 1, "per_page": 1, "organization_id": org},
            session=session, timeout=REQUEST_TIMEOUT_SECONDS, attempts=attempts, delay=5, max_delay=5, block_seconds=5, permanent=(),
            validate=lambda data: isinstance(data, dict) and str(data.get("code")) == "200", label="key check",
            logger=_QUIET,
        )
    except common.RetriesExhausted as error:
        raise ApiKeyVerificationError(f"Edmingle did not accept the key -- {error}") from error


class _QuietLogger:
    """get_json's retry warnings would only add noise to a cron log; the final error says what happened."""

    warning = error = info = staticmethod(lambda *args, **kwargs: None)


_QUIET = _QuietLogger()


def build_api_key_email(
    api_key: str,
    username: str,
    generated_at: datetime | None = None,
    verified: bool = False,
) -> tuple[str, str]:
    timestamp = (generated_at or datetime.now().astimezone()).astimezone()
    lines = [
        "A new Edmingle API key was generated successfully.",
        "",
        f"Username     : {username}",
        f"Generated at : {timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"API key      : {api_key}",
    ]
    if verified:
        lines.append("Verified     : the new key was accepted by Edmingle right after it was saved")
    return "[Vyoma Edmingle] New API Key Generated", "\n".join([*lines, ""])


def send_api_key_email(api_key: str, username: str, verified: bool = False) -> None:
    """Send the full key once through the shared mailer; never print or persist it."""
    subject, body = build_api_key_email(api_key, username, verified=verified)
    if not common.send_mail(NOTIFICATIONS, subject, body):
        raise EmailDeliveryError("Email delivery failed (see the warning above)")


def send_notice_email(subject: str, body: str) -> None:
    """Status email for a skipped or failed rotation (best-effort, never raises). Must never contain the key."""
    common.send_mail(NOTIFICATIONS, subject, body)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one Edmingle API key, persist it to credentials.yaml, verify it, and email a notification."
    )
    parser.add_argument("--check-config", action="store_true",
                        help="Check non-secret settings without contacting Edmingle, writing credentials.yaml, or sending email.")
    parser.add_argument("--verify-only", action="store_true",
                        help="Check that the key currently in credentials.yaml works (one read-only call). Does not rotate anything.")
    parser.add_argument("--force", action="store_true",
                        help="Rotate even if other pipelines are running; they will fail with invalid credentials until restarted.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    key_persisted = False
    try:
        validate_settings()
        if args.check_config:
            print("Configuration structure is valid; no network request, file write, or email was sent.")
            return 0
        if args.verify_only:
            try:
                verify_api_key(EDMINGLE_CURRENT_API_KEY)
            except ApiKeyVerificationError as error:
                print(f"The key currently in credentials.yaml is NOT accepted: {error}", file=sys.stderr)
                return 1
            print("The key currently in credentials.yaml is accepted by Edmingle.")
            return 0

        # Before any prompt or network call: the login below revokes the old key immediately.
        ensure_no_pipeline_running(force=args.force)

        username, password = read_credentials()
        api_key = generate_api_key(username, password)

        # Persist to the shared credentials.yaml FIRST -- the old key is already revoked, so this
        # is the step that keeps every pipeline working even if what follows fails.
        update_shared_api_key(CREDENTIALS_PATH, api_key)
        key_persisted = True

        verify_api_key(api_key)

        send_api_key_email(api_key, username, verified=True)
        print(
            "New Edmingle API key generated, saved to credentials.yaml, checked against Edmingle, and "
            "emailed successfully. The key was not printed or saved anywhere else."
        )
        return 0
    except RotationBlockedError as error:
        print(str(error), file=sys.stderr)
        send_notice_email("[Vyoma Edmingle] API key rotation SKIPPED", f"{error}\n\nNothing was changed.")
        return 2
    except ApiKeyVerificationError as error:
        message = f"credentials.yaml WAS updated with the new key, but Edmingle did not accept it: {error}"
        print(message, file=sys.stderr)
        send_notice_email("[Vyoma Edmingle] API key rotation FAILED - new key not accepted",
                          f"{message}\n\nThe old key is already revoked, so pipelines will fail until this is fixed. "
                          f"The key was NOT emailed. Check credentials.yaml on the server and the Edmingle tutor login.")
        return 1
    except (ApiKeyGenerationError, EmailDeliveryError, CredentialsUpdateError, ValueError) as error:
        if key_persisted:
            print(
                f"credentials.yaml WAS updated with the new key (and it was accepted by Edmingle), "
                f"but a later step failed: {error}",
                file=sys.stderr,
            )
        else:
            print(f"Failed safely: {error}", file=sys.stderr)
            if not (args.check_config or args.verify_only):
                send_notice_email("[Vyoma Edmingle] API key rotation FAILED", f"{error}\n\nNothing was changed.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
