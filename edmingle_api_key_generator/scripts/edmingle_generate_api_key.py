#!/usr/bin/env python3
"""Generate one Edmingle API key, save it into the shared credentials.yaml, check that it works,
and email a notification -- without ever printing/logging the key itself.

Exit codes: 0 done, 1 failed, 2 skipped because another pipeline is running (nothing changed).
Scheduled monthly (25th, 09:00 IST) by cron; see ../EDMINGLE_API_KEY_GENERATOR.md.
"""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
import time
from typing import Any

# Shared bytecode cache across every ela_datasets/ pipeline -- must be set before any local import.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

import edmingle_api_key_settings as settings
import requests
from edmingle_api_key_email import EmailDeliveryError, send_api_key_email, send_notice_email
from edmingle_credentials_writer import CredentialsUpdateError, update_shared_api_key
from edmingle_rotation_guard import RotationBlockedError, ensure_no_pipeline_running


class ApiKeyGenerationError(RuntimeError):
    """Edmingle did not return a usable API key."""


class ApiKeyVerificationError(RuntimeError):
    """Edmingle did not accept a key on a real (read-only) API call."""


def validate_settings() -> None:
    if not settings.EDMINGLE_LOGIN_URL.startswith("https://"):
        raise ValueError("EDMINGLE_LOGIN_URL must use HTTPS")
    if settings.REQUEST_TIMEOUT_SECONDS <= 0:
        raise ValueError("REQUEST_TIMEOUT_SECONDS must be positive")
    if not settings.EMAIL_FROM or not settings.EMAIL_TO:
        raise ValueError("Email sender and recipients are not configured")
    if not settings.EDMINGLE_BASE_URL.startswith("https://") or not settings.EDMINGLE_ORGANIZATION_ID:
        raise ValueError("credentials.yaml needs edmingle.base_url (https) and edmingle.organization_id "
                         "to check a new key")


def read_credentials() -> tuple[str, str]:
    username = settings.EDMINGLE_USERNAME.strip()
    password = settings.EDMINGLE_PASSWORD
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
    close_client = session is None
    login_json = json.dumps(
        {"username": username, "password": password}, separators=(",", ":")
    )
    try:
        try:
            response = client.post(
                settings.EDMINGLE_LOGIN_URL,
                files={"JSONString": (None, login_json)},
                headers={
                    "Accept": "application/json",
                    "User-Agent": "vyoma-edmingle-api-key-generator/1.0",
                },
                timeout=settings.REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as error:
            raise ApiKeyGenerationError(
                f"Could not contact Edmingle: {type(error).__name__}"
            ) from error
        try:
            payload = response.json()
        except (ValueError, json.JSONDecodeError) as error:
            raise ApiKeyGenerationError(
                f"Edmingle returned HTTP {response.status_code} with invalid JSON"
            ) from error
        if response.status_code == 429:
            raise ApiKeyGenerationError("Edmingle rate-limited the login request; try again later")
        if not 200 <= response.status_code < 300:
            message = payload.get("message") if isinstance(payload, dict) else None
            safe_message = str(message)[:200] if message else "request failed"
            raise ApiKeyGenerationError(
                f"Edmingle returned HTTP {response.status_code}: {safe_message}"
            )
        return extract_api_key(payload)
    finally:
        if close_client:
            client.close()


def verify_api_key(
    api_key: str,
    session: requests.Session | Any | None = None,
    attempts: int = 3,
    delay_seconds: float = 5.0,
    sleep=time.sleep,
) -> None:
    """One read-only GET (a 1-row batch list) with `api_key`; raises unless Edmingle accepts it.
    Retries a few times in case a brand-new key takes a moment to become active."""
    org = settings.EDMINGLE_ORGANIZATION_ID
    client = session or requests
    reason = "no attempt made"
    for attempt in range(1, attempts + 1):
        try:
            response = client.get(
                f"{settings.EDMINGLE_BASE_URL}/short/masterbatch",
                params={"status": 3, "page": 1, "per_page": 1, "organization_id": org},
                headers={"apikey": api_key, "orgid": org, "ORGID": org, "Accept": "application/json"},
                timeout=settings.REQUEST_TIMEOUT_SECONDS,
            )
            try:
                body = response.json()
            except (ValueError, json.JSONDecodeError):
                body = {}
            code = body.get("code") if isinstance(body, dict) else None
            if response.status_code == 200 and str(code) == "200":
                return
            message = str(body.get("message"))[:80] if isinstance(body, dict) and body.get("message") else "-"
            reason = f"HTTP {response.status_code}, code {code}, message {message}"
        except requests.RequestException as error:
            reason = f"could not contact Edmingle ({type(error).__name__})"
        if attempt < attempts:
            sleep(delay_seconds)
    raise ApiKeyVerificationError(f"Edmingle did not accept the key after {attempts} attempts: {reason}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one Edmingle API key, persist it to credentials.yaml, verify it, and email a notification."
    )
    parser.add_argument(
        "--check-config", action="store_true",
        help="Check non-secret settings without contacting Edmingle, writing credentials.yaml, or sending email.",
    )
    parser.add_argument(
        "--verify-only", action="store_true",
        help="Check that the key currently in credentials.yaml works (one read-only call). Does not rotate anything.",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Rotate even if other pipelines are running; they will fail with invalid credentials until restarted.",
    )
    return parser.parse_args()


def _notify(subject: str, body: str) -> None:
    """Best-effort status email for a skipped/failed run. Uses only the configured app password (a
    scheduled run has no one to prompt) and never raises."""
    if not settings.EMAIL_APP_PASSWORD:
        return
    try:
        send_notice_email(subject, body, settings.EMAIL_APP_PASSWORD)
    except (EmailDeliveryError, ValueError) as error:
        print(f"Could not send the status email: {error}", file=sys.stderr)


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
                verify_api_key(settings.EDMINGLE_CURRENT_API_KEY)
            except ApiKeyVerificationError as error:
                print(f"The key currently in credentials.yaml is NOT accepted: {error}", file=sys.stderr)
                return 1
            print("The key currently in credentials.yaml is accepted by Edmingle.")
            return 0

        # Before any prompt or network call: the login below revokes the old key immediately.
        ensure_no_pipeline_running(force=args.force)

        username, password = read_credentials()
        email_password = settings.EMAIL_APP_PASSWORD
        if not email_password:
            email_password = getpass.getpass("Gmail app password (input hidden): ")
        if not email_password:
            raise ValueError("Gmail app password is required")

        api_key = generate_api_key(username, password)

        # Persist to the shared credentials.yaml FIRST -- the old key is already revoked, so this
        # is the step that keeps every pipeline working even if what follows fails.
        update_shared_api_key(settings.CREDENTIALS_PATH, api_key)
        key_persisted = True

        verify_api_key(api_key)

        send_api_key_email(api_key, username, email_password, verified=True)
        print(
            "New Edmingle API key generated, saved to credentials.yaml, checked against Edmingle, and "
            "emailed successfully. The key was not printed or saved anywhere else."
        )
        return 0
    except RotationBlockedError as error:
        print(str(error), file=sys.stderr)
        _notify("[Vyoma Edmingle] API key rotation SKIPPED", f"{error}\n\nNothing was changed.")
        return 2
    except ApiKeyVerificationError as error:
        message = (f"credentials.yaml WAS updated with the new key, but Edmingle did not accept it: {error}")
        print(message, file=sys.stderr)
        _notify("[Vyoma Edmingle] API key rotation FAILED - new key not accepted",
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
                _notify("[Vyoma Edmingle] API key rotation FAILED", f"{error}\n\nNothing was changed.")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
