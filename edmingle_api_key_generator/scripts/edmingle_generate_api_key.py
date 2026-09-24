#!/usr/bin/env python3
"""Generate one Edmingle API key, save it into the shared credentials.yaml,
and email a notification -- without ever printing/logging the key itself."""

from __future__ import annotations

import argparse
import getpass
import json
import os
import sys
from typing import Any

# Shared bytecode cache across every ela_datasets/ pipeline -- must be set before any local import.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

import edmingle_api_key_settings as settings
import requests
from edmingle_api_key_email import EmailDeliveryError, send_api_key_email
from edmingle_credentials_writer import CredentialsUpdateError, update_shared_api_key


class ApiKeyGenerationError(RuntimeError):
    """Edmingle did not return a usable API key."""


def validate_settings() -> None:
    if not settings.EDMINGLE_LOGIN_URL.startswith("https://"):
        raise ValueError("EDMINGLE_LOGIN_URL must use HTTPS")
    if settings.REQUEST_TIMEOUT_SECONDS <= 0:
        raise ValueError("REQUEST_TIMEOUT_SECONDS must be positive")
    if not settings.EMAIL_FROM or not settings.EMAIL_TO:
        raise ValueError("Email sender and recipients are not configured")


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate one Edmingle API key, persist it to credentials.yaml, and email a notification."
    )
    parser.add_argument(
        "--check-config", action="store_true",
        help="Check non-secret settings without contacting Edmingle, writing credentials.yaml, or sending email.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    key_persisted = False
    try:
        validate_settings()
        if args.check_config:
            print("Configuration structure is valid; no network request, file write, or email was sent.")
            return 0
        username, password = read_credentials()
        email_password = settings.EMAIL_APP_PASSWORD
        if not email_password:
            email_password = getpass.getpass("Gmail app password (input hidden): ")
        if not email_password:
            raise ValueError("Gmail app password is required")

        api_key = generate_api_key(username, password)

        # Persist to the shared credentials.yaml FIRST -- every other
        # pipeline reading that file benefits immediately, and this is the
        # operationally important step even if the notification email
        # below happens to fail.
        update_shared_api_key(settings.CREDENTIALS_PATH, api_key)
        key_persisted = True

        send_api_key_email(api_key, username, email_password)
        print(
            "New Edmingle API key generated, saved to credentials.yaml, and emailed "
            "successfully. The key was not printed or saved anywhere else."
        )
        return 0
    except (ApiKeyGenerationError, EmailDeliveryError, CredentialsUpdateError, ValueError) as error:
        if key_persisted:
            print(
                f"credentials.yaml WAS updated with the new key, but a later step failed: {error}",
                file=sys.stderr,
            )
        else:
            print(f"Failed safely: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
