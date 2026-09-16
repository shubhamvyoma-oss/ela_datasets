"""
edmingle_config.py

Loads edmingle_config.json, fills in sensible defaults for everything else
so the config file only needs to state what differs from the defaults, and
merges in the shared Edmingle API credentials (api_key, organization_id)
from ../../credentials.yaml -- the single source of truth for those values
across every pipeline in ela_datasets/.

Also sends email notifications, reading recipient/SMTP settings from this
pipeline's own notifications.yaml (channels.email.*) rather than from the
JSON config.
"""

import json
import logging
import smtplib
import sys
from email.mime.text import MIMEText
from pathlib import Path

import yaml

DEFAULTS = {
    "chunk_days": 30,
    "per_page": 200,
    "max_calls_per_minute": 30,
    "request_timeout_seconds": 30,
    "initial_retry_delay_seconds": 2,
    "maximum_retry_delay_seconds": 60,
    "rate_limit_block_seconds": 300,
}

CREDENTIALS_PATH = Path(__file__).resolve().parent.parent.parent / "credentials.yaml"
NOTIFICATIONS_PATH = Path(__file__).resolve().parent / "notifications.yaml"


def load_credentials(credentials_path: Path = CREDENTIALS_PATH) -> dict:
    if not credentials_path.exists():
        sys.exit(f"Shared credentials file not found: {credentials_path}")

    creds = yaml.safe_load(credentials_path.read_text()) or {}
    edmingle_cfg = creds.get("edmingle", {})

    required = ["api_key", "organization_id"]
    missing = [key for key in required if not edmingle_cfg.get(key)]
    if missing:
        sys.exit(
            f"Credentials file {credentials_path} is missing required "
            f"edmingle keys: {', '.join(missing)}"
        )

    return edmingle_cfg


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        sys.exit(
            f"Config file not found: {config_path}\n"
            f"Copy edmingle_config.json.example to {config_path.name} and fill it in."
        )
    config = json.loads(config_path.read_text())

    edmingle_cfg = load_credentials()
    config["api_key"] = edmingle_cfg["api_key"]
    config["organization_id"] = edmingle_cfg["organization_id"]

    for key, default in DEFAULTS.items():
        config.setdefault(key, default)

    return config


def load_notifications(notifications_path: Path = NOTIFICATIONS_PATH) -> dict:
    if not notifications_path.exists():
        sys.exit(f"Notifications file not found: {notifications_path}")
    return yaml.safe_load(notifications_path.read_text()) or {}


def send_mail(cfg: dict, subject: str, body: str, logger: logging.Logger) -> None:
    notifications = load_notifications()
    email_cfg = notifications.get("channels", {}).get("email", {})

    if not email_cfg.get("enabled"):
        logger.warning("Email channel disabled in notifications.yaml; skipping email notification.")
        return

    smtp_cfg = email_cfg.get("smtp", {})
    to_addresses = email_cfg.get("to_addresses") or []

    required = ["host", "port", "username", "password"]
    if not all(smtp_cfg.get(k) for k in required) or not to_addresses:
        logger.warning("SMTP not fully configured in notifications.yaml; skipping email notification.")
        return

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_cfg.get("from_address", smtp_cfg["username"])
    msg["To"] = ", ".join(to_addresses)

    try:
        with smtplib.SMTP(smtp_cfg["host"], int(smtp_cfg["port"]), timeout=30) as server:
            server.starttls()
            server.login(smtp_cfg["username"], smtp_cfg["password"])
            server.sendmail(msg["From"], to_addresses, msg.as_string())
        logger.info(f"Email sent: {subject!r}")
    except Exception as exc:
        logger.error(f"Failed to send email ({subject!r}): {exc}")
