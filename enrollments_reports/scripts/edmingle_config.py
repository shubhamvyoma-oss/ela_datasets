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
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import common

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

    edmingle_cfg = common.load_credentials(credentials_path)

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
    return common.load_notifications(notifications_path.parent)


def send_mail(cfg: dict, subject: str, body: str, logger: logging.Logger) -> None:
    notifications = load_notifications()
    common.send_mail(notifications, subject, body, logger)
