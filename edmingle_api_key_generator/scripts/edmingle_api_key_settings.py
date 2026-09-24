"""User-editable settings for the standalone Edmingle API-key generator.

Credentials come from two files, matching the ela_datasets/-wide
convention:
- ../credentials.yaml (shared across every pipeline) -- Edmingle tutor
  login used to generate a key.
- ../notifications.yaml (this pipeline's own folder) -- where the
  generated-key notification is delivered.

Neither file is committed to version control.
"""

from __future__ import annotations

import os
import sys

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.normpath(os.path.join(_SCRIPT_DIR, "..", "..")))
import common

CREDENTIALS_PATH = os.path.normpath(os.path.join(_SCRIPT_DIR, "..", "..", "credentials.yaml"))

_edmingle = common.load_credentials(CREDENTIALS_PATH)
_notifications = common.load_notifications("edmingle_api_key_generator")

_tutor_login = _edmingle.get("tutor_login", {})
_email_channel = _notifications.get("channels", {}).get("email", {})
_smtp = _email_channel.get("smtp", {})

# Edmingle login settings ------------------------------------------------------
EDMINGLE_LOGIN_URL = _tutor_login.get("login_url", "")
EDMINGLE_USERNAME = _tutor_login.get("username", "")
EDMINGLE_PASSWORD = _tutor_login.get("password", "")
REQUEST_TIMEOUT_SECONDS = 30

# Email settings ---------------------------------------------------------------
EMAIL_FROM = _smtp.get("from_address", "")
EMAIL_TO = tuple(_email_channel.get("to_addresses", []))
SMTP_HOST = _smtp.get("host", "")
SMTP_PORT = int(_smtp.get("port", 587))
SMTP_TIMEOUT_SECONDS = int(_smtp.get("timeout_seconds", 20))
EMAIL_APP_PASSWORD = _smtp.get("app_password", "")
