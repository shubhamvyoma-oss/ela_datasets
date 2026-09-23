"""
common.py

Shared helpers for every pipeline under ela_datasets/, extracted from code
that existed as 3-7 near-identical copies across pipeline folders:
credentials loading, notification-settings loading, SMTP email sending, a
rolling-window rate limiter, and crash-safe atomic file writes.

What this deliberately does NOT centralize: each pipeline keeps its own
notifications.yaml (recipients/thresholds genuinely differ per pipeline)
and its own config.yaml / *.json (pipeline-specific tuning) -- only the
*code* to load and use them lives here. See NOTIFICATIONS.md at the repo
root for a one-page index of which pipeline emails whom.

Every pipeline is one directory below the repo root (e.g.
enrollments_reports/scripts/edmingle_export.py), so `from pathlib import
Path; sys.path.insert(0, str(Path(__file__).resolve().parents[2]))`
before `import common` is how each script reaches this module -- see any
pipeline's entry point for the exact lines.
"""

from __future__ import annotations

import csv
import json
import logging
import os
import smtplib
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any, Iterable

import yaml

REPO_ROOT = Path(__file__).resolve().parent


# ------------------------------------------------------------------ config

def load_credentials(path: str | Path | None = None) -> dict:
    """Load the shared `edmingle:` block from ela_datasets/credentials.yaml.
    `path` defaults to credentials.yaml next to this module (the repo
    root). Raises FileNotFoundError / yaml errors as-is -- every pipeline
    has always treated a missing/malformed credentials file as fatal."""
    p = Path(path) if path else REPO_ROOT / "credentials.yaml"
    with open(p, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("edmingle", {})


def load_notifications(pipeline_dir: str | Path) -> dict:
    """Load <pipeline_dir>/notifications.yaml. Returns {} if the file is
    missing, matching every pipeline's existing behavior of treating a
    missing notifications file as "notifications disabled", not an error."""
    p = Path(pipeline_dir) / "notifications.yaml"
    if not p.exists():
        return {}
    with open(p, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


# ------------------------------------------------------------------- email

def send_mail(notifications: dict, subject: str, body: str,
              logger: logging.Logger | None = None) -> bool:
    """Send a plain-text email per a loaded notifications.yaml's
    channels.email block. Best-effort: returns False and logs a warning
    instead of raising, matching every pipeline's existing "never let a
    notification failure fail the run" behavior.

    Tolerates the field-naming that already differs across pipelines'
    notifications.yaml files (not normalized here, to avoid touching live
    secrets): login can be `username` or fall back to `from_address`;
    the secret can be `app_password` or `password`; `to_addresses` can be
    a list or a comma-separated string.
    """
    log = logger or logging.getLogger(__name__)
    email_cfg = (notifications or {}).get("channels", {}).get("email", {})
    if not email_cfg.get("enabled"):
        log.warning("Email notifications disabled or not configured -- skipping send.")
        return False

    smtp_cfg = email_cfg.get("smtp", {})
    host = smtp_cfg.get("host")
    port = int(smtp_cfg.get("port", 587))
    from_address = smtp_cfg.get("from_address") or smtp_cfg.get("username")
    login_user = smtp_cfg.get("username") or from_address
    secret = smtp_cfg.get("app_password") or smtp_cfg.get("password")
    timeout = int(smtp_cfg.get("timeout_seconds", 30))
    use_tls = smtp_cfg.get("use_tls", True)

    to_raw = email_cfg.get("to_addresses") or []
    to_addresses = (
        [a.strip() for a in to_raw.split(",") if a.strip()]
        if isinstance(to_raw, str) else list(to_raw)
    )

    if not (host and login_user and secret and from_address and to_addresses):
        log.warning("Email notification config incomplete -- skipping send.")
        return False

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = from_address
    msg["To"] = ", ".join(to_addresses)

    try:
        with smtplib.SMTP(host, port, timeout=timeout) as server:
            if use_tls:
                server.starttls()
            server.login(login_user, secret)
            server.sendmail(from_address, to_addresses, msg.as_string())
        log.info(f"Email sent: {subject!r}")
        return True
    except Exception as exc:
        log.warning(f"Failed to send notification email ({subject!r}): {exc}")
        return False


# ------------------------------------------------------------ rate limiter

class RollingRateLimiter:
    """A true rolling-window rate limiter (deque of call timestamps), not a
    flat delay between calls -- acquire() blocks only long enough to keep
    the call count within `max_calls` per `window_seconds`. reset() clears
    history (e.g. after a 429 cooldown, so resuming doesn't immediately
    re-trip the limit).

    Previously duplicated (with an injectable clock=/sleep= pair that no
    test in the repo ever overrode) across enrollments_reports,
    ela_mis_datasets and country_wise_data -- consolidated here using
    time.monotonic/time.sleep directly.
    """

    def __init__(self, max_calls: int, window_seconds: float = 60.0,
                 logger: logging.Logger | None = None) -> None:
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self.logger = logger or logging.getLogger(__name__)
        self.calls: deque[float] = deque()

    def acquire(self) -> None:
        while True:
            now = time.monotonic()
            while self.calls and now - self.calls[0] >= self.window_seconds:
                self.calls.popleft()
            if len(self.calls) < self.max_calls:
                self.calls.append(now)
                return
            wait_seconds = max(0.0, self.window_seconds - (now - self.calls[0]))
            if wait_seconds >= 1.0:
                self.logger.info(f"Rate limit reached; waiting {wait_seconds:.2f} seconds")
            else:
                self.logger.debug(f"Rate-limit pacing wait: {wait_seconds:.2f} seconds")
            time.sleep(wait_seconds)

    def reset(self) -> None:
        self.calls.clear()


# --------------------------------------------------------------- file I/O

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def format_duration(seconds: float) -> str:
    total_seconds = max(0, int(round(seconds)))
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, seconds = divmod(remainder, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if days or hours:
        parts.append(f"{hours}h")
    if days or hours or minutes:
        parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)


def atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    """Write JSON to `path` via temp-file + fsync + os.replace, so a crash
    or power-loss mid-write can never leave a truncated/corrupt file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def atomic_write_csv(path: Path, fieldnames: list[str], rows: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))
