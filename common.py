"""
common.py

Shared helpers for every pipeline under ela_datasets/, extracted from code
that existed as 3-7 near-identical copies across pipeline folders:
credentials loading, notification-settings loading, SMTP email sending, a
rolling-window rate limiter, and crash-safe atomic file writes.

Each pipeline keeps its own notifications.yaml (recipients/thresholds genuinely differ per
pipeline) in that pipeline's own root folder (e.g. attendance/notifications.yaml) -- a sibling of
its scripts/ and output/ folders, not inside scripts/ itself, and not shared with other pipelines.
See NOTIFICATIONS.md at the repo root for a one-page index of which pipeline emails whom.

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
import random
import smtplib
import time
import uuid
from collections import deque
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

import requests
import yaml

REPO_ROOT = Path(__file__).resolve().parent


# ------------------------------------------------------------------ config

def load_credentials(path: str | Path | None = None) -> dict:
    """Load the shared `edmingle:` block from ela_datasets/credentials.yaml.
    `path` defaults to credentials.yaml next to this module (the repo
    root). Raises FileNotFoundError / yaml errors as-is -- every pipeline
    has always treated a missing/malformed credentials file as fatal."""
    p = Path(path) if path else REPO_ROOT / "credentials.yaml"
    with open(p, encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("edmingle", {})


def edmingle_settings(need_institute: bool = False, path: str | Path | None = None) -> dict:
    """The Edmingle settings every pipeline shares, read only from credentials.yaml:
    api_key, organization_id (str), institute_id (str), base_url (no trailing slash).
    Exits with a clear message if base_url or organization_id (and institute_id, when
    need_institute) is missing -- there are deliberately no built-in fallback ids or URLs."""
    try:
        creds = load_credentials(path)
    except FileNotFoundError as exc:
        raise SystemExit(f"credentials.yaml not found: {exc.filename}") from exc
    needed = ["base_url", "organization_id"] + (["institute_id"] if need_institute else [])
    missing = [k for k in needed if not creds.get(k)]
    if missing:
        raise SystemExit(f"credentials.yaml is missing edmingle.{', edmingle.'.join(missing)}")
    return {
        "api_key": str(creds.get("api_key", "")),
        "organization_id": str(creds["organization_id"]),
        "institute_id": str(creds.get("institute_id", "")),
        "base_url": str(creds["base_url"]).rstrip("/"),
    }


def auth_headers(api_key: str, org_id: str | int) -> dict:
    """Auth headers accepted by every Edmingle endpoint used here. The key goes in headers,
    never in the URL, so it cannot end up in a logged URL or a requests exception message."""
    return {"apikey": str(api_key), "orgid": str(org_id), "ORGID": str(org_id)}


def load_notifications(pipeline_name: str) -> dict:
    """Load <pipeline_name>/notifications.yaml from the repo root (that pipeline's own folder,
    a sibling of its scripts/ and output/ folders). Returns {} if the file is missing, matching
    every pipeline's existing behavior of treating a missing notifications file as "notifications
    disabled", not an error (some callers layer their own hard-fail check on top before calling
    this, when they want a missing file to be fatal instead)."""
    p = REPO_ROOT / pipeline_name / "notifications.yaml"
    if not p.exists():
        return {}
    with open(p, encoding="utf-8") as f:
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


# ---------------------------------------------------------- HTTP GET with retries

class ApiError(RuntimeError):
    """An Edmingle call that did not succeed (see PermanentAPIError / RetriesExhausted)."""


class PermanentAPIError(ApiError):
    """A status retrying cannot fix (bad key, org id or endpoint); .status holds it."""

    def __init__(self, message: str, status: int | None = None) -> None:
        super().__init__(message)
        self.status = status


class RetriesExhausted(ApiError):
    """A call with a finite `attempts` kept failing."""


def retry_after(resp: requests.Response) -> float:
    """The response's Retry-After header in seconds, or 0."""
    try:
        return float(resp.headers.get("Retry-After", ""))
    except (TypeError, ValueError):
        return 0.0


def get_json(
    url: str, *, headers: dict, params: dict | None = None, session: Any = None, timeout: float = 60,
    attempts: int | None = None, delay: float = 5.0, max_delay: float = 300.0, jitter: float = 0.0,
    permanent: Iterable[int] = (400, 401, 403, 404), block_seconds: float | Callable = 300,
    rate_limiter: "RollingRateLimiter | None" = None, validate: Callable[[Any], bool] | None = None,
    on_network_error: Callable[[Exception], bool] | None = None, label: str = "",
    logger: logging.Logger | Any | None = None,
) -> Any:
    """GET `url` and return the parsed JSON, retrying what a retry can fix -- the one HTTP loop
    every pipeline uses. `attempts=None` retries forever.

    - permanent statuses raise PermanentAPIError at once;
    - 429 waits `block_seconds` (a number, raised to any Retry-After header, or a function of the
      response), resets `rate_limiter`, and counts as an attempt;
    - other statuses, network errors, invalid JSON and `validate(data)` being false back off
      exponentially (delay, doubling to max_delay, plus up to `jitter` seconds);
    - `on_network_error(exc)` returning True means "handled, try again" without using an attempt
      (e.g. after waiting for the internet to come back);
    - after `attempts` failed attempts, RetriesExhausted is raised.
    The API key belongs in `headers`, never in the URL, so nothing logged here can contain it."""
    log = logger or logging.getLogger(__name__)
    caller = session or requests
    wait = delay
    attempt = 0
    while True:
        attempt += 1
        if rate_limiter:
            rate_limiter.acquire()
        try:
            resp = caller.get(url, headers=headers, params=params, timeout=timeout)
        except requests.RequestException as exc:
            if on_network_error and on_network_error(exc):
                attempt -= 1
                continue
            problem = f"network error ({type(exc).__name__})"
        else:
            if resp.status_code == 429:
                if attempts and attempt >= attempts:
                    raise RetriesExhausted(f"{label} rate limited (429) on every one of {attempt} attempts")
                pause = block_seconds(resp) if callable(block_seconds) else max(block_seconds, retry_after(resp))
                log.warning(f"{label} rate limited (429) on attempt {attempt}; waiting {pause:.0f}s")
                time.sleep(pause)
                if rate_limiter:
                    rate_limiter.reset()
                wait = delay
                continue
            if resp.status_code in permanent:
                raise PermanentAPIError(f"{label} HTTP {resp.status_code}: {resp.text[:300]}", resp.status_code)
            snippet = f": {' '.join(resp.text.split())[:120]}" if getattr(resp, "text", "") else ""  # what Edmingle said (never the key)
            if resp.status_code != 200:
                problem = f"HTTP {resp.status_code}{snippet}"
            else:
                try:
                    data = resp.json()
                except ValueError:
                    problem = "invalid JSON"
                else:
                    if validate is None or validate(data):
                        return data
                    problem = f"unexpected response shape{snippet}"
        if attempts and attempt >= attempts:
            raise RetriesExhausted(f"{label} failed after {attempt} attempts: {problem}")
        log.warning(f"{label} {problem} (attempt {attempt}); retrying in {wait:.1f}s")
        time.sleep(wait + random.uniform(0, jitter))
        wait = min(max_delay, wait * 2)


# --------------------------------------------------------------- file I/O

def utc_now() -> str:
    return datetime.now(UTC).isoformat()


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
