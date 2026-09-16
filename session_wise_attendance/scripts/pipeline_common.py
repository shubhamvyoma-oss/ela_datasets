"""
pipeline_common.py
--------------------------------------------------------------------------
Shared helpers used by every network-calling script in the pipeline:
config loading, HTTP 429 backoff parsing, output-folder resolution,
rate-limit spacing, per-run file logging, and the end-of-run email report.
Centralized here so the 4 pipeline scripts can't drift into 4 slightly
different copies of the same logic (they used to each carry their own).

CREDENTIALS
-----------
The Edmingle api_key/org_id/orgid/institute_id used to live directly in
config.yaml. They now live in the shared ../../credentials.yaml (one Edmingle
key for every pipeline under ela_datasets/ -- see that file's header
comment), and SMTP settings now live in this pipeline's own
notifications.yaml (recipients/channels are per-pipeline, not shared).
load_config() merges both onto the dict it returns, in the exact same
shape (api_key/org_id/orgid/institute_id/smtp keys) the 5 scripts already
read -- so no other call site needs to change.

USAGE
-----
    from pipeline_common import (
        load_config, parse_retry_after_seconds, resolve_output_folder,
        RateLimiter, PipelineRunLogger, send_run_report,
    )
"""

import re
import smtplib
import sys
import time
from datetime import datetime
from email.mime.text import MIMEText
from pathlib import Path

import yaml

DEFAULT_BLOCK_WAIT_SECONDS = 31 * 60  # fallback if "Try after X minutes" can't be parsed


def load_config(config_path: Path) -> dict:
    """Reads config.yaml; returns {} (with a warning) if it doesn't exist.
    Also merges in the shared Edmingle credentials from ../../credentials.yaml
    and this pipeline's notification/SMTP settings from notifications.yaml
    (same folder as config.yaml) -- see module docstring."""
    if not config_path.exists():
        print(f"[WARN] {config_path} not found — pass --apikey explicitly.")
        config = {}
    else:
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}

    _merge_credentials(config, config_path)
    _merge_notifications(config, config_path)
    return config


def _merge_credentials(config: dict, config_path: Path) -> None:
    """Merges api_key/org_id/orgid/institute_id from the shared
    ../../credentials.yaml onto config, in the same keys the 5 scripts already
    read via config.get("api_key")/config.get("org_id", ...)/etc. org_id
    and orgid are kept as two separate keys (some endpoints expect the
    uppercase param name) but both are sourced from the same
    edmingle.organization_id value."""
    # .resolve() first: Path('.').parent == Path('.') in pathlib, so a
    # relative config_path (e.g. when a script is launched as
    # `python3 script.py` from inside its own folder, giving a relative
    # __file__) would otherwise make parent.parent.parent stay in the same
    # folder instead of climbing to ela_datasets/. config.yaml now lives in
    # this pipeline's scripts/ subfolder (one level deeper than before the
    # scripts/output split), so climbing to ela_datasets/ takes one extra
    # ".parent": scripts/ -> session_wise_attendance/ -> ela_datasets/.
    credentials_path = config_path.resolve().parent.parent.parent / "credentials.yaml"
    if not credentials_path.exists():
        print(f"[WARN] {credentials_path} not found — pass --apikey explicitly.")
        return
    with open(credentials_path, "r", encoding="utf-8") as f:
        credentials = yaml.safe_load(f) or {}
    edmingle = credentials.get("edmingle") or {}

    if "api_key" in edmingle:
        config["api_key"] = edmingle["api_key"]
    if "organization_id" in edmingle:
        config["org_id"] = edmingle["organization_id"]
        config["orgid"] = edmingle["organization_id"]
    if "institute_id" in edmingle:
        config["institute_id"] = edmingle["institute_id"]
    # base_url isn't currently read from config.yaml by any script (each
    # hardcodes its own BASE_URL constant), but keep this here so config
    # still has a usable base_url even if that ever changes.
    if "base_url" not in config and "base_url" in edmingle:
        config["base_url"] = edmingle["base_url"]


def _merge_notifications(config: dict, config_path: Path) -> None:
    """Merges this pipeline's notifications.yaml (email SMTP settings +
    recipients) onto config["smtp"], in the exact shape send_run_report()
    already expects (host/port/username/app_password/from_address/
    use_tls/to_addresses in one flat dict)."""
    notifications_path = config_path.resolve().parent / "notifications.yaml"
    if not notifications_path.exists():
        return
    with open(notifications_path, "r", encoding="utf-8") as f:
        notifications = yaml.safe_load(f) or {}
    email_cfg = ((notifications.get("channels") or {}).get("email")) or {}
    if not email_cfg.get("enabled", False):
        return

    smtp = dict(email_cfg.get("smtp") or {})
    if "to_addresses" in email_cfg:
        smtp["to_addresses"] = email_cfg["to_addresses"]
    config["smtp"] = smtp


def parse_retry_after_seconds(response_text: str, default_seconds: int = DEFAULT_BLOCK_WAIT_SECONDS) -> int:
    """Parses "Try after 29.93 minutes" out of Edmingle's 429 body. Falls back
    to default_seconds if the message can't be parsed."""
    match = re.search(r"[Tt]ry after ([\d.]+)\s*minutes?", response_text)
    if match:
        return int(float(match.group(1)) * 60) + 15  # +15s buffer past what Edmingle reports
    return default_seconds


def resolve_output_folder(config: dict, script_path: Path) -> Path:
    """Resolves config's output_folder relative to the SCRIPT's own location
    (not the process cwd — matters when launched via Task Scheduler), creates it.
    Scripts live in this pipeline's scripts/ subfolder now (one level below
    the pipeline root), so a relative output_folder is resolved against
    <script's folder>/../output -- the pipeline's output/ subfolder -- not
    the script's own directory."""
    raw_output_folder = config.get("output_folder", ".")
    output_folder = Path(raw_output_folder)
    if not output_folder.is_absolute():
        output_folder = (script_path.parent / ".." / "output" / output_folder).resolve()
    output_folder.mkdir(parents=True, exist_ok=True)
    return output_folder


class RateLimiter:
    """Sleeps out whatever's left of the target per-call spacing, accounting
    for time already spent on the call itself — keeps every script safely
    under Edmingle's hard 30 calls/min cap."""

    def __init__(self, calls_per_minute: float):
        self.delay_seconds = 60.0 / calls_per_minute
        self._call_start = None

    def start(self):
        """Call at the top of each loop iteration, before the network call."""
        self._call_start = time.time()

    def wait(self):
        """Call right after the network call returns."""
        elapsed = time.time() - self._call_start
        remaining = self.delay_seconds - elapsed
        if remaining > 0:
            time.sleep(remaining)


class _Tee:
    """Writes to two streams at once — used to mirror stdout/stderr into a log file."""

    def __init__(self, *streams):
        self.streams = streams

    def write(self, data):
        for s in self.streams:
            s.write(data)

    def flush(self):
        for s in self.streams:
            s.flush()


class PipelineRunLogger:
    """Context manager: mirrors every print() during a run into
    logs/<stage_name>/<stage_name>_<timestamp>.log, in addition to the
    console — so every layer's process (start time, rows processed,
    skipped/error counts, estimates) leaves a durable, timestamped trail
    for debugging without touching every print() call site."""

    def __init__(self, stage_name: str, script_path: Path, args_summary: str = ""):
        self.stage_name = stage_name
        # Scripts live in this pipeline's scripts/ subfolder now; run logs
        # are generated output, so they go under <script's folder>/../output
        # (the pipeline's output/ subfolder), same convention as
        # resolve_output_folder(), not next to the scripts themselves.
        self.logs_dir = (script_path.parent / ".." / "output" / "logs" / stage_name).resolve()
        self.args_summary = args_summary
        self.start_time = None
        self.log_path = None

    def __enter__(self):
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.start_time = datetime.now()
        stamp = self.start_time.strftime("%Y%m%d_%H%M%S")
        self.log_path = self.logs_dir / f"{self.stage_name}_{stamp}.log"
        self._log_file = open(self.log_path, "w", encoding="utf-8")
        self._orig_stdout = sys.stdout
        self._orig_stderr = sys.stderr
        sys.stdout = _Tee(self._orig_stdout, self._log_file)
        sys.stderr = _Tee(self._orig_stderr, self._log_file)
        print(f"[RUN START] stage={self.stage_name} time={self.start_time.isoformat()} args={self.args_summary}")
        return self

    def __exit__(self, exc_type, exc, tb):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        # SystemExit(0)/(None) is a normal exit (e.g. --help) not a failure — only
        # flag real errors (non-zero sys.exit, or any other raised exception).
        is_clean_exit = exc_type is SystemExit and exc.code in (0, None)
        if exc_type is not None and not is_clean_exit:
            print(f"[RUN FAILED] stage={self.stage_name} error={exc}")
        print(f"[RUN END] stage={self.stage_name} time={end_time.isoformat()} "
              f"duration_sec={duration:.1f} log={self.log_path}")
        sys.stdout = self._orig_stdout
        sys.stderr = self._orig_stderr
        self._log_file.close()
        return False  # never swallow exceptions — let the script's own error handling see them


def send_run_report(stage_name: str, summary: dict, config: dict):
    """Emails a plaintext run summary using config.yaml's smtp: block.
    Best-effort only: a missing/placeholder SMTP config must never crash
    the pipeline, so every failure is caught here and logged as a warning."""
    smtp_cfg = config.get("smtp") or {}
    to_addresses = smtp_cfg.get("to_addresses") or []
    if not smtp_cfg or not to_addresses:
        print(f"[WARN] Email report skipped for {stage_name}: no smtp config / to_addresses in config.yaml")
        return

    lines = [f"Vyoma attendance pipeline - {stage_name} run report", ""]
    for key, value in summary.items():
        lines.append(f"{key}: {value}")
    body = "\n".join(lines)

    status = "SUCCESS" if not summary.get("errors") else "COMPLETED WITH ERRORS"
    msg = MIMEText(body)
    msg["Subject"] = f"[Vyoma Pipeline] {stage_name} - {status} ({datetime.now().strftime('%Y-%m-%d %H:%M')})"
    msg["From"] = smtp_cfg.get("from_address") or smtp_cfg.get("username", "")
    msg["To"] = ", ".join(to_addresses)

    try:
        with smtplib.SMTP(smtp_cfg["host"], smtp_cfg.get("port", 587), timeout=30) as server:
            if smtp_cfg.get("use_tls", True):
                server.starttls()
            server.login(smtp_cfg["username"], smtp_cfg["app_password"])
            server.sendmail(msg["From"], to_addresses, msg.as_string())
        print(f"[INFO] Emailed run report for {stage_name} to {', '.join(to_addresses)}")
    except Exception as e:
        print(f"[WARN] Email report failed for {stage_name}: {e}")
