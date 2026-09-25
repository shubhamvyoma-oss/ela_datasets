"""
pipeline_common.py -- shared helpers for this pipeline's 4 network-calling scripts: config
loading (credentials.yaml + this pipeline's notifications.yaml), HTTP 429 backoff parsing,
output-folder resolution, rate-limit spacing, per-run file logging, and the end-of-run email
report. Credential/notification reading and SMTP sending delegate to ela_datasets/common.py.

USAGE
    from pipeline_common import (
        load_config, parse_retry_after_seconds, resolve_output_folder,
        RateLimiter, PipelineRunLogger, send_run_report, BASE_URL, auth_headers, require_config,
    )
"""

import re
import sys
import time
from datetime import datetime
from pathlib import Path

# The 4 entry-point scripts only add their own scripts/ folder to sys.path, not the
# ela_datasets/ repo root -- so this module adds it itself before importing common.py.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import common

DEFAULT_BLOCK_WAIT_SECONDS = 31 * 60  # fallback if "Try after X minutes" can't be parsed

# Read once, from credentials.yaml only (no built-in fallback URL or ids).
BASE_URL = common.edmingle_settings()["base_url"]
auth_headers = common.auth_headers  # re-exported for the stage scripts


def require_config(config: dict, key: str):
    """config[key] from credentials.yaml, or exit with a clear message -- no hardcoded fallback ids."""
    value = config.get(key)
    if not value:
        sys.exit(f"[ERROR] {key} is missing: set edmingle.{'organization_id' if key == 'org_id' else key} "
                 f"in ../../credentials.yaml")
    return int(value)


def load_config(script_dir: Path) -> dict:
    """Builds the config dict from the shared ../../credentials.yaml and this pipeline's own
    ../notifications.yaml. This pipeline has no config.yaml."""
    config: dict = {}
    _merge_credentials(config, script_dir)
    _merge_notifications(config)
    return config


def _merge_credentials(config: dict, script_dir: Path) -> None:
    """Copies api_key, org_id/orgid (the same organization_id under the two spellings the scripts
    use), institute_id and base_url from ../../credentials.yaml onto config."""
    credentials_path = script_dir.resolve().parent.parent / "credentials.yaml"
    edmingle = common.load_credentials(credentials_path)

    if "api_key" in edmingle:
        config["api_key"] = edmingle["api_key"]
    if "organization_id" in edmingle:
        config["org_id"] = edmingle["organization_id"]
        config["orgid"] = edmingle["organization_id"]
    if "institute_id" in edmingle:
        config["institute_id"] = edmingle["institute_id"]
    if "base_url" in edmingle:
        config["base_url"] = edmingle["base_url"]


def _merge_notifications(config: dict) -> None:
    """Puts this pipeline's SMTP settings and recipients (from its own notifications.yaml) onto
    config["smtp"] in the flat shape send_run_report() checks, and keeps the raw notifications dict on
    config["_notifications"] for common.send_mail(). A missing file means notifications are off."""
    notifications = common.load_notifications("session_wise_attendance")
    config["_notifications"] = notifications

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
    (not the process cwd — matters under cron), creates it.
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
    under Edmingle's hard 30 calls/min cap.

    NOTE: this is a flat per-call delay (start()/wait(), sleeping out
    whatever's left of a fixed 60/calls_per_minute spacing around each
    network call), not a true rolling window. It does not share a shape
    with common.py's RollingRateLimiter (max_calls/window_seconds,
    acquire()/reset(), deque-based) used by other migrated pipelines, so it
    was deliberately NOT migrated to it -- forcing the two together would
    change how every --calls_per_minute run here actually paces requests."""

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
        # Run logs are generated output, so they go under ../output/logs/, same convention as
        # resolve_output_folder() -- not next to the scripts themselves.
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


class _PrintLogger:
    """Minimal logging.Logger-like shim (just .info()/.warning()) so
    common.send_mail()'s log lines come out as this pipeline's existing
    print()-based [INFO]/[WARN] lines. This pipeline has never used the
    `logging` module -- everything goes through print(), which
    PipelineRunLogger already mirrors into the run's log file -- so this
    keeps send_run_report()'s console/log output in the same style instead
    of introducing a second, differently-formatted logging path."""

    @staticmethod
    def info(msg, *args, **kwargs):
        print(f"[INFO] {msg}")

    @staticmethod
    def warning(msg, *args, **kwargs):
        print(f"[WARN] {msg}")


def send_run_report(stage_name: str, summary: dict, config: dict):
    """Emails a plaintext run summary. Best-effort: a missing/placeholder SMTP config never crashes
    the pipeline. A report is sent when the config has an smtp block with at least one recipient
    (merged from notifications.yaml by _merge_notifications); sending goes through
    common.send_mail(), which never raises."""
    smtp_cfg = config.get("smtp") or {}
    to_addresses = smtp_cfg.get("to_addresses") or []
    if not smtp_cfg or not to_addresses:
        print(f"[WARN] Email report skipped for {stage_name}: no smtp config / to_addresses in notifications.yaml")
        return

    lines = [f"Vyoma attendance pipeline - {stage_name} run report", ""]
    for key, value in summary.items():
        lines.append(f"{key}: {value}")
    body = "\n".join(lines)

    status = "SUCCESS" if not summary.get("errors") else "COMPLETED WITH ERRORS"
    subject = f"[Vyoma Pipeline] {stage_name} - {status} ({datetime.now().strftime('%Y-%m-%d %H:%M')})"

    notifications = config.get("_notifications") or {}
    common.send_mail(notifications, subject, body, _PrintLogger())
