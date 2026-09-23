"""
attendance.py
==============================

Production-grade Edmingle report_type=55 attendance pipeline.
Pulls daily attendance data for a date range, saves raw staging CSVs
per day, combines them, and produces a one-row-per-batch summary.

Designed for long multi-month runs (Jan 2025 to Jun 2026 = 546 days,
~68 min runtime, ~150 MB raw output) with full operational robustness:

    - Paths + behaviour in config.yaml; Edmingle API credentials in the
      shared ../../credentials.yaml; email/SMTP settings in notifications.yaml
      -- never hardcoded here
    - Rotating daily log file (30-day retention)
    - Checkpoint / resume  -- kill and restart at any point safely
    - Lock file            -- prevents concurrent duplicate runs
    - Startup API validation before the long loop begins
    - Disk space check before starting
    - Exponential backoff + jitter for 429 / 5xx / timeouts
    - Retry-After header respected for 429 rate-limit responses
    - Fatal stops (no retry) for 401 / 403 / 404 + immediate email
    - Consecutive-error circuit breaker
    - Per-day staging CSVs (one file per date = resumable after crash)
    - SIGINT / SIGTERM handler -- checkpoints cleanly before exit
    - HTML email alerts: critical errors, warnings, and completion
    - Dry-run mode -- logs and simulates everything, calls no real API
    - --retry-failed flag -- re-process only previously failed dates

NETWORK RESILIENCE (v1.2.0)
    Network outages NO LONGER kill the run. When a request fails
    because the internet itself is down (DNS failure / no route),
    the pipeline pauses, probes connectivity every
    api.connectivity_check_interval_seconds, and automatically
    resumes the SAME date the moment the connection returns.
    Offline waiting does NOT consume retry attempts and does NOT
    trip the circuit breaker -- those are reserved for genuine API
    failures while the internet is up (e.g. Edmingle itself down).
    api.max_offline_wait_minutes caps the wait (0 = wait forever).

    Device shutdown / power loss: per-day staging CSVs + checkpoint
    mean at most the in-flight date is re-fetched. Re-run the same
    command after boot and it resumes. Pair with run_pipeline.bat
    (auto-restart wrapper) + Windows Task Scheduler "At startup"
    trigger for fully hands-off recovery.

STUDENT STATUS FILTERING (v1.1.0)
    Only ACTIVE students are counted. Any row whose studentBatchStatus
    is not in pipeline.active_status_values (default: ["Active"]) --
    e.g. "Archived" -- is dropped during cleaning. This is ON by
    default; set pipeline.exclude_inactive_students: false in
    config.yaml to disable.

USAGE
  python attendance.py --from 2020-01-01 --to 2026-08-31
  python attendance.py --date 2026-06-15
  python attendance.py                       # config lookback
  python attendance.py --from-file raw.csv  # skip API
  python attendance.py --dry-run --from 2020-01-01 --to 2020-01-07
  python attendance.py --retry-failed        # re-run failed dates only
  python attendance.py --reset-checkpoint    # start fresh
  python attendance.py --config /other/config.yaml ...

DEPENDENCIES
  pip install pandas requests pyyaml
"""

import argparse
import json
import logging
import logging.handlers
import os
import platform
import random
import signal
import smtplib
import socket
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set
# before any local module import below, so this and every module it pulls
# in gets compiled into one shared location instead of a scripts/__pycache__
# folder per pipeline.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import common

import pandas as pd
import requests
import yaml


# ============================================================
# CONSTANTS
# ============================================================

VERSION = "1.2.0"
IST = timezone(timedelta(hours=5, minutes=30))

OUTPUT_COLUMNS = [
    "batch_Id", "batchName", "bundle_Id", "bundleName", "course_Id",
    "courseName", "teacher_Id", "teacherName", "total_students_enrolled", "first_class_date",
    "last_class_date", "first_class_attendance", "last_class_attendance",
    "total_present_marks", "total_absent_marks",
    "attendance_percentage", "average_class_attendance",
    "highest_class_attendance", "lowest_class_attendance", "average_rating",
    "retention_percentage", "attendance_drop",
]

# One row per (batch, session) -- written as the session-wise CSV.
# NOTE: total_classes_remaining = planned - conducted. With report_type=55
# "planned" only includes sessions present in the report data, so remaining
# stays 0 unless the report contains future-dated sessions. True remaining
# counts need the batch schedule endpoint (future enhancement).
SESSION_OUTPUT_COLUMNS = [
    "batch_Id", "batchName", "course_Id", "courseName", "session_number",
    "classDate", "is_conducted", "present_count", "absent_count",
    "late_count", "total_marked", "session_attendance_percentage",
]


# ============================================================
# CUSTOM EXCEPTIONS
# ============================================================

class FatalAPIError(Exception):
    """401 / 403 / 404 -- no retry, stop the run immediately."""

class PipelineError(Exception):
    """General unrecoverable pipeline error."""


# ============================================================
# CONFIG
# ============================================================

_DEFAULTS = {
    "api": {
        "timeout_seconds": 60,
        "rate_limit_sleep_seconds": 2.5,
        "max_retries": 3,
        "retry_backoff_base_seconds": 5,
        "retry_backoff_max_seconds": 120,
        "retry_jitter_seconds": 1.0,
        "max_consecutive_errors": 3,
        "validate_on_startup": True,
        # ── Network resilience (v1.2.0) ──────────────────────────
        # When the internet drops, pause and auto-resume instead of
        # failing dates / tripping the circuit breaker.
        "wait_for_reconnect": True,
        "connectivity_check_interval_seconds": 30,
        "connectivity_check_timeout_seconds": 5,
        "connectivity_probe_host": "8.8.8.8",   # raw-internet probe (no DNS needed)
        "connectivity_probe_port": 53,
        "max_offline_wait_minutes": 0,          # 0 = wait forever
    },
    "email": {
        "enabled": False,
        "smtp_port": 587,
        "smtp_timeout_seconds": 15,
        "notify_on_critical": True,
        "notify_on_warning": False,
        "notify_on_completion": True,
    },
    "pipeline": {
        "default_lookback_days": 30,
        "save_combined_raw_csv": True,
        "cleanup_staging_after_combine": False,
        "min_free_disk_mb": 512,
        "present_value": "P",
        "absent_value": "A",
        "late_value": "L",
        "write_session_wise_csv": True,
        "session_id_column": "attendance_id",
        "date_format": "%d %b %Y",
        "time_format": "%I:%M %p",
        "treat_zero_rating_as_missing": True,
        # Keep ONLY active students. Rows with any other
        # studentBatchStatus (e.g. "Archived") are excluded.
        # ON by default as of v1.1.0.
        "exclude_inactive_students": True,
        "active_status_values": ["Active"],
    },
    "paths": {},
}


def _deep_merge(base: dict, override: dict) -> dict:
    result = base.copy()
    for k, v in override.items():
        if k in result and isinstance(result[k], dict) and isinstance(v, dict):
            result[k] = _deep_merge(result[k], v)
        else:
            result[k] = v
    return result


def load_config(config_path: str) -> dict:
    path = Path(config_path)
    if not path.exists():
        sys.exit(
            f"\nConfig file not found: {config_path}\n"
            f"Copy config.yaml.example to config.yaml, fill in your credentials.\n"
        )
    with open(path, encoding="utf-8") as f:
        user_cfg = yaml.safe_load(f) or {}

    cfg = _deep_merge(_DEFAULTS, user_cfg)

    # ── Shared Edmingle credentials (ela_datasets/credentials.yaml) ──────
    # The Edmingle API key + org id are shared by every pipeline under
    # ela_datasets/ and live in one place, two levels above this script's
    # own folder (attendance/scripts/attendance.py -> ela_datasets/).
    # They are no longer read from config.yaml.
    script_dir = Path(__file__).resolve().parent
    creds_path = script_dir.parent.parent / "credentials.yaml"
    if not creds_path.exists():
        sys.exit(f"\nShared credentials file not found: {creds_path}\n")
    edmingle_cfg = common.load_credentials(creds_path)
    cfg["api"]["key"]    = edmingle_cfg.get("api_key", "")
    cfg["api"]["org_id"] = str(edmingle_cfg.get("organization_id", ""))

    # ── Per-pipeline notification settings (attendance/notifications.yaml) ──
    # Email/SMTP settings are dedicated to this pipeline (not shared) and
    # live alongside config.yaml, no longer inside it.
    notif_path = script_dir / "notifications.yaml"
    if not notif_path.exists():
        sys.exit(f"\nNotifications file not found: {notif_path}\n")
    notif_cfg = common.load_notifications(script_dir)
    email_channel = ((notif_cfg.get("channels", {}) or {}).get("email", {})) or {}
    smtp = email_channel.get("smtp", {}) or {}
    cfg["email"]["enabled"]              = email_channel.get(
        "enabled", cfg["email"].get("enabled", False))
    cfg["email"]["smtp_host"]            = smtp.get("host")
    cfg["email"]["smtp_port"]            = smtp.get(
        "port", cfg["email"].get("smtp_port", 587))
    cfg["email"]["smtp_user"]            = smtp.get("username")
    cfg["email"]["smtp_password"]        = smtp.get("app_password")
    cfg["email"]["from_address"]         = smtp.get("from_address")
    cfg["email"]["smtp_timeout_seconds"] = smtp.get(
        "timeout_seconds", cfg["email"].get("smtp_timeout_seconds", 15))
    cfg["email"]["to_addresses"]         = email_channel.get("to_addresses", [])
    cfg["email"]["notify_on_critical"]   = email_channel.get(
        "notify_on_critical", cfg["email"].get("notify_on_critical", True))
    cfg["email"]["notify_on_warning"]    = email_channel.get(
        "notify_on_warning", cfg["email"].get("notify_on_warning", False))
    cfg["email"]["notify_on_completion"] = email_channel.get(
        "notify_on_completion", cfg["email"].get("notify_on_completion", True))

    required = [("api", "url"),
                ("paths", "output_folder"), ("paths", "log_folder")]
    missing = [f"{a}.{b}" for a, b in required
               if not str(cfg.get(a, {}).get(b, "")).strip()]
    if missing:
        sys.exit(f"\nMissing required config keys: {', '.join(missing)}\n")

    if not str(cfg["api"].get("key", "")).strip():
        sys.exit(f"\nMissing edmingle.api_key in shared credentials file: {creds_path}\n")
    if not str(cfg["api"].get("org_id", "")).strip():
        sys.exit(f"\nMissing edmingle.organization_id in shared credentials file: {creds_path}\n")

    # ── Anchor relative path config values to THIS SCRIPT'S folder ───────
    # A bare relative string in YAML (e.g. "." or "./logs") resolves
    # against the process's current working directory at runtime -- which
    # is NOT the same as "the script's own folder" if the pipeline is
    # invoked from elsewhere (cron, Task Scheduler, a wrapper .bat that
    # cd's somewhere else, etc). Anchor explicitly to script_dir (with
    # config.yaml's paths pointing at ../output) so output always lands in
    # the attendance/output/ folder regardless of the caller's cwd.
    # Absolute paths (still supported, e.g. for a dedicated data drive)
    # are left untouched.
    for _key in ("output_folder", "log_folder", "staging_folder",
                 "checkpoint_file", "lock_file"):
        _val = cfg["paths"].get(_key)
        if _val and not Path(_val).is_absolute():
            cfg["paths"][_key] = str((script_dir / _val).resolve())

    out = Path(cfg["paths"]["output_folder"])
    cfg["paths"].setdefault("staging_folder",  str(out / "staging"))
    cfg["paths"].setdefault("checkpoint_file", str(out / "pipeline_checkpoint.json"))
    cfg["paths"].setdefault("lock_file",        str(out / "pipeline.lock"))

    return cfg


# ============================================================
# LOGGING
# ============================================================

def setup_logging(cfg: dict, verbose: bool = False) -> logging.Logger:
    log_dir = Path(cfg["paths"]["log_folder"])
    log_dir.mkdir(parents=True, exist_ok=True)

    fmt     = "%(asctime)s IST | %(levelname)-8s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.DEBUG)

    fh = logging.handlers.TimedRotatingFileHandler(
        log_dir / "pipeline.log", when="midnight", interval=1,
        backupCount=30, encoding="utf-8"
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter(fmt, datefmt=datefmt))

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)
    ch.setFormatter(logging.Formatter(fmt, datefmt=datefmt))

    logger.addHandler(fh)
    logger.addHandler(ch)
    logging.Formatter.converter = lambda *args: datetime.now(IST).timetuple()
    return logger


# ============================================================
# LOCK FILE
# ============================================================

class LockFile:
    def __init__(self, path: str, log: logging.Logger):
        self.path = Path(path)
        self.log  = log

    def acquire(self):
        if self.path.exists():
            try:
                pid = int(self.path.read_text().strip())
                if self._pid_running(pid):
                    raise PipelineError(
                        f"Another instance already running (PID {pid}). "
                        f"Lock: {self.path}. "
                        f"If that process is dead, delete the lock file and re-run."
                    )
                self.log.warning(f"Stale lock (PID {pid} not running). Removing.")
                self.path.unlink()
            except ValueError:
                self.log.warning(f"Unreadable lock file. Removing.")
                self.path.unlink()
        self.path.write_text(str(os.getpid()))
        self.log.debug(f"Lock acquired (PID {os.getpid()})")

    def release(self):
        if self.path.exists():
            self.path.unlink()
            self.log.debug("Lock released.")

    @staticmethod
    def _pid_running(pid: int) -> bool:
        try:
            if platform.system() == "Windows":
                import ctypes
                handle = ctypes.windll.kernel32.OpenProcess(0x00100000, False, pid)
                if handle:
                    ctypes.windll.kernel32.CloseHandle(handle)
                    return True
                return False
            else:
                os.kill(pid, 0)
                return True
        except (OSError, PermissionError):
            return False


# ============================================================
# CHECKPOINT
# ============================================================

class Checkpoint:
    SUCCESS = "success"
    FAILED  = "failed"
    SKIPPED = "skipped"

    def __init__(self, path: str, log: logging.Logger):
        self.path  = Path(path)
        self.log   = log
        self._data = {}

    def load(self):
        if self.path.exists():
            try:
                with open(self.path, encoding="utf-8") as f:
                    self._data = json.load(f)
                done = sum(1 for v in self._data.get("dates", {}).values()
                           if v["status"] == self.SUCCESS)
                self.log.info(f"Checkpoint loaded: {done} date(s) already done.")
            except (json.JSONDecodeError, KeyError) as e:
                self.log.warning(f"Checkpoint corrupt ({e}). Starting fresh.")
                self._data = {}
        else:
            self.log.info("No checkpoint found. Fresh run.")
        self._data.setdefault("dates", {})
        self._data.setdefault("version", VERSION)

    def reset(self):
        self._data = {"dates": {}, "version": VERSION}
        self._save()
        self.log.info("Checkpoint reset.")

    def is_done(self, date_str: str) -> bool:
        return self._data["dates"].get(date_str, {}).get("status") == self.SUCCESS

    def is_failed(self, date_str: str) -> bool:
        return self._data["dates"].get(date_str, {}).get("status") == self.FAILED

    def mark(self, date_str: str, status: str, **extra):
        self._data["dates"][date_str] = {
            "status": status,
            "updated_at": datetime.now(IST).isoformat(),
            **extra,
        }
        self._save()

    def failed_dates(self) -> list:
        return [d for d, v in self._data["dates"].items()
                if v["status"] == self.FAILED]

    def _save(self):
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2, default=str)
        tmp.replace(self.path)


# ============================================================
# EMAIL
# ============================================================

class EmailNotifier:
    """HTML email alerts. Never raises -- email failure never crashes the pipeline."""

    def __init__(self, cfg: dict, log: logging.Logger):
        self.cfg     = cfg["email"]
        self.log     = log
        self.enabled = self.cfg.get("enabled", False)

    def _send(self, subject: str, body_html: str):
        if not self.enabled:
            return
        try:
            msg = MIMEMultipart("alternative")
            msg["From"]    = self.cfg["from_address"]
            msg["To"]      = ", ".join(self.cfg["to_addresses"])
            msg["Subject"] = subject
            msg.attach(MIMEText(body_html, "html"))
            with smtplib.SMTP(
                self.cfg["smtp_host"], self.cfg["smtp_port"],
                timeout=self.cfg.get("smtp_timeout_seconds", 15),
            ) as srv:
                srv.ehlo()
                srv.starttls()
                srv.login(self.cfg["smtp_user"], self.cfg["smtp_password"])
                srv.sendmail(self.cfg["from_address"],
                             self.cfg["to_addresses"], msg.as_string())
            self.log.info(f"Email sent: {subject}")
        except Exception as e:
            self.log.error(f"Email failed (pipeline continues): {e}")

    @staticmethod
    def _html(title: str, color: str, rows: list) -> str:
        rows_html = "".join(
            f"<tr>"
            f"<td style='padding:4px 12px;border:1px solid #ddd'><b>{k}</b></td>"
            f"<td style='padding:4px 12px;border:1px solid #ddd'>{v}</td>"
            f"</tr>"
            for k, v in rows
        )
        return (
            f"<html><body style='font-family:Arial,sans-serif;color:#333'>"
            f"<h2 style='background:{color};color:#fff;padding:10px 16px;"
            f"border-radius:4px'>{title}</h2>"
            f"<table style='border-collapse:collapse;font-size:14px'>{rows_html}</table>"
            f"<p style='color:#888;font-size:12px;margin-top:20px'>"
            f"Edmingle Attendance Pipeline v{VERSION}</p>"
            f"</body></html>"
        )

    def send_critical(self, error: str, context: dict):
        if not self.cfg.get("notify_on_critical", True):
            return
        body = self._html("CRITICAL Error", "#c0392b",
                          [("Error", error), *context.items()])
        self._send(f"[CRITICAL] Edmingle Pipeline -- {error[:80]}", body)

    def send_warning(self, message: str, context: dict):
        if not self.cfg.get("notify_on_warning", False):
            return
        body = self._html("Pipeline Warning", "#e67e22",
                          [("Warning", message), *context.items()])
        self._send(f"[WARNING] Edmingle Pipeline -- {message[:80]}", body)

    def send_completion(self, stats: dict):
        if not self.cfg.get("notify_on_completion", True):
            return
        body = self._html("Pipeline Completed", "#27ae60", list(stats.items()))
        self._send("[SUCCESS] Edmingle Pipeline -- Run Complete", body)


# ============================================================
# DISK SPACE CHECK
# ============================================================

def check_disk_space(cfg: dict, log: logging.Logger):
    required_mb = cfg["pipeline"]["min_free_disk_mb"]
    folder      = cfg["paths"]["output_folder"]
    Path(folder).mkdir(parents=True, exist_ok=True)
    try:
        import shutil
        free_mb = shutil.disk_usage(folder).free / (1024 ** 2)
        log.info(f"Free disk space: {free_mb:,.0f} MB (required: {required_mb} MB)")
        if free_mb < required_mb:
            raise PipelineError(
                f"Insufficient disk space: {free_mb:.0f} MB free, "
                f"{required_mb} MB required in {folder}."
            )
    except PipelineError:
        raise
    except Exception as e:
        log.warning(f"Disk space check skipped ({e}). Continuing.")


# ============================================================
# API LAYER
# ============================================================

def build_api_session() -> requests.Session:
    return requests.Session()


# ── Network connectivity (v1.2.0) ────────────────────────────────────

def _api_host(cfg: dict) -> str:
    from urllib.parse import urlparse
    return urlparse(cfg["api"]["url"]).hostname or "vyoma-api.edmingle.com"


def is_online(cfg: dict) -> bool:
    """
    True only if BOTH pass:
      1. Raw TCP connect to probe host (8.8.8.8:53) -- proves internet
         is up without needing DNS.
      2. DNS resolution of the API host -- proves we can actually
         reach Edmingle (your outage was a DNS failure: Errno 11001).
    """
    api_cfg = cfg["api"]
    timeout = api_cfg.get("connectivity_check_timeout_seconds", 5)
    try:
        with socket.create_connection(
            (api_cfg.get("connectivity_probe_host", "8.8.8.8"),
             api_cfg.get("connectivity_probe_port", 53)),
            timeout=timeout,
        ):
            pass
        socket.getaddrinfo(_api_host(cfg), 443)
        return True
    except OSError:
        return False


def wait_for_connection(cfg: dict, log: logging.Logger, context: str = ""):
    """
    Block until the internet is back. Logs progress, never consumes
    retry attempts. Respects api.max_offline_wait_minutes (0 = forever).
    Raises PipelineError only if the cap is exceeded.
    SIGINT/SIGTERM still work during the wait (checkpoint is safe).
    """
    api_cfg   = cfg["api"]
    interval  = api_cfg.get("connectivity_check_interval_seconds", 30)
    max_min   = api_cfg.get("max_offline_wait_minutes", 0)
    started   = time.monotonic()
    checks    = 0

    log.warning(
        f"NETWORK DOWN{' at ' + context if context else ''}. "
        f"Pausing pipeline -- probing every {interval}s until reconnected. "
        f"(Checkpoint is safe; Ctrl+C anytime to stop and resume later.)"
    )

    while True:
        if is_online(cfg):
            offline_s = time.monotonic() - started
            log.info(
                f"NETWORK RESTORED after {offline_s/60:.1f} min "
                f"({checks} probe(s)). Resuming{' ' + context if context else ''}."
            )
            time.sleep(2)  # small settle time after reconnect
            return

        checks += 1
        elapsed_min = (time.monotonic() - started) / 60
        # Log every probe for the first 5, then every 10th (avoid log spam
        # during an overnight outage).
        if checks <= 5 or checks % 10 == 0:
            log.info(
                f"  Still offline ({elapsed_min:.1f} min, probe #{checks}). "
                f"Next check in {interval}s."
            )
        if max_min and elapsed_min >= max_min:
            raise PipelineError(
                f"Offline for {elapsed_min:.0f} min, exceeding "
                f"api.max_offline_wait_minutes={max_min}. Stopping "
                f"(checkpoint saved -- re-run to resume)."
            )
        time.sleep(interval)


def _day_params(date_str: str, cfg: dict) -> dict:
    day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=IST)
    return {
        "apikey":          cfg["api"]["key"],
        "ORGID":           cfg["api"]["org_id"],   # must be uppercase
        "report_type":     55,
        "organization_id": int(cfg["api"]["org_id"]),
        "start_time":      int(day.replace(hour=0,  minute=0,  second=0).timestamp()),
        "end_time":        int(day.replace(hour=23, minute=59, second=59).timestamp()),
        "response_type":   1,
    }


def fetch_one_day(
    date_str: str,
    session: requests.Session,
    cfg: dict,
    log: logging.Logger,
    dry_run: bool = False,
) -> pd.DataFrame:
    """
    Returns a DataFrame on success (possibly empty for 0-row days).
    Raises FatalAPIError for 401 / 403 / 404.
    Raises ValueError when all retries are exhausted.
    """
    if dry_run:
        rows = random.randint(0, 250)
        log.info(f"  [{date_str}] [DRY RUN] Simulated {rows} rows.")
        if rows == 0:
            return pd.DataFrame()
        return pd.DataFrame({
            "student_Id":              range(rows),
            "batch_Id":                [99999] * rows,
            "attendance_id":           [f"{date_str}-SIM"] * rows,
            "classDate":               ["01 Jan 2026"] * rows,
            "studentAttendanceStatus": ["P" if i % 3 else "A" for i in range(rows)],
            "batchName":               ["Simulation Batch"] * rows,
            "bundle_Id":               [0] * rows, "bundleName": ["SIM"] * rows,
            "course_Id":               [0] * rows, "courseName": ["SIM"] * rows,
            "teacher_Id":              [0] * rows, "teacherName": ["SIM"] * rows,
            "startTime":               ["7:00 AM"] * rows,
            "studentRating":           [0] * rows,
            "studentBatchStatus":      ["Active" if i % 5 else "Archived"
                                        for i in range(rows)],
        })

    api_cfg    = cfg["api"]
    max_r      = api_cfg["max_retries"]
    back_base  = api_cfg["retry_backoff_base_seconds"]
    back_max   = api_cfg["retry_backoff_max_seconds"]
    jitter_max = api_cfg["retry_jitter_seconds"]
    params     = _day_params(date_str, cfg)

    wait_reconnect = api_cfg.get("wait_for_reconnect", True)
    attempt = 0
    while attempt < max_r:
        attempt += 1
        try:
            resp = session.get(api_cfg["url"], params=params,
                               timeout=api_cfg["timeout_seconds"])

            # ── HTTP status ──────────────────────────────────────────────
            if resp.status_code == 200:
                pass  # continue to JSON parsing

            elif resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                if retry_after:
                    sleep_s = int(retry_after) + 2
                    log.warning(
                        f"  [{date_str}] 429 rate-limited. "
                        f"Retry-After={retry_after}s. Sleeping {sleep_s}s."
                    )
                else:
                    sleep_s = min(
                        back_base * (2 ** attempt) + random.uniform(0, jitter_max),
                        back_max,
                    )
                    log.warning(
                        f"  [{date_str}] 429 rate-limited (no header). "
                        f"Backoff {sleep_s:.1f}s (attempt {attempt}/{max_r})."
                    )
                time.sleep(sleep_s)
                continue

            elif resp.status_code == 401:
                msg = "HTTP 401 Unauthorised -- check edmingle.api_key and edmingle.organization_id in the shared credentials.yaml."
                log.critical(f"  [{date_str}] {msg}")
                raise FatalAPIError(msg)

            elif resp.status_code == 403:
                msg = "HTTP 403 Forbidden -- API key lacks permission for this endpoint."
                log.critical(f"  [{date_str}] {msg}")
                raise FatalAPIError(msg)

            elif resp.status_code == 404:
                msg = "HTTP 404 -- endpoint not found. Check api.url in config.yaml."
                log.critical(f"  [{date_str}] {msg}")
                raise FatalAPIError(msg)

            elif resp.status_code == 400:
                log.error(
                    f"  [{date_str}] HTTP 400 Bad Request. "
                    f"Response: {resp.text[:300]}. Skipping date."
                )
                return pd.DataFrame()   # bad params; skip, do not retry

            elif resp.status_code >= 500:
                sleep_s = min(
                    back_base * (2 ** attempt) + random.uniform(0, jitter_max), back_max
                )
                log.warning(
                    f"  [{date_str}] HTTP {resp.status_code} server error. "
                    f"Backoff {sleep_s:.1f}s (attempt {attempt}/{max_r})."
                )
                time.sleep(sleep_s)
                continue

            else:
                log.error(
                    f"  [{date_str}] Unexpected HTTP {resp.status_code} "
                    f"(attempt {attempt}/{max_r}). Body: {resp.text[:200]}"
                )
                time.sleep(back_base)
                continue

            # ── JSON parsing ─────────────────────────────────────────────
            try:
                data = resp.json()
            except ValueError:
                log.error(
                    f"  [{date_str}] Non-JSON response (attempt {attempt}/{max_r}): "
                    f"{resp.text[:200]}"
                )
                time.sleep(back_base)
                continue

            # ── Edmingle application-level error codes ────────────────────
            err_code = str(data.get("error_code") or data.get("code") or "")
            if err_code == "6001":
                log.error(
                    f"  [{date_str}] Edmingle 6001 (invalid params): "
                    f"{data.get('message', '')}. Skipping."
                )
                return pd.DataFrame()
            if err_code == "6002":
                msg = f"Edmingle 6002 (auth failure): {data.get('message', '')}"
                log.critical(f"  [{date_str}] {msg}")
                raise FatalAPIError(msg)

            if "data" not in data:
                log.error(
                    f"  [{date_str}] No 'data' key in response "
                    f"(attempt {attempt}/{max_r}): {str(data)[:300]}"
                )
                time.sleep(back_base)
                continue

            rows = data["data"]
            if not rows:
                log.info(f"  [{date_str}] 0 rows (quiet day / no sessions).")
                return pd.DataFrame()

            df = pd.DataFrame(rows)
            log.info(f"  [{date_str}] {len(df):,} rows fetched.")
            return df

        except FatalAPIError:
            raise
        except (requests.exceptions.Timeout,
                requests.exceptions.ConnectionError) as e:
            # ── Is this a real internet outage, or just a flaky call? ────
            if wait_reconnect and not is_online(cfg):
                # Internet is DOWN: this is not the API's fault and not
                # this date's fault. Wait for reconnect, then retry the
                # same date WITHOUT consuming a retry attempt.
                wait_for_connection(cfg, log, context=f"[{date_str}]")
                attempt -= 1
                continue
            # Internet is up but the call failed (transient blip or the
            # Edmingle server itself is unreachable) -> normal backoff,
            # attempt IS consumed, circuit breaker still protects us.
            kind    = "Timeout" if isinstance(e, requests.exceptions.Timeout) else "Connection error"
            sleep_s = min(back_base * (2 ** attempt) + random.uniform(0, jitter_max), back_max)
            err_msg = str(e).replace(str(api_cfg["key"]), "***APIKEY***")[:250]
            log.warning(
                f"  [{date_str}] {kind} while online (attempt {attempt}/{max_r}): "
                f"{err_msg}. Backoff {sleep_s:.1f}s."
            )
            time.sleep(sleep_s)
        except requests.exceptions.RequestException as e:
            log.error(f"  [{date_str}] Request exception: {e}. Skipping.")
            return pd.DataFrame()

    log.error(f"  [{date_str}] All {max_r} attempts exhausted.")
    raise ValueError(f"All retries exhausted for {date_str}")


def validate_api_connection(session: requests.Session, cfg: dict, log: logging.Logger):
    log.info("Validating API connection before main loop...")
    yesterday = (datetime.now(IST) - timedelta(days=1)).strftime("%Y-%m-%d")
    try:
        result = fetch_one_day(yesterday, session, cfg, log, dry_run=False)
        log.info(
            f"API validation OK -- "
            f"{'0 rows' if result.empty else f'{len(result):,} rows'} "
            f"for {yesterday}."
        )
    except FatalAPIError as e:
        raise PipelineError(f"API validation failed: {e}")


# ============================================================
# DATE HELPERS
# ============================================================

def build_date_list(args, cfg: dict) -> list:
    if args.date:
        return [args.date]
    if args.from_date and args.to_date:
        cur   = datetime.strptime(args.from_date, "%Y-%m-%d")
        end   = datetime.strptime(args.to_date,   "%Y-%m-%d")
        dates = []
        while cur <= end:
            dates.append(cur.strftime("%Y-%m-%d"))
            cur += timedelta(days=1)
        return dates
    days  = cfg["pipeline"]["default_lookback_days"]
    end   = (datetime.now(IST) - timedelta(days=1)).date()
    start = end - timedelta(days=days - 1)
    dates, d = [], start
    while d <= end:
        dates.append(d.strftime("%Y-%m-%d"))
        d += timedelta(days=1)
    return dates


# ============================================================
# PULL ORCHESTRATION
# ============================================================

def run_pull_loop(
    dates: list,
    session: requests.Session,
    cfg: dict,
    checkpoint: Checkpoint,
    emailer: EmailNotifier,
    log: logging.Logger,
    dry_run: bool,
) -> list:
    staging_dir    = Path(cfg["paths"]["staging_folder"])
    staging_dir.mkdir(parents=True, exist_ok=True)
    sleep_s        = cfg["api"]["rate_limit_sleep_seconds"]
    max_consec     = cfg["api"]["max_consecutive_errors"]

    consecutive = 0
    n_ok = n_fail = n_skip = n_resumed = 0
    successful_files = []

    log.info(
        f"{'[DRY RUN] ' if dry_run else ''}"
        f"Pull loop: {len(dates)} date(s), {dates[0]} to {dates[-1]}"
    )

    for i, date_str in enumerate(dates, start=1):

        # ── Already done (checkpoint) ────────────────────────────────────
        if checkpoint.is_done(date_str):
            staging_file = staging_dir / f"raw_{date_str}.csv"
            if staging_file.exists():
                successful_files.append(str(staging_file))
                n_resumed += 1
                log.debug(f"  [{date_str}] Checkpoint done. Skipping.")
            else:
                # File missing but checkpoint says done -- re-fetch
                log.warning(
                    f"  [{date_str}] Checkpoint says done but staging file missing. "
                    f"Re-fetching."
                )
                checkpoint.mark(date_str, "pending")
            continue

        # ── Progress every 25 dates ──────────────────────────────────────
        if (i - 1) % 25 == 0:
            pct = (i - 1) / len(dates) * 100
            log.info(
                f"Progress: {i-1}/{len(dates)} ({pct:.0f}%)  "
                f"ok={n_ok} fail={n_fail} skip={n_skip} resumed={n_resumed}"
            )

        # ── Fetch ────────────────────────────────────────────────────────
        try:
            df = fetch_one_day(date_str, session, cfg, log, dry_run=dry_run)

        except FatalAPIError as e:
            log.critical(f"Fatal API error: {e}. Run aborted.")
            emailer.send_critical(str(e), {
                "Date":     date_str,
                "Progress": f"{i-1}/{len(dates)} dates completed",
                "Action":   "Fix config.yaml and re-run (checkpoint is saved, run will resume).",
            })
            raise PipelineError(f"Fatal API error: {e}")

        except ValueError:
            # All retries exhausted
            consecutive += 1
            n_fail      += 1
            checkpoint.mark(date_str, Checkpoint.FAILED, error="All retries exhausted")
            log.error(
                f"  [{date_str}] Failed. Consecutive failures: "
                f"{consecutive}/{max_consec}."
            )
            emailer.send_warning(f"Date {date_str} failed after all retries", {
                "Consecutive failures": str(consecutive),
                "Max allowed":          str(max_consec),
            })
            if consecutive >= max_consec:
                msg = (
                    f"Circuit breaker: {consecutive} consecutive failures at {date_str}. "
                    f"Stopping pull."
                )
                log.critical(msg)
                emailer.send_critical(msg, {
                    "Last failed date": date_str,
                    "Progress":         f"{i-1}/{len(dates)} dates",
                    "Action":           "Fix the issue and re-run (checkpoint is saved).",
                })
                raise PipelineError(msg)

            if i < len(dates):
                time.sleep(sleep_s)
            continue

        # ── 0-row response (valid quiet day) ─────────────────────────────
        if df.empty:
            consecutive = 0   # a valid API response resets the consecutive counter
            n_skip     += 1
            checkpoint.mark(date_str, Checkpoint.SKIPPED, rows=0)
            if i < len(dates):
                time.sleep(sleep_s)
            continue

        # ── Save staging CSV ──────────────────────────────────────────────
        consecutive      = 0
        staging_file     = staging_dir / f"raw_{date_str}.csv"
        df.to_csv(staging_file, index=False, encoding="utf-8-sig")
        checkpoint.mark(date_str, Checkpoint.SUCCESS,
                        rows=len(df), file=str(staging_file))
        successful_files.append(str(staging_file))
        n_ok += 1

        if i < len(dates):
            time.sleep(sleep_s)

    log.info(
        f"Pull loop done -- ok={n_ok} fail={n_fail} skip={n_skip} "
        f"resumed={n_resumed} | total={len(dates)}"
    )
    return successful_files


# ============================================================
# COMBINE STAGING FILES
# ============================================================

def combine_staging_files(
    file_paths: list, cfg: dict, label: str, log: logging.Logger
) -> pd.DataFrame:
    if not file_paths:
        raise PipelineError("No staging files to combine.")

    log.info(f"Combining {len(file_paths)} staging file(s)...")
    frames = []
    for p in sorted(file_paths):
        try:
            frames.append(pd.read_csv(p, low_memory=False))
        except Exception as e:
            log.warning(f"Could not read {p}: {e}. Skipping.")

    if not frames:
        raise PipelineError("All staging files were unreadable.")

    combined = pd.concat(frames, ignore_index=True)
    log.info(f"Combined: {len(combined):,} rows from {len(frames)} file(s).")

    if cfg["pipeline"]["save_combined_raw_csv"]:
        ts       = datetime.now(IST).strftime("%Y%m%d_%H%M%S")
        out_path = (
            Path(cfg["paths"]["output_folder"])
            / f"attendance_raw_{label}_{ts}.csv"
        )
        combined.to_csv(out_path, index=False, encoding="utf-8-sig")
        log.info(f"Combined raw CSV saved: {out_path}")

    if cfg["pipeline"]["cleanup_staging_after_combine"]:
        removed = 0
        for p in file_paths:
            try:
                Path(p).unlink(); removed += 1
            except OSError:
                pass
        log.info(f"Removed {removed} staging file(s).")

    return combined


# ============================================================
# CLEAN + VALIDATE
# ============================================================

def resolve_session_id_column(df: pd.DataFrame, cfg: dict, log: logging.Logger) -> str:
    col = cfg["pipeline"]["session_id_column"]
    if col in df.columns:
        return col
    log.warning(
        f"'{col}' not found -- falling back to 'class_Id'. "
        f"class_Id is a subject/stream, NOT a session. "
        f"Session counts will be UNDERCOUNTED."
    )
    return "class_Id"


def filter_active_students(df: pd.DataFrame, cfg: dict, log: logging.Logger) -> pd.DataFrame:
    """
    Keep ONLY rows whose studentBatchStatus is in
    pipeline.active_status_values (default: ["Active"]).
    Everything else -- "Archived" or any other status -- is dropped.

    Uses an allow-list (keep Active) rather than a block-list (drop
    Archived) so any new/unexpected status value Edmingle introduces
    is excluded by default instead of silently counted.
    """
    pcfg = cfg["pipeline"]
    if not pcfg.get("exclude_inactive_students", True):
        log.info("Student status filter DISABLED (exclude_inactive_students: false). "
                 "All statuses counted, including Archived.")
        return df

    if "studentBatchStatus" not in df.columns:
        log.warning(
            "'studentBatchStatus' column not found in data -- "
            "cannot filter Archived students. ALL rows kept."
        )
        return df

    active_vals = pcfg.get("active_status_values", ["Active"])
    status      = df["studentBatchStatus"].astype(str).str.strip()
    keep_mask   = status.isin(active_vals)

    n_total   = len(df)
    n_dropped = int((~keep_mask).sum())

    if n_dropped:
        dropped_breakdown = (
            status[~keep_mask].value_counts().to_dict()
        )
        log.info(
            f"Student status filter: keeping only {active_vals}. "
            f"Dropped {n_dropped:,}/{n_total:,} row(s): {dropped_breakdown}"
        )
    else:
        log.info(f"Student status filter: all {n_total:,} rows already Active. "
                 f"Nothing dropped.")

    return df[keep_mask]


def clean_data(df: pd.DataFrame, session_col: str, cfg: dict, log: logging.Logger) -> pd.DataFrame:
    pcfg  = cfg["pipeline"]
    TODAY = pd.Timestamp(datetime.now(IST).date())
    df    = df.copy()

    df["classDate"] = pd.to_datetime(
        df["classDate"], format=pcfg["date_format"], errors="coerce"
    )
    bad = int(df["classDate"].isna().sum())
    if bad:
        log.warning(f"Dropping {bad} row(s) with unparseable classDate.")
    df = df.dropna(subset=["classDate"])

    before = len(df)
    df     = df.drop_duplicates()
    if (removed := before - len(df)):
        log.info(f"Removed {removed:,} exact duplicate row(s).")

    key_cols = ["batch_Id", "student_Id", session_col]
    missing  = df[key_cols].isna().any(axis=1)
    if missing.any():
        log.warning(f"Dropping {int(missing.sum()):,} row(s) missing key columns.")
        df = df[~missing]

    conflict = df.groupby(["student_Id", session_col]).size()
    if (n := int((conflict > 1).sum())):
        log.warning(
            f"{n} (student_Id, {session_col}) pairs have >1 row after dedup. "
            f"NOT auto-resolved -- all rows kept."
        )

    # ── ACTIVE-ONLY FILTER: drop Archived / non-active students ──────────
    df = filter_active_students(df, cfg, log)

    start_parsed      = pd.to_datetime(df["startTime"], format=pcfg["time_format"], errors="coerce")
    seconds           = start_parsed.dt.hour.fillna(0) * 3600 + start_parsed.dt.minute.fillna(0) * 60
    df["_class_datetime"] = df["classDate"] + pd.to_timedelta(seconds, unit="s")

    log.info(f"Clean complete: {len(df):,} rows ready for summarisation.")
    return df.reset_index(drop=True)


def validate_present_value(df: pd.DataFrame, cfg: dict):
    pv       = cfg["pipeline"]["present_value"]
    observed = sorted(df["studentAttendanceStatus"].dropna().unique().tolist())
    if pv not in observed:
        raise PipelineError(
            f"PRESENT_VALUE={pv!r} not in studentAttendanceStatus. "
            f"Observed: {observed}. "
            f"Every attendance metric would be 0. "
            f"Fix pipeline.present_value in config.yaml."
        )


# ============================================================
# BATCH SUMMARY COMPUTATION
# ============================================================

def build_class_summary(df: pd.DataFrame, session_col: str, cfg: dict) -> pd.DataFrame:
    TODAY = pd.Timestamp(datetime.now(IST).date())
    pv    = cfg["pipeline"]["present_value"]
    av    = cfg["pipeline"].get("absent_value", "A")
    lv    = cfg["pipeline"].get("late_value", "L")
    df    = df.copy()
    df["is_present"]       = df["studentAttendanceStatus"] == pv
    df["_present_student"] = df["student_Id"].where(df["is_present"])
    df["_absent_student"]  = df["student_Id"].where(df["studentAttendanceStatus"] == av)
    df["_late_student"]    = df["student_Id"].where(df["studentAttendanceStatus"] == lv)
    # "marked" = any real status (excludes the "-" not-marked placeholder)
    df["_marked_student"]  = df["student_Id"].where(
        df["studentAttendanceStatus"].isin([pv, av, lv, "E", "OL", "NA"])
    )

    cs = (
        df.groupby(["batch_Id", session_col])
          .agg(classDate=("classDate", "first"),
               _class_datetime=("_class_datetime", "min"),
               batchName=("batchName", "first"),
               course_Id=("course_Id", "first"),
               courseName=("courseName", "first"),
               present_count=("_present_student", "nunique"),
               absent_count=("_absent_student", "nunique"),
               late_count=("_late_student", "nunique"),
               total_marked=("_marked_student", "nunique"))
          .reset_index()
    )
    cs["is_conducted"] = cs["classDate"] <= TODAY
    cs = cs.sort_values(["batch_Id", "_class_datetime", session_col]).reset_index(drop=True)
    cs["session_number"] = cs.groupby("batch_Id").cumcount() + 1
    cs["session_attendance_percentage"] = (
        (cs["present_count"] / cs["total_marked"].replace(0, pd.NA) * 100)
        .astype("Float64").round(2)
    )
    return cs


def compute_batch_summary(df: pd.DataFrame, session_col: str, cfg: dict) -> pd.DataFrame:
    pv = cfg["pipeline"]["present_value"]
    df = df.copy()
    df["is_present"] = df["studentAttendanceStatus"] == pv

    cs        = build_class_summary(df, session_col, cfg)
    conducted = cs[cs["is_conducted"]]

    basic = ["batchName", "bundle_Id", "bundleName", "course_Id",
             "courseName", "teacher_Id", "teacherName"]
    summary = df.groupby("batch_Id")[basic].first()

    summary["total_students_enrolled"] = df.groupby("batch_Id")["student_Id"].nunique()
    summary["total_classes_planned"]   = cs.groupby("batch_Id")[session_col].nunique()
    summary["total_classes_conducted"] = (
        conducted.groupby("batch_Id")[session_col].nunique()
        .reindex(summary.index, fill_value=0)
    )
    summary["total_classes_remaining"] = (
        summary["total_classes_planned"] - summary["total_classes_conducted"]
    ).clip(lower=0)

    # Attendance-mark totals across all conducted sessions of the batch.
    # (Counts marks, not unique students: one student present in 30
    # sessions contributes 30 to total_present_marks.)
    summary["total_present_marks"] = (
        conducted.groupby("batch_Id")["present_count"].sum()
        .reindex(summary.index, fill_value=0)
    )
    summary["total_absent_marks"] = (
        conducted.groupby("batch_Id")["absent_count"].sum()
        .reindex(summary.index, fill_value=0)
    )

    summary["first_class_date"] = cs.groupby("batch_Id")["classDate"].min()
    summary["last_class_date"]  = (
        conducted.groupby("batch_Id")["classDate"].max().reindex(summary.index)
    )

    summary["first_class_attendance"] = (
        cs.groupby("batch_Id").first()["present_count"].reindex(summary.index, fill_value=0)
    )

    last_att = conducted.groupby("batch_Id").last()["present_count"].reindex(summary.index)
    summary["last_class_attendance"] = last_att
    has_c = summary["last_class_date"].notna()
    summary.loc[has_c & summary["last_class_attendance"].isna(), "last_class_attendance"] = 0

    avg_p = cs.groupby("batch_Id")["present_count"].mean()
    summary["attendance_percentage"]    = (avg_p / summary["total_students_enrolled"] * 100).round(2)

    cg = conducted.groupby("batch_Id")["present_count"]
    summary["average_class_attendance"] = cg.mean().round(2).reindex(summary.index)
    summary["highest_class_attendance"] = cg.max().reindex(summary.index)
    summary["lowest_class_attendance"]  = cg.min().reindex(summary.index)

    rating = pd.to_numeric(df["studentRating"], errors="coerce")
    if cfg["pipeline"]["treat_zero_rating_as_missing"]:
        rating = rating.replace(0, float("nan"))
    df["_rating"] = rating
    summary["average_rating"] = df.groupby("batch_Id")["_rating"].mean().round(2)

    summary["retention_percentage"] = (
        summary["last_class_attendance"] / summary["first_class_attendance"] * 100
    ).replace([float("inf"), float("-inf")], float("nan")).round(2)
    summary["attendance_drop"] = summary["first_class_attendance"] - summary["last_class_attendance"]

    summary = summary.reset_index()
    for col in ["total_classes_conducted", "total_classes_remaining",
                "total_present_marks", "total_absent_marks",
                "first_class_attendance", "last_class_attendance",
                "highest_class_attendance", "lowest_class_attendance", "attendance_drop"]:
        summary[col] = summary[col].astype("Int64")

    return summary[OUTPUT_COLUMNS]


def build_session_wise_output(df: pd.DataFrame, session_col: str, cfg: dict) -> pd.DataFrame:
    """One row per (batch, session): who was present/absent/late in every
    class that has happened, in chronological order per batch."""
    cs = build_class_summary(df, session_col, cfg)
    out = cs.rename(columns={session_col: "session_id"}).copy()
    keep = SESSION_OUTPUT_COLUMNS.copy()
    keep.insert(keep.index("session_number") + 1, "session_id")
    return out[keep]


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(
        description="Production-grade Edmingle report_type=55 attendance pipeline"
    )
    parser.add_argument("--config",           default="config.yaml")
    parser.add_argument("--date",             type=str)
    parser.add_argument("--from",             dest="from_date", type=str)
    parser.add_argument("--to",               dest="to_date",   type=str)
    parser.add_argument("--from-file",        dest="from_file", type=str)
    parser.add_argument("--dry-run",          action="store_true")
    parser.add_argument("--retry-failed",     action="store_true")
    parser.add_argument("--reset-checkpoint", action="store_true")
    parser.add_argument("--verbose",          action="store_true")
    args = parser.parse_args()

    cfg     = load_config(args.config)
    log     = setup_logging(cfg, verbose=args.verbose)
    emailer = EmailNotifier(cfg, log)
    lock    = LockFile(cfg["paths"]["lock_file"], log)

    log.info("=" * 60)
    log.info(f"Edmingle Attendance Pipeline v{VERSION}")
    log.info(f"Config : {args.config}")
    if cfg["pipeline"].get("exclude_inactive_students", True):
        log.info(f"Filter : ACTIVE students only "
                 f"(keeping {cfg['pipeline'].get('active_status_values', ['Active'])})")
    else:
        log.info("Filter : DISABLED -- all student statuses counted")
    if args.dry_run:
        log.info("MODE   : DRY RUN -- no real API calls")
    log.info("=" * 60)

    try:
        lock.acquire()
    except PipelineError as e:
        log.critical(str(e))
        sys.exit(1)

    def _graceful_exit(sig, _frame):
        log.warning(
            f"Received {signal.Signals(sig).name}. "
            f"Checkpoint saved -- re-run to resume."
        )
        lock.release()
        sys.exit(0)

    signal.signal(signal.SIGINT,  _graceful_exit)
    signal.signal(signal.SIGTERM, _graceful_exit)

    chk = Checkpoint(cfg["paths"]["checkpoint_file"], log)
    if args.reset_checkpoint:
        chk.reset()
    else:
        chk.load()

    run_start = datetime.now(IST)
    exit_code = 0

    try:
        Path(cfg["paths"]["output_folder"]).mkdir(parents=True, exist_ok=True)

        # ── Offline mode ─────────────────────────────────────────────────
        if args.from_file:
            log.info(f"--from-file: loading {args.from_file}")
            raw_df = pd.read_csv(args.from_file, low_memory=False)
            label  = Path(args.from_file).stem

        # ── Online mode ──────────────────────────────────────────────────
        else:
            check_disk_space(cfg, log)
            session = build_api_session()

            # If launched while offline (e.g. auto-started at boot before
            # WiFi connects), wait for internet instead of failing.
            if (not args.dry_run
                    and cfg["api"].get("wait_for_reconnect", True)
                    and not is_online(cfg)):
                wait_for_connection(cfg, log, context="startup")

            if not args.dry_run and cfg["api"].get("validate_on_startup", True):
                try:
                    validate_api_connection(session, cfg, log)
                except PipelineError as e:
                    log.critical(str(e))
                    emailer.send_critical(str(e), {"Stage": "Startup API validation"})
                    lock.release()
                    sys.exit(1)

            if args.retry_failed:
                dates = chk.failed_dates()
                if not dates:
                    log.info("No failed dates in checkpoint. Nothing to retry.")
                    lock.release()
                    sys.exit(0)
                log.info(f"--retry-failed: {len(dates)} date(s) to retry.")
                for d in dates:
                    chk.mark(d, "pending")
            else:
                dates = build_date_list(args, cfg)

            label = dates[0] if len(dates) == 1 else f"{dates[0]}_to_{dates[-1]}"

            staging_files = run_pull_loop(
                dates, session, cfg, chk, emailer, log, dry_run=args.dry_run
            )
            if not staging_files:
                log.warning("No data fetched for any date. Nothing to summarise.")
                lock.release()
                sys.exit(0)

            raw_df = combine_staging_files(staging_files, cfg, label, log)

        # ── Summary ──────────────────────────────────────────────────────
        log.info("Running batch attendance summary...")
        session_col = resolve_session_id_column(raw_df, cfg, log)
        clean       = clean_data(raw_df, session_col, cfg, log)
        validate_present_value(clean, cfg)

        summary = compute_batch_summary(clean, session_col, cfg)
        summary = summary.sort_values("batchName").reset_index(drop=True)

        ts           = datetime.now(IST).strftime("%Y%m%d_%H%M%S")
        summary_path = (
            Path(cfg["paths"]["output_folder"])
            / f"batch_attendance_summary_{label}_{ts}.csv"
        )
        summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
        log.info(f"Summary: {len(summary):,} batches -- {summary_path}")

        session_path = None
        if cfg["pipeline"].get("write_session_wise_csv", True):
            session_df = build_session_wise_output(clean, session_col, cfg)
            session_df = session_df.sort_values(
                ["batchName", "session_number"]
            ).reset_index(drop=True)
            session_path = (
                Path(cfg["paths"]["output_folder"])
                / f"session_wise_attendance_{label}_{ts}.csv"
            )
            session_df.to_csv(session_path, index=False, encoding="utf-8-sig")
            log.info(
                f"Session-wise: {len(session_df):,} sessions across "
                f"{session_df['batch_Id'].nunique():,} batches -- {session_path}"
            )

        # ── Completion ────────────────────────────────────────────────────
        elapsed = (datetime.now(IST) - run_start).total_seconds()
        stats = {
            "Date range":          label,
            "Student filter":      ("Active only"
                                    if cfg["pipeline"].get("exclude_inactive_students", True)
                                    else "All statuses"),
            "Batches in summary":  str(len(summary)),
            "Total raw rows":      f"{len(raw_df):,}",
            "Summary file":        str(summary_path),
            "Session-wise file":   str(session_path) if session_path else "disabled",
            "Elapsed":             f"{elapsed / 60:.1f} minutes",
            "Completed at":        datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        }
        for k, v in stats.items():
            log.info(f"  {k}: {v}")

        emailer.send_completion(stats)
        log.info("Pipeline complete.")

    except PipelineError as e:
        log.critical(f"Pipeline error: {e}")
        emailer.send_critical(str(e), {
            "Elapsed": f"{(datetime.now(IST) - run_start).total_seconds() / 60:.1f} min"
        })
        exit_code = 1

    except Exception as e:
        tb = traceback.format_exc()
        log.critical(f"Unexpected error: {e}\n{tb}")
        emailer.send_critical(f"Unexpected error: {e}", {
            "Traceback": f"<pre style='font-size:12px'>{tb[:2000]}</pre>",
            "Elapsed":   f"{(datetime.now(IST) - run_start).total_seconds() / 60:.1f} min",
        })
        exit_code = 1

    finally:
        lock.release()

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
