#!/usr/bin/env python3
# Vyoma Samskrta Pathasala — Edmingle Data Pipeline
# Script    : edmingle_student_course_sync.py
# Purpose   : Fetch all student profiles and course enrollments from Edmingle API
# Author    : Shankararama Sharma (core logic) | Shubham (improvements)
# Modified  : June 2026
# Run time  : ~68-80 hours for full run of 122,000+ students
# Resume    : Automatically resumes from last checkpoint if interrupted


# =============================================================================
# SECTION 1 — IMPORTS
# Standard library + third-party dependencies
# =============================================================================

from __future__ import annotations

# Standard library — file, system, network, data handling
import argparse  # parse command line arguments (--config flag)
import csv  # read and write CSV files
import json  # read and write JSON config and state files
import logging  # structured logging to file and console
import os  # file system operations (fsync, replace)
import re  # regex for parsing legacy page number file
import shutil  # disk usage check + file copy
import sys  # exit codes and Python version info
import time  # sleep between API calls and timing
import uuid  # generate unique temp filenames for atomic writes
from collections.abc import Iterable  # type hints
from datetime import datetime  # timestamps for state and logging
from pathlib import Path  # cross-platform file paths
from typing import Any

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set
# before any local module import below, so this and every module it pulls
# in gets compiled into one shared location instead of a scripts/__pycache__
# folder per pipeline.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

# Shared helpers (credentials/notifications loading, email sending, the
# rolling-window rate limiter, and crash-safe atomic write helpers) now
# live in ela_datasets/common.py, two directories up from this script
# (scripts/ -> ela_mis_datasets/ -> ela_datasets/).
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
# Third-party — must be installed via: pip install requests
import requests

import common
from common import (
    RollingRateLimiter,
    atomic_write_csv,
    atomic_write_json,
    format_duration,
    read_csv_rows,
    utc_now,
)

# =============================================================================
# SECTION 1A — SCRIPT DIRECTORY ANCHOR
# All output/state/log files must resolve relative to THIS script's own
# folder, not the process's current working directory (cwd). Without this,
# running the script from a different cwd (e.g. `python /full/path/to/
# edmingle_student_course_sync.py` while sitting in an unrelated directory)
# would scatter its CSV/JSON/log files into whatever folder happened to be
# the caller's cwd instead of ela_mis_datasets/.
# =============================================================================

SCRIPT_DIR = Path(__file__).resolve().parent

# All generated CSV/state/log/legacy files now live in output/, a sibling
# of this script's own scripts/ folder, rather than alongside the script.
OUTPUT_DIR = (SCRIPT_DIR / ".." / "output").resolve()


# =============================================================================
# SECTION 1B — SHARED CREDENTIAL / NOTIFICATION LOADERS
# credentials.yaml (one level up, shared by every pipeline in ela_datasets/)
# holds the Edmingle api_key/organization_id.
# notifications.yaml (same directory as this script) holds this pipeline's
# own SMTP/email alert settings.
# =============================================================================

def _load_credentials(base_dir: Path) -> dict[str, Any]:
    # Reads the shared credentials.yaml one directory above this pipeline.
    # Delegates the actual file read to common.load_credentials(), which
    # raises FileNotFoundError / yaml errors as-is on a missing/malformed
    # file -- same "never silently proceed" behavior this always had.
    credentials_path = (base_dir / ".." / ".." / "credentials.yaml").resolve()
    return common.load_credentials(credentials_path)


def _load_notifications_config() -> dict[str, Any]:
    # Reads this pipeline's notifications config from the repo-wide
    # notifications/ folder (2026-09-25), not this pipeline's own scripts/
    # folder. A missing file was always a hard failure here (previously an
    # unhandled FileNotFoundError from open()) -- preserved explicitly, since
    # common.load_notifications() on its own treats a missing file as
    # "notifications disabled" (returns {}), which is not this script's
    # existing behavior.
    notifications_path = common.REPO_ROOT / "notifications" / "ela_mis_datasets.yaml"
    if not notifications_path.exists():
        raise FileNotFoundError(f"Notifications file not found: {notifications_path}")
    return common.load_notifications("ela_mis_datasets")


_notifications_config = _load_notifications_config()
_email_config = (_notifications_config.get("channels", {}) or {}).get("email", {}) or {}
_smtp_config = _email_config.get("smtp", {}) or {}


# =============================================================================
# SECTION 2 — ALERT CONFIGURATION
# Email credentials/SMTP mechanics for failure and completion notifications
# now live in common.send_mail() (see send_email_alert below), which reads
# them straight from _notifications_config -- no separate module-level
# ALERT_EMAIL_*/SMTP_HOST/SMTP_PORT constants needed here any more.
# =============================================================================

# How often (in seconds) to send a periodic status-update email during the
# course pull, expressed in notifications.yaml as hours (default 6 hours =
# 21600 seconds — the value this replaced when it was a hardcoded constant).
# Loaded from notifications.yaml -> channels.email.status_update_interval_hours
STATUS_UPDATE_INTERVAL = int(
    float(_email_config.get("status_update_interval_hours", 6)) * 3600
)


# =============================================================================
# SECTION 3 — FIELD DEFINITIONS
# Column names for the two output CSV files
# These must exactly match what the Edmingle API returns
# =============================================================================

# Columns saved to edmingle_students.csv — one row per student
STUDENT_FIELDS = [
    "contact_number",               # primary phone number
    "contact_number_2",             # secondary phone number
    "contact_number_2_country_id",  # country code for secondary number
    "contact_number_2_dial_code",   # dial code for secondary number
    "contact_number_country_id",    # country code for primary number
    "contact_number_dial_code",     # dial code for primary number
    "date",                         # registration date DD/MM/YYYY
    "email",                        # student email address
    "formatted_date",               # human readable e.g. "7th Jun 2020"
    "is_archived",                  # 0 = active, 1 = archived
    "name",                         # full name
    "parent_contact_number",        # parent or guardian phone
    "parent_contact_number_country_id",
    "parent_contact_number_dial_code",
    "parent_email",                 # parent or guardian email
    "parent_name",                  # parent or guardian full name
    "registration_number",          # Vyoma internal ID e.g. "30301"
    "role",                         # 1 = student
    "status",                       # account status code
    "time",                         # registration time
    "user_id",                      # Edmingle unique user identifier
    "user_username",                # login username (native field -- already
                                     # covers what a "UserName" custom-field
                                     # column would duplicate; see removed
                                     # column note below)
    "PhoneNumber",                  # custom field "phone_number_text" (International Phone Number)
    "Age",                          # custom field "age_dropdown" (the only age field
                                     # actually visible/mandatory on the live signup
                                     # form -- "age" and "user_age" are legacy fields
                                     # hidden from every form, confirmed via a live
                                     # API sample on 2026-09-23)
    "LastName",                     # custom field "user_last_name" (User Last Name)
    # "UserName" removed 2026-09-23: was read from customfield_data by list
    # POSITION (index 0), not by name -- wrong for the vast majority of
    # students since the list's order/length varies per student (confirmed:
    # was >99% blank/garbage). The native "user_username" column above
    # already gives every student's actual username (100% populated),
    # making a separate custom-field-based "UserName" column redundant even
    # once fixed to look up by name instead of position.
]

# Columns saved to edmingle_course_enrollments.csv — one row per class session per student
COURSE_FIELDS = [
    "user_id",                  # Edmingle unique user identifier
    "name",                     # student full name
    "email",                    # student email
    "class_id",                 # individual class session ID
    "class_name",               # class session name
    "tutor_name",               # instructor name
    "total_classes",            # total sessions scheduled in this batch
    "present",                  # sessions this student attended
    "absent",                   # sessions this student missed
    "late",                     # sessions attended late
    "excused",                  # sessions marked excused
    "start_date",               # batch start date as unix timestamp
    "end_date",                 # batch end date as unix timestamp
    "master_batch_id",          # parent course/batch ID
    "master_batch_name",        # parent course/batch name
    "classusers_start_date",    # student enrollment date as unix timestamp
    "classusers_end_date",      # student enrollment end as unix timestamp
    "batch_status",             # 0=active 1=archived 3=completed
    "cu_status",                # 1=enrolled 2=cancelled
    "cu_state",                 # enrollment state code
    "institution_bundle_id",    # Vyoma internal bundle reference
    "archived_at",              # archive timestamp — 0 means not archived
    "bundle_id",                # course bundle identifier
]


# =============================================================================
# SECTION 4 — FILE AND API CONSTANTS
# Output filenames and Edmingle API endpoint URLs
# =============================================================================

# Default output file names — all saved in same folder as this script
DEFAULT_FILES = {
    "student_master":        "edmingle_students.csv",                       # final student output
    "course_master":         "edmingle_course_enrollments.csv",             # final course output
    "state":                 "edmingle_sync_state.json",                    # checkpoint state
    "log":                   "edmingle_sync.log",                           # run log
    "course_progress":       "edmingle_course_enrollments.in_progress.csv", # temp during run
    "course_snapshot":       "edmingle_course_students_snapshot.csv",       # student list snapshot
    "legacy_student_master": "students_data_2.csv",                        # old format — auto migrated
    "legacy_course_master":  "studentCoursesEnrolled.csv",                 # old format — auto migrated
    "legacy_page_state":     "PageNo.txt",                                 # old page number tracker
}

# Edmingle API endpoints for Vyoma organisation (org ID 683)
STUDENTS_URL = "https://vyoma-api.edmingle.com/nuSource/api/v1/organization/students"
COURSES_URL  = "https://vyoma-api.edmingle.com/nuSource/api/v1/admin/classes/attendance"

# HTTP codes where retrying will never succeed — raise error immediately
PERMANENT_HTTP_STATUSES = {400, 401, 403, 404}

# HTTP codes that indicate a temporary issue — wait and retry
TRANSIENT_HTTP_STATUSES = {408, 429}


# =============================================================================
# SECTION 5 — EMAIL ALERT FUNCTION  [ADDED BY SHUBHAM]
# Sends notification email on script failure or completion via
# common.send_mail() -- skips silently (logs a warning) if email is
# disabled or SMTP config is incomplete in notifications.yaml
# =============================================================================

def send_email_alert(subject: str, body: str) -> None:
    # Connect-and-send mechanics now delegate to common.send_mail(), which
    # is a drop-in replacement for the previous smtplib/EmailMessage logic
    # here: it also treats the from-address as the SMTP login username (no
    # separate username field in this pipeline's notifications.yaml), and
    # accepts to_addresses as a comma-separated string (as stored in
    # _email_config) or a list. It never raises -- logs a warning and
    # returns False instead -- matching this function's original
    # "never let email failure crash the main pipeline" behavior. Uses the
    # notifications.yaml already loaded once at import time (module-level
    # _notifications_config) so every existing call site (startup checks,
    # error handlers, completion, mid-run status, etc.) needs no changes.
    logger = logging.getLogger("edmingle_sync")
    common.send_mail(_notifications_config, subject, body, logger)


# =============================================================================
# SECTION 6 — STARTUP CHECKS  [ADDED BY SHUBHAM]
# Validates environment before starting the 80-hour run
# Checks 3 things: Python version, disk space, API key validity
# Sends email and exits immediately if any check fails
# =============================================================================

def run_startup_checks(config: dict[str, Any]) -> None:
    print("=" * 55)
    print("  VYOMA EDMINGLE SYNC — STARTUP CHECKS")
    print("=" * 55)

    # Disk space must be at least 2 GB free
    # Checked against SCRIPT_DIR (not cwd) since that's where the multi-GB
    # output CSVs actually get written
    free_bytes = shutil.disk_usage(SCRIPT_DIR).free
    free_gb    = free_bytes / (1024 ** 3)
    print(f"  Free disk space  : {free_gb:.1f} GB")
    if free_gb < 2.0:
        msg = f"Only {free_gb:.1f} GB free. Need at least 2 GB. Free up space before running."
        print(f"  FAIL — {msg}")
        send_email_alert(
            "[Vyoma Pipeline] STARTUP FAILED — Not enough disk space",
            f"Script failed startup check.\n\n{msg}"
        )
        sys.exit(1)
    print("  PASS — Disk space OK")

    # Check 3 — API key must be valid before starting 80-hour run
    api_key = str(config.get("api_key", ""))
    org_id  = str(config.get("organization_id", ""))
    print(f"  API key (last 8) : ...{api_key[-8:]}")
    print(f"  Organisation ID  : {org_id}")
    try:
        # Fetch just 1 student as a lightweight key validation test
        resp = requests.get(
            STUDENTS_URL,
            params={
                "apikey":          api_key,
                "ORGID":           org_id,
                "organization_id": org_id,
                "per_page":        1,
                "page":            1,
            },
            timeout=15,
        )
        if resp.status_code == 200:
            print("  PASS — API key is valid")
        elif resp.status_code in (400, 401, 403):
            # Key is expired or incorrect — stop now rather than 80 hours later
            msg = (
                f"API key invalid or expired. HTTP {resp.status_code}. "
                f"Get a fresh key from Shankar and update edmingle_sync_config.json."
            )
            print(f"  FAIL — {msg}")
            send_email_alert(
                "[Vyoma Pipeline] STARTUP FAILED — API key expired",
                f"Script failed startup check.\n\n{msg}"
            )
            sys.exit(1)
        else:
            # Unexpected response — warn but do not block the run
            print(f"  WARN — API returned HTTP {resp.status_code} — proceeding anyway")
    except Exception as e:
        # Network issue during check — warn but do not block the run
        print(f"  WARN — Could not validate API key ({e}) — proceeding anyway")

    # All checks passed
    print("=" * 55)
    print("  All startup checks passed. Starting sync...")
    print("=" * 55)
    print()


# =============================================================================
# SECTION 7 — STARTUP SUMMARY  [ADDED BY SHUBHAM]
# Logs key run info to the log file before the pipeline starts
# Shows rate, disk space, resume point, and estimated completion time
# =============================================================================

def print_startup_summary(config: dict[str, Any], state: dict[str, Any]) -> None:
    # Read current state values
    last_page          = state.get("last_completed_student_page", 0)
    course_in_progress = state.get("course_refresh", {}).get("in_progress", False)
    total_students     = state.get("course_refresh", {}).get("total_students", "unknown")
    next_index         = state.get("course_refresh", {}).get("next_student_index", 0)
    rate               = int(config.get("max_calls_per_minute", 30))
    free_gb            = shutil.disk_usage(SCRIPT_DIR).free / (1024 ** 3)

    logger = logging.getLogger("edmingle_sync")
    logger.info("=" * 50)
    logger.info("RUN SUMMARY")
    logger.info("  Rate limit         : %d calls/min", rate)
    logger.info("  Free disk space    : %.1f GB", free_gb)
    logger.info("  Last student page  : %d", last_page)
    logger.info("  Course in progress : %s", course_in_progress)

    if course_in_progress:
        # Resuming a previous run — show exact resume point
        logger.info("  Resuming from      : student %d of %s", next_index, total_students)
        if isinstance(total_students, int) and total_students > 0:
            remaining = total_students - next_index
            est_min   = remaining / rate
            logger.info(
                "  Est. remaining     : %d students (~%.0f min / %.1f hrs)",
                remaining, est_min, est_min / 60
            )
    else:
        # Fresh full run — estimate total duration
        est_min = 122000 / rate
        logger.info(
            "  Est. total time    : ~%.0f min / %.1f hrs for full run",
            est_min, est_min / 60
        )

    logger.info("=" * 50)


# =============================================================================
# SECTION 8 — CUSTOM EXCEPTION
# Raised when an API error means retrying will never help
# =============================================================================

class PermanentAPIError(RuntimeError):
    # Used for HTTP 401, 403, 404 — no point retrying these
    pass


# =============================================================================
# SECTION 9 — UTILITY FUNCTIONS
# Small helpers used throughout the script
#
# utc_now, format_duration, atomic_write_json, atomic_write_csv, and
# read_csv_rows used to be defined here but were byte-for-byte identical
# (temp-file + fsync + os.replace mechanics, same ISO-8601 timestamp format,
# same duration breakdown) to the versions now consolidated in the shared
# ela_datasets/common.py, so they are imported from there instead (see the
# `from common import ...` line near the top of this file). Only
# atomic_copy, parse_legacy_page, calculate_start_page, merge_students, and
# extract_student remain here -- they are specific to this pipeline's
# legacy-migration and student/course business logic.
# =============================================================================

def atomic_copy(source: Path, destination: Path) -> None:
    # Copies a file atomically — used for legacy file migration
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{uuid.uuid4().hex}.tmp")
    try:
        shutil.copyfile(source, temporary)
        with temporary.open("rb") as handle:
            os.fsync(handle.fileno())
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)


def parse_legacy_page(path: Path) -> int:
    # Reads old PageNo.txt to find where the previous script run stopped
    if not path.exists():
        return 1
    match = re.search(r"\d+", path.read_text(encoding="utf-8"))
    return max(1, int(match.group())) if match else 1


def calculate_start_page(last_completed_page: int, overlap_pages: int) -> int:
    # Go back N pages from last run to catch students who registered during that run
    return max(1, last_completed_page - overlap_pages)


def merge_students(
    existing_rows: Iterable[dict[str, Any]],
    fetched_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    # Combines existing and newly fetched students
    # Deduplicates by user_id — fetched rows overwrite existing for same user
    by_user_id: dict[str, dict[str, Any]] = {}
    for row in existing_rows:
        user_id = str(row.get("user_id", "")).strip()
        if user_id:
            by_user_id[user_id] = {field: row.get(field, "") for field in STUDENT_FIELDS}
    for row in fetched_rows:
        user_id = str(row.get("user_id", "")).strip()
        if user_id:
            by_user_id[user_id] = {field: row.get(field, "") for field in STUDENT_FIELDS}
    return list(by_user_id.values())


def extract_student(student: dict[str, Any]) -> dict[str, Any]:
    # Flattens one student API response into a flat dict matching STUDENT_FIELDS
    row = {field: student.get(field, "") for field in STUDENT_FIELDS}
    custom_fields = student.get("customfield_data")
    if isinstance(custom_fields, list):
        # customfield_data is a variable-length list whose order and length
        # differ per student (each entry only appears if that student's
        # organization form config includes it and, for some fields, only
        # if the student filled it in) -- there is no stable position to
        # index into. Build a name -> value lookup instead and match on
        # each field's own "field_name" key, confirmed via a live API
        # sample on 2026-09-23 (see git history for the raw sample).
        by_name = {
            cf.get("field_name"): cf.get("field_value", "")
            for cf in custom_fields
            if isinstance(cf, dict)
        }
        name_mapping = {
            "PhoneNumber": "phone_number_text",
            "Age": "age_dropdown",
            "LastName": "user_last_name",
        }
        for field, source_name in name_mapping.items():
            if source_name in by_name:
                row[field] = by_name[source_name]
    return row


# =============================================================================
# SECTION 10 — RATE LIMITER
# Ensures script stays within max_calls_per_minute from config
# Uses rolling window — more precise than fixed sleep between calls
#
# The RollingRateLimiter class used to be defined here (deque-based rolling
# window, with an injectable clock=/sleep= pair that nothing in this repo
# ever overrode) but was byte-for-byte identical in behavior to the shared
# version consolidated into ela_datasets/common.py (minus the unused
# clock=/sleep= injection points, which common.py drops in favor of
# time.monotonic/time.sleep directly) -- it is imported from there instead
# (see the `from common import RollingRateLimiter` line near the top of
# this file).
# =============================================================================


# =============================================================================
# SECTION 11 — MAIN SYNC CLASS
# Core pipeline — student data fetch and course enrollment fetch
# Original logic by Shankararama Sharma — only run() method was modified
# =============================================================================

class EdmingleSync:

    def __init__(
        self,
        config_path: Path,
        session: requests.Session | Any | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        # Previously accepted injectable `sleep=`/`clock=` params (separate
        # from the rate limiter's own, now-removed clock=/sleep= pair) for
        # test seams. Confirmed via a repo-wide grep that nothing anywhere
        # (this file's own single call site in main(), and no test in the
        # repo) ever constructs EdmingleSync with non-default sleep/clock
        # arguments, so they're removed here too -- internal use now calls
        # time.sleep()/time.monotonic() directly (see request_json below).
        self.config_path = config_path.resolve()
        self.base_dir    = self.config_path.parent
        self.config      = self._load_config()
        # Build full file paths anchored to SCRIPT_DIR (this script's own
        # folder) — not the cwd the process happened to be launched from —
        # so output/state/log files always land in ela_mis_datasets/
        # regardless of how/from-where the script was invoked.
        self.paths = {
            name: OUTPUT_DIR / self.config.get("files", {}).get(name, default)
            for name, default in DEFAULT_FILES.items()
        }
        self.logger       = logger or configure_logging(self.paths["log"])
        self.session      = session or requests.Session()
        self.rate_limiter = RollingRateLimiter(
            int(self.config["max_calls_per_minute"]),
            window_seconds=60.0,
            logger=self.logger,
        )
        # Auth headers sent with every Edmingle API request
        self.headers = {
            "apikey": str(self.config["api_key"]),
            "ORGID":  str(self.config["organization_id"]),
        }

    def _load_config(self) -> dict[str, Any]:
        # Reads edmingle_sync_config.json and validates all required keys exist
        with self.config_path.open("r", encoding="utf-8") as handle:
            config = json.load(handle)
        # api_key / organization_id now live in the shared credentials.yaml
        # (one directory up), not in this pipeline's own JSON config
        creds = _load_credentials(self.base_dir)
        config["api_key"] = creds.get("api_key", "")
        config["organization_id"] = creds.get("organization_id", "")
        required = [
            "api_key", "organization_id", "overlap_pages",
            "students_per_page", "max_calls_per_minute",
            "request_timeout_seconds", "initial_retry_delay_seconds",
            "maximum_retry_delay_seconds", "rate_limit_block_seconds",
        ]
        missing = [key for key in required if key not in config]
        if missing:
            raise ValueError(f"Missing configuration values: {', '.join(missing)}")
        return config

    def load_state(self) -> dict[str, Any]:
        # Reads checkpoint from edmingle_sync_state.json
        # Returns default first-run state if file does not exist
        state_path = self.paths["state"]
        if not state_path.exists():
            return {
                "version": 1,
                "last_completed_student_page": 1,
                "course_refresh": {"in_progress": False},
            }
        with state_path.open("r", encoding="utf-8") as handle:
            return json.load(handle)

    def save_state(self, state: dict[str, Any]) -> None:
        # Saves current progress checkpoint atomically to disk
        atomic_write_json(self.paths["state"], state)

    def migrate_legacy_files(self) -> None:
        # One-time migration from old script outputs to new format
        # Runs silently — only triggers if legacy files exist and new ones do not
        state_path     = self.paths["state"]
        student_master = self.paths["student_master"]
        course_master  = self.paths["course_master"]

        # Migrate students_data_2.csv to edmingle_students.csv if needed
        if not student_master.exists() and self.paths["legacy_student_master"].exists():
            legacy_rows   = read_csv_rows(self.paths["legacy_student_master"])
            migrated_rows = merge_students([], legacy_rows)
            atomic_write_csv(student_master, STUDENT_FIELDS, migrated_rows)
            self.logger.info(
                "Migrated %d unique students from %s",
                len(migrated_rows),
                self.paths["legacy_student_master"].name,
            )

        # Migrate studentCoursesEnrolled.csv to edmingle_course_enrollments.csv if needed
        if not course_master.exists() and self.paths["legacy_course_master"].exists():
            atomic_copy(self.paths["legacy_course_master"], course_master)
            self.logger.info(
                "Migrated course enrollments from %s",
                self.paths["legacy_course_master"].name,
            )

        # Initialise state from PageNo.txt if no state file exists
        if not state_path.exists():
            legacy_page = parse_legacy_page(self.paths["legacy_page_state"])
            self.save_state({
                "version": 1,
                "last_completed_student_page": legacy_page,
                "course_refresh": {"in_progress": False},
                "migrated_at": utc_now(),
            })
            self.logger.info("Initialized state at student page %d", legacy_page)

    def request_json(
        self,
        url: str,
        *,
        params: dict[str, Any],
        expected_list_key: str,
        context: str,
    ) -> dict[str, Any]:
        # Makes one API call with infinite retry and exponential backoff
        # Returns parsed JSON response dict — never returns on permanent error
        delay         = float(self.config["initial_retry_delay_seconds"])
        maximum_delay = float(self.config["maximum_retry_delay_seconds"])
        timeout       = float(self.config["request_timeout_seconds"])
        attempt       = 0

        while True:
            attempt += 1
            self.rate_limiter.acquire()  # wait for available rate limit slot

            try:
                response = self.session.get(
                    url, headers=self.headers, params=params, timeout=timeout
                )
            except requests.RequestException as error:
                # Network error — log and retry with exponential backoff
                self.logger.warning(
                    "%s request attempt %d failed: %s; retrying in %.2f seconds",
                    context, attempt, type(error).__name__, delay,
                )
                time.sleep(delay)
                delay = min(maximum_delay, delay * 2)  # double wait each retry
                continue

            if response.status_code == 429:
                # Edmingle rate limit triggered — wait the block period before retry
                retry_after = response.headers.get("Retry-After", "")
                try:
                    retry_after_seconds = float(retry_after)
                except (TypeError, ValueError):
                    retry_after_seconds = 0.0
                block_seconds = max(
                    float(self.config["rate_limit_block_seconds"]),
                    retry_after_seconds,
                )
                self.logger.warning(
                    "%s triggered HTTP 429 rate-limit lock on attempt %d; "
                    "waiting %.2f seconds before retrying",
                    context, attempt, block_seconds,
                )
                time.sleep(block_seconds)
                self.rate_limiter.reset()  # reset call history after long pause
                delay = float(self.config["initial_retry_delay_seconds"])
                continue

            if response.status_code in PERMANENT_HTTP_STATUSES:
                # 401/403/404 — retrying will never help, raise error immediately
                message = (
                    f"{context} returned permanent HTTP {response.status_code}: "
                    f"{response.text[:300]}"
                )
                self.logger.error(message)
                # Alert immediately if API key expired mid-run — most common permanent error
                if response.status_code == 401:
                    send_email_alert(
                        "[Vyoma Pipeline] STOPPED — API key expired during run",
                        f"The script stopped because the API key expired mid-run.\n\n"
                        f"Context : {context}\n"
                        f"HTTP    : {response.status_code}\n\n"
                        f"Action  : Get a fresh API key from Shankar.\n"
                        f"          Update edmingle_sync_config.json on the server.\n"
                        f"          Run the script again — it will resume from checkpoint."
                    )
                raise PermanentAPIError(message)

            if response.status_code != 200:
                # Other non-200 — classify as transient or retryable and retry
                classification = (
                    "transient"
                    if response.status_code in TRANSIENT_HTTP_STATUSES
                    or response.status_code >= 500
                    else "retryable"
                )
                self.logger.warning(
                    "%s returned %s HTTP %d on attempt %d; retrying in %.2f seconds",
                    context, classification, response.status_code, attempt, delay,
                )
                time.sleep(delay)
                delay = min(maximum_delay, delay * 2)
                continue

            try:
                data = response.json()
            except (ValueError, json.JSONDecodeError) as error:
                # Malformed JSON — retry
                self.logger.warning(
                    "%s returned invalid JSON on attempt %d: %s; retrying in %.2f seconds",
                    context, attempt, error, delay,
                )
                time.sleep(delay)
                delay = min(maximum_delay, delay * 2)
                continue

            if not isinstance(data, dict) or not isinstance(data.get(expected_list_key), list):
                # Wrong response structure — retry
                self.logger.warning(
                    "%s returned an invalid response shape on attempt %d; "
                    "retrying in %.2f seconds",
                    context, attempt, delay,
                )
                time.sleep(delay)
                delay = min(maximum_delay, delay * 2)
                continue

            return data  # successful response

    def sync_students(self, state: dict[str, Any]) -> None:
        # Fetches all student pages from Edmingle and updates student master CSV
        # Starts from last checkpoint minus overlap pages to catch new registrations
        last_page = int(state.get("last_completed_student_page", 1))
        overlap   = int(self.config["overlap_pages"])
        page      = calculate_start_page(last_page, overlap)
        fetched: list[dict[str, Any]] = []
        last_populated_page = last_page
        self.logger.info(
            "Starting student sync at page %d with %d-page overlap", page, overlap
        )

        while True:
            data = self.request_json(
                STUDENTS_URL,
                params={
                    "organization_id": self.config["organization_id"],
                    "is_archived":     0,
                    "per_page":        self.config["students_per_page"],
                    "page":            page,
                },
                expected_list_key="students",
                context=f"student page {page}",
            )
            students = data["students"]
            if not students:
                # Empty page means all students have been fetched
                self.logger.info("Student page %d was empty; student fetch complete", page)
                break
            fetched.extend(extract_student(student) for student in students)
            last_populated_page = page
            self.logger.info("Fetched student page %d containing %d rows", page, len(students))
            page += 1

        # Merge newly fetched students with existing master file and save
        existing = read_csv_rows(self.paths["student_master"])
        merged   = merge_students(existing, fetched)
        atomic_write_csv(self.paths["student_master"], STUDENT_FIELDS, merged)
        state["last_completed_student_page"] = last_populated_page
        state["last_student_sync_at"]        = utc_now()
        self.save_state(state)
        self.logger.info(
            "Published student master with %d unique students; %d rows fetched",
            len(merged), len(fetched),
        )

    def _valid_course_users(self) -> list[dict[str, str]]:
        # Returns students who have valid user_ids for the course enrollment fetch
        users: list[dict[str, str]] = []
        for row in read_csv_rows(self.paths["student_master"]):
            user_id = row.get("user_id", "").strip()
            if user_id and user_id != "NA":
                users.append({
                    "user_id": user_id,
                    "name":    row.get("name", "").strip(),
                    "email":   row.get("email", "").strip(),
                })
        return users

    def _start_course_refresh(self, state: dict[str, Any]) -> dict[str, Any]:
        # Initialises a fresh course enrollment run
        # Takes a snapshot of the student list so it stays stable during the run
        users = self._valid_course_users()
        atomic_write_csv(self.paths["course_snapshot"], ["user_id", "name", "email"], users)
        atomic_write_csv(self.paths["course_progress"], COURSE_FIELDS, [])
        output_offset = self.paths["course_progress"].stat().st_size
        state["course_refresh"] = {
            "in_progress":            True,
            "started_at":             utc_now(),
            "active_elapsed_seconds": 0.0,
            "next_student_index":     0,
            "output_offset":          output_offset,
            "total_students":         len(users),
        }
        self.save_state(state)
        self.logger.info("Started full course refresh for %d students", len(users))
        return state

    def _prepare_progress_for_resume(self, refresh: dict[str, Any]) -> None:
        # Truncates in-progress file back to last safe checkpoint position
        # Prevents duplicate rows if script was interrupted mid-write
        progress = self.paths["course_progress"]
        snapshot = self.paths["course_snapshot"]
        if not progress.exists() or not snapshot.exists():
            raise RuntimeError(
                "Course refresh state exists but its progress or snapshot file is missing"
            )
        output_offset = int(refresh["output_offset"])
        if progress.stat().st_size < output_offset:
            raise RuntimeError(
                "Course progress file is shorter than its saved checkpoint offset"
            )
        # Truncate to last confirmed safe byte position
        with progress.open("r+b") as handle:
            handle.truncate(output_offset)
            handle.flush()
            os.fsync(handle.fileno())

    def _recover_completed_course_publication(
        self, state: dict[str, Any], refresh: dict[str, Any]
    ) -> bool:
        # Handles edge case: run finished but crashed before state was updated
        completed = int(refresh.get("next_student_index", 0))
        total     = int(refresh.get("total_students", -1))
        if (
            completed == total
            and total >= 0
            and not self.paths["course_progress"].exists()
            and self.paths["course_master"].exists()
        ):
            self.paths["course_snapshot"].unlink(missing_ok=True)
            state["course_refresh"] = {
                "in_progress":    False,
                "completed_at":   utc_now(),
                "total_students": total,
            }
            self.save_state(state)
            self.logger.info("Recovered completed course enrollment publication")
            return True
        return False

    def sync_courses(self, state: dict[str, Any]) -> None:
        # Main course enrollment loop — 1 API call per student
        # Saves checkpoint after each student so any crash is resumable
        refresh = state.get("course_refresh", {"in_progress": False})
        if not refresh.get("in_progress"):
            # Fresh start — initialise the course refresh
            state   = self._start_course_refresh(state)
            refresh = state["course_refresh"]
        else:
            # Check if run actually already finished but state was not updated
            if self._recover_completed_course_publication(state, refresh):
                return
            self.logger.info(
                "Resuming course refresh at student %d of %d",
                int(refresh["next_student_index"]) + 1,
                int(refresh["total_students"]),
            )

        # Truncate progress file to last confirmed safe position
        self._prepare_progress_for_resume(refresh)
        users      = read_csv_rows(self.paths["course_snapshot"])
        next_index = int(refresh["next_student_index"])
        session_started_at = time.monotonic()

        with self.paths["course_progress"].open("a", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=COURSE_FIELDS, extrasaction="ignore")
            for index in range(next_index, len(users)):
                user    = users[index]
                user_id = user["user_id"]

                # Fetch all enrolled class sessions for this student
                data    = self.request_json(
                    COURSES_URL,
                    params={"user_id": user_id, "response_type": 1},
                    expected_list_key="classes",
                    context=f"course user_id {user_id}",
                )
                classes = data["classes"]

                # Write one row per class session for this student
                for course in classes:
                    row = {
                        "user_id": user_id,
                        "name":    user.get("name", ""),
                        "email":   user.get("email", ""),
                    }
                    row.update(course)
                    writer.writerow(row)

                # Flush to physical disk after every student — survives a crash
                handle.flush()
                os.fsync(handle.fileno())

                # Update checkpoint — next resume starts from index + 1
                refresh["next_student_index"]     = index + 1
                refresh["output_offset"]          = self.paths["course_progress"].stat().st_size
                refresh["last_completed_user_id"] = user_id
                active_elapsed                    = float(refresh.get("active_elapsed_seconds", 0.0))
                active_elapsed                   += time.monotonic() - session_started_at
                refresh["active_elapsed_seconds"] = active_elapsed
                session_started_at                = time.monotonic()
                state["course_refresh"]           = refresh
                self.save_state(state)  # atomic checkpoint save to disk

                # Log progress every 100 students with elapsed time and ETA
                completed = index + 1
                if completed == 1 or completed % 100 == 0 or completed == len(users):
                    average_seconds     = active_elapsed / completed
                    estimated_remaining = average_seconds * (len(users) - completed)
                    self.logger.info(
                        "Course checkpoint saved: %d/%d students; elapsed %s; "
                        "estimated remaining %s",
                        completed,
                        len(users),
                        format_duration(active_elapsed),
                        format_duration(estimated_remaining),
                    )

                # Send status update email every STATUS_UPDATE_INTERVAL seconds of
                # active processing time — so you know run is alive. The interval
                # is configured in notifications.yaml (status_update_interval_hours),
                # not hardcoded here — see module-level STATUS_UPDATE_INTERVAL.
                last_update = refresh.get("last_status_email_at", 0.0)
                if active_elapsed - last_update >= STATUS_UPDATE_INTERVAL:
                    pct             = (completed / len(users)) * 100
                    free_gb         = shutil.disk_usage(SCRIPT_DIR).free / (1024 ** 3)
                    avg_sec         = active_elapsed / completed if completed > 0 else 0
                    est_remaining   = avg_sec * (len(users) - completed)
                    send_email_alert(
                        f"[Vyoma Pipeline] STATUS — {pct:.1f}% complete",
                        f"Vyoma Edmingle sync is still running.\n\n"
                        f"Progress  : {completed:,} / {len(users):,} students ({pct:.1f}%)\n"
                        f"Elapsed   : {format_duration(active_elapsed)}\n"
                        f"Remaining : ~{format_duration(est_remaining)}\n"
                        f"Disk free : {free_gb:.1f} GB\n\n"
                        f"No action needed — script is running normally."
                    )
                    # Save timestamp so next update fires STATUS_UPDATE_INTERVAL from now
                    refresh["last_status_email_at"] = active_elapsed
                    self.logger.info("Status update email sent at %.1f%% complete", pct)

        # Atomically rename progress file to final output filename
        os.replace(self.paths["course_progress"], self.paths["course_master"])
        self.paths["course_snapshot"].unlink(missing_ok=True)
        state["course_refresh"] = {
            "in_progress":    False,
            "completed_at":   utc_now(),
            "total_students": len(users),
        }
        self.save_state(state)
        self.logger.info("Published complete course enrollment master")

    def run(self) -> None:
        # Top level orchestrator — runs student sync then course sync
        self.logger.info("Edmingle sync run started")
        self.migrate_legacy_files()   # one-time migration if legacy files exist
        state = self.load_state()     # load checkpoint from last run

        # Print run summary to log before starting
        print_startup_summary(self.config, state)

        if state.get("course_refresh", {}).get("in_progress"):
            # Course refresh was interrupted — skip student sync and resume directly
            self.sync_courses(state)
            self.logger.info("Resumed Edmingle sync run completed")
            send_email_alert(
                "[Vyoma Pipeline] COMPLETED — Course sync finished",
                "The Edmingle course enrollment sync has completed successfully.\n\n"
                "Output file : edmingle_course_enrollments.csv\n"
                "Check       : edmingle_sync.log for full details."
            )
            return

        # Full run — fetch students first then courses
        self.sync_students(state)
        self.sync_courses(self.load_state())
        self.logger.info("Edmingle sync run completed")
        send_email_alert(
            "[Vyoma Pipeline] COMPLETED — Full sync finished",
            "The Edmingle full sync has completed successfully.\n\n"
            "Output files:\n"
            "  edmingle_students.csv\n"
            "  edmingle_course_enrollments.csv\n\n"
            "Check edmingle_sync.log for full run details."
        )


# =============================================================================
# SECTION 12 — LOGGING SETUP
# Writes structured logs to both file and terminal console
# Log format: 2026-06-25 09:30:00 INFO message here
# =============================================================================

def configure_logging(log_path: Path) -> logging.Logger:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("edmingle_sync")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    # File handler — appends to edmingle_sync.log
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Console handler — shows live output in terminal while running
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


# =============================================================================
# SECTION 13 — ARGUMENT PARSER
# Allows overriding config file path from command line
# Default: looks for edmingle_sync_config.json in same folder as this script
# Usage  : python3 edmingle_student_course_sync.py --config /path/to/config.json
# =============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Incrementally sync Edmingle students and rebuild course enrollments."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).with_name("edmingle_sync_config.json"),
        help="Path to the JSON configuration file.",
    )
    return parser.parse_args()


# =============================================================================
# SECTION 14 — MAIN ENTRY POINT  [MODIFIED BY SHUBHAM]
# Runs startup checks then starts the sync pipeline
# On crash: writes SCRIPT_FAILED.txt, sends email alert, returns exit code 1
# On Ctrl+C: logs clean interrupt message, returns exit code 130
# =============================================================================

def main() -> int:
    args = parse_args()
    try:
        # Load config early so startup checks can read API key before sync starts
        with args.config.open("r", encoding="utf-8") as f:
            config = json.load(f)
        # api_key / organization_id now live in the shared credentials.yaml
        # (one directory up), not in this pipeline's own JSON config
        creds = _load_credentials(args.config.resolve().parent)
        config["api_key"] = creds.get("api_key", "")
        config["organization_id"] = creds.get("organization_id", "")

        # Run all pre-flight checks — exits immediately if any check fails
        run_startup_checks(config)

        # Start the full sync pipeline
        sync = EdmingleSync(args.config)
        # Estimate total run time from the actual configured rate limit and the
        # approximate full-organization student count (same 122,000 assumption
        # used in print_startup_summary for a fresh run) — not a fixed guess.
        rate           = int(sync.config.get("max_calls_per_minute", 30))
        estimated_hours = (122000 / rate) / 60
        send_email_alert(
            "[Vyoma Pipeline] STARTED — Sync has begun",
            f"The Edmingle sync script has started successfully.\n\n"
            f"Time    : {datetime.now()}\n"
            f"Server  : 195.35.6.99\n"
            f"Est. time : ~{estimated_hours:.0f} hours (at {rate} calls/min, ~122,000 students)\n"
            f"You will receive a status update every "
            f"{STATUS_UPDATE_INTERVAL / 3600:.1f} hours."
        )
        sync.run()

    except KeyboardInterrupt:
        # User pressed Ctrl+C — checkpoint already saved, safe to resume later
        logging.getLogger("edmingle_sync").warning(
            "Run interrupted; the next run will resume from the saved checkpoint"
        )
        return 130

    except Exception:
        # Unexpected crash — log full traceback
        logger = logging.getLogger("edmingle_sync")
        if logger.handlers:
            logger.exception("Edmingle sync run failed")
        else:
            print("Edmingle sync run failed", file=sys.stderr)

        # Write SCRIPT_FAILED.txt so failure is immediately visible in VS Code sidebar
        # Anchored to SCRIPT_DIR — not cwd — so it always lands in ela_mis_datasets/
        try:
            with open(OUTPUT_DIR / "SCRIPT_FAILED.txt", "w") as f:
                f.write(f"Script failed at : {datetime.now()}\n")
                f.write("Action required  : Check edmingle_sync.log for the error\n")
                f.write("To resume        : Run the script again — it will auto-resume\n")
        except Exception:
            pass  # do not let file write failure hide the real error

        # Send failure email with step-by-step resume instructions
        send_email_alert(
            "[Vyoma Pipeline] FAILED — Script crashed",
            f"The Edmingle sync script has crashed.\n\n"
            f"Time    : {datetime.now()}\n\n"
            f"Action  : Check edmingle_sync.log on the server for the error.\n\n"
            f"To resume:\n"
            f"  1. SSH into the VPS\n"
            f"  2. cd {SCRIPT_DIR}\n"
            f"  3. tmux attach -t vyoma\n"
            f"  4. python3 edmingle_student_course_sync.py\n"
            f"  The script will automatically resume from last checkpoint."
        )
        return 1

    return 0


# Script entry point — raises SystemExit so OS receives the return code from main()
if __name__ == "__main__":
    raise SystemExit(main())
