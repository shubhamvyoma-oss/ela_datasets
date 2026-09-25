#!/usr/bin/env python3
# Fetches all student profiles and course enrollments from Edmingle.
# Author: Shankararama Sharma (core logic), Shubham (improvements). ~68-80hr full run, auto-resumes.

from __future__ import annotations

# Standard library — file, system, network, data handling
import argparse
import csv
import json
import logging
import os
import shutil
import socket
import sys
import time
from datetime import datetime
from collections.abc import Iterable
from itertools import chain
from pathlib import Path
from typing import Any

# Shared bytecode cache across every ela_datasets/ pipeline -- must be set before any local import.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

# Shared helpers (credentials/notifications, email, rate limiter, atomic writes) live in
# ela_datasets/common.py, two directories up (scripts/ -> ela_mis_datasets/ -> ela_datasets/).
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

# All output/state/log files resolve relative to this script's own folder, not the caller's cwd.
SCRIPT_DIR = Path(__file__).resolve().parent

# All generated CSV/state/log files live in output/, a sibling of scripts/.
OUTPUT_DIR = (SCRIPT_DIR / ".." / "output").resolve()


def _load_notifications_config() -> dict[str, Any]:
    # Reads ela_mis_datasets/notifications.yaml. A missing file is a hard failure for this script
    # (common.load_notifications() alone would treat it as "notifications disabled").
    notifications_path = common.REPO_ROOT / "ela_mis_datasets" / "notifications.yaml"
    if not notifications_path.exists():
        raise FileNotFoundError(f"Notifications file not found: {notifications_path}")
    return common.load_notifications("ela_mis_datasets")


_notifications_config = _load_notifications_config()
_email_config = (_notifications_config.get("channels", {}) or {}).get("email", {}) or {}
_smtp_config = _email_config.get("smtp", {}) or {}


# Periodic status-email interval during the course pull, from notifications.yaml's
# channels.email.status_update_interval_hours (default 6h), converted to seconds.
STATUS_UPDATE_INTERVAL = int(
    float(_email_config.get("status_update_interval_hours", 6)) * 3600
)


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
    # "UserName" was removed 2026-09-23: it was read by list position (wrong for >99% of students) and
    # the native "user_username" column above already holds every username.
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


# Default output file names, all in output/
DEFAULT_FILES = {
    "student_master":  "edmingle_students.csv",                       # final student output
    "course_master":   "edmingle_course_enrollments.csv",             # final course output
    "state":           "edmingle_sync_state.json",                    # checkpoint state
    "log":             "edmingle_sync.log",                           # run log
    "course_progress": "edmingle_course_enrollments.in_progress.csv", # temp during run
}

# Edmingle API endpoints, built from edmingle.base_url in credentials.yaml
_BASE_URL    = common.edmingle_settings()["base_url"]
STUDENTS_URL = f"{_BASE_URL}/organization/students"
COURSES_URL  = f"{_BASE_URL}/admin/classes/attendance"


def send_email_alert(subject: str, body: str) -> None:
    # Delegates to common.send_mail() -- never raises, logs a warning and returns instead,
    # so email failure never crashes the pipeline.
    logger = logging.getLogger("edmingle_sync")
    common.send_mail(_notifications_config, subject, body, logger)


def _roster_row_count() -> int:
    path = OUTPUT_DIR / DEFAULT_FILES["student_master"]
    if not path.exists():
        return 0
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return max(sum(1 for _ in csv.reader(handle)) - 1, 0)


def _fail_startup(subject: str, message: str) -> None:
    print(f"STARTUP FAILED -- {message}")
    send_email_alert(f"[Vyoma Pipeline] STARTUP FAILED — {subject}", f"Script failed startup check.\n\n{message}")
    sys.exit(1)


def run_startup_checks(config: dict[str, Any]) -> int:
    """Stop before a multi-day run if the disk is too full or the API key is bad. Returns the number of
    students Edmingle reports (only used for the time estimate in the STARTED email); falls back to the
    roster file's row count when it cannot be read."""
    free_gb = shutil.disk_usage(SCRIPT_DIR).free / 1024 ** 3
    if free_gb < 2.0:
        _fail_startup("Not enough disk space", f"Only {free_gb:.1f} GB free. Need at least 2 GB. Free up space before running.")
    student_count = _roster_row_count()
    try:
        # One student is enough to validate the key, and total_rows is the real student count. The endpoint
        # counts every student, so it takes ~18 s.
        resp = requests.get(
            STUDENTS_URL, headers=common.auth_headers(config["api_key"], config["organization_id"]),
            params={"organization_id": config["organization_id"], "per_page": 1, "page": 1}, timeout=60,
        )
    except requests.RequestException as error:
        print(f"WARN: could not validate the API key ({type(error).__name__}) -- proceeding anyway")
        return student_count
    if resp.status_code in (400, 401, 403):
        _fail_startup("API key expired",
                      f"API key invalid or expired. HTTP {resp.status_code}. Check edmingle.api_key in credentials.yaml "
                      f"(edmingle_api_key_generator rotates it on the 25th; run edmingle_generate_api_key.py by hand "
                      f"if it is stale).")
    if resp.status_code != 200:
        print(f"WARN: API returned HTTP {resp.status_code} -- proceeding anyway")
        return student_count
    try:
        student_count = int(resp.json()["page_context"]["total_rows"])
    except (ValueError, KeyError, TypeError):
        pass  # keep the roster-file estimate
    print(f"Startup checks passed: {free_gb:.1f} GB free, {student_count:,} students in Edmingle.")
    return student_count


PermanentAPIError = common.PermanentAPIError  # HTTP 400/401/403/404 -- retrying will never help


def calculate_start_page(last_completed_page: int, overlap_pages: int) -> int:
    # Go back N pages from last run to catch students who registered during that run
    return max(1, last_completed_page - overlap_pages)


def merge_students(
    existing_rows: Iterable[dict[str, Any]],
    fetched_rows: Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    # Deduplicates by user_id -- a fetched row overwrites an existing one for the same user
    by_user_id: dict[str, dict[str, Any]] = {}
    for row in chain(existing_rows, fetched_rows):
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


# Core pipeline -- student data fetch and course enrollment fetch. Original logic by
# Shankararama Sharma; only run() was modified since. RollingRateLimiter itself lives in common.py.
class EdmingleSync:

    def __init__(
        self,
        config_path: Path,
        session: requests.Session | Any | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.config_path = config_path.resolve()
        self.config      = self._load_config()
        # Anchored to SCRIPT_DIR, not the caller's cwd, so output always lands in ela_mis_datasets/.
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
        creds = common.edmingle_settings()  # always the repo-root credentials.yaml, wherever --config points
        config["api_key"] = creds["api_key"]
        config["organization_id"] = creds["organization_id"]
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

    def request_json(
        self,
        url: str,
        *,
        params: dict[str, Any],
        expected_list_key: str,
        context: str,
    ) -> dict[str, Any]:
        # One API call with infinite retry and exponential backoff (see common.get_json).
        # Returns the parsed JSON dict -- never returns on a permanent error.
        try:
            return common.get_json(
                url, headers=self.headers, params=params, session=self.session,
                timeout=float(self.config["request_timeout_seconds"]),
                delay=float(self.config["initial_retry_delay_seconds"]),
                max_delay=float(self.config["maximum_retry_delay_seconds"]),
                block_seconds=float(self.config["rate_limit_block_seconds"]),
                rate_limiter=self.rate_limiter,
                validate=lambda data: isinstance(data, dict) and isinstance(data.get(expected_list_key), list),
                label=context, logger=self.logger,
            )
        except PermanentAPIError as error:
            self.logger.error(str(error))
            # Alert immediately if the API key expired mid-run -- the most common permanent error
            if error.status == 401:
                send_email_alert(
                    "[Vyoma Pipeline] STOPPED — API key expired during run",
                    f"The script stopped because the API key expired mid-run.\n\n"
                    f"Context : {context}\n"
                    f"HTTP    : {error.status}\n\n"
                    f"Action  : Check edmingle.api_key in credentials.yaml (edmingle_api_key_generator\n"
                    f"          rotates it on the 25th; run edmingle_generate_api_key.py if it is stale).\n"
                    f"          Run the script again — it will resume from checkpoint."
                )
            raise

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

    def _start_course_refresh(self, state: dict[str, Any], users: list[dict[str, str]]) -> dict[str, Any]:
        # Initialises a fresh course enrollment run over `users` (the roster is not rewritten while a
        # refresh is in progress, so a resumed run recomputes the identical list)
        atomic_write_csv(self.paths["course_progress"], COURSE_FIELDS, [])
        state["course_refresh"] = {
            "in_progress":            True,
            "started_at":             utc_now(),
            "active_elapsed_seconds": 0.0,
            "next_student_index":     0,
            "output_offset":          self.paths["course_progress"].stat().st_size,
            "total_students":         len(users),
        }
        self.save_state(state)
        self.logger.info("Started full course refresh for %d students", len(users))
        return state["course_refresh"]

    def _resume_course_refresh(self, state: dict[str, Any], refresh: dict[str, Any], users: list[dict[str, str]]) -> bool:
        # Checks an interrupted refresh is still consistent and cuts the progress file back to its last
        # confirmed byte (no duplicate rows after a mid-write crash). Returns True if the refresh had
        # actually finished and only its final state was lost.
        progress = self.paths["course_progress"]
        total = int(refresh["total_students"])
        if int(refresh["next_student_index"]) == total and not progress.exists() and self.paths["course_master"].exists():
            state["course_refresh"] = {"in_progress": False, "completed_at": utc_now(), "total_students": total}
            self.save_state(state)
            self.logger.info("Recovered completed course enrollment publication")
            return True
        if len(users) != total:
            raise RuntimeError(f"The roster has {len(users)} eligible students but this refresh started with {total}")
        output_offset = int(refresh["output_offset"])
        if not progress.exists() or progress.stat().st_size < output_offset:
            raise RuntimeError("Course progress file is missing or shorter than its saved checkpoint offset")
        with progress.open("r+b") as handle:
            handle.truncate(output_offset)
            handle.flush()
            os.fsync(handle.fileno())
        self.logger.info("Resuming course refresh at student %d of %d", int(refresh["next_student_index"]) + 1, total)
        return False

    def sync_courses(self, state: dict[str, Any]) -> None:
        # Main course enrollment loop — 1 API call per student
        # Saves checkpoint after each student so any crash is resumable
        refresh = state.get("course_refresh", {"in_progress": False})
        users = self._valid_course_users()
        if not refresh.get("in_progress"):
            refresh = self._start_course_refresh(state, users)
        elif self._resume_course_refresh(state, refresh, users):
            return
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
        state = self.load_state()     # load checkpoint from last run

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


# Usage: python3 edmingle_student_course_sync.py --config /path/to/config.json
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


# Runs startup checks then the sync pipeline. Crash -> SCRIPT_FAILED.txt + email, exit 1.
# Ctrl+C -> clean interrupt log, exit 130.
def main() -> int:
    args = parse_args()
    try:
        sync = EdmingleSync(args.config)
        student_count = run_startup_checks(sync.config)  # exits if the disk or the API key is bad
        rate = int(sync.config["max_calls_per_minute"])
        estimate = (f"~{student_count / rate / 60:.0f} hours (at {rate} calls/min, ~{student_count:,} students)"
                    if student_count else "unknown")
        send_email_alert(
            "[Vyoma Pipeline] STARTED — Sync has begun",
            f"The Edmingle sync script has started successfully.\n\n"
            f"Time    : {datetime.now()}\n"
            f"Server  : {socket.gethostname()}\n"
            f"Est. time : {estimate}\n"
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
            f"  3. tmux attach -t ela_mis_datasets\n"
            f"  4. python3 edmingle_student_course_sync.py\n"
            f"  The script will automatically resume from last checkpoint."
        )
        return 1

    return 0


# Script entry point — raises SystemExit so OS receives the return code from main()
if __name__ == "__main__":
    raise SystemExit(main())
