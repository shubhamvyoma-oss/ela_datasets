"""
attendance.py -- Edmingle report_type=55 attendance pipeline: fetches daily attendance for a date
range, stages one CSV per day (crash-safe resume), and produces a per-batch summary CSV. Full
behavior (resume, retry/backoff, email alerts, config keys) is
documented in ../ATTENDANCE.md, not repeated here.

USAGE
  python attendance.py --from 2020-01-01 --to 2026-08-31
  python attendance.py --date 2026-06-15
  python attendance.py                       # config lookback
  python attendance.py --from-file raw.csv  # skip API
  python attendance.py --dry-run --from 2020-01-01 --to 2020-01-07
  (a re-run of the same command skips finished days and retries the rest; delete output/staging to start over)
  python attendance.py --config /other/config.yaml ...
"""

import argparse
import fcntl
import logging
import logging.handlers
import os
import random
import shutil
import signal
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Shared bytecode cache across every ela_datasets/ pipeline -- must be set before any local import.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import pandas as pd
import requests
import yaml

import common

# ── CONSTANTS ──

VERSION = "1.3.0"
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

# One row per (batch, session). NOTE: total_classes_remaining stays 0 unless the report includes
# future-dated sessions -- true remaining counts need the batch schedule endpoint (not this one).
SESSION_OUTPUT_COLUMNS = [
    "batch_Id", "batchName", "course_Id", "courseName", "session_number",
    "classDate", "is_conducted", "present_count", "absent_count",
    "late_count", "total_marked", "session_attendance_percentage",
]


# ── CUSTOM EXCEPTIONS ──

class FatalAPIError(Exception):
    """401 / 403 / 404 -- no retry, stop the run immediately."""

class PipelineError(Exception):
    """General unrecoverable pipeline error."""


# ── CONFIG ──

_DEFAULTS = {
    "api": {
        "timeout_seconds": 60,
        "rate_limit_sleep_seconds": 2.5,
        "max_retries": 3,
        "retry_backoff_base_seconds": 5,
        "retry_backoff_max_seconds": 120,
        "retry_jitter_seconds": 1.0,
        "max_consecutive_errors": 3,
    },
    "email": {
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
            f"See ../ATTENDANCE.md for the required config.yaml keys.\n"
        )
    with open(path, encoding="utf-8") as f:
        user_cfg = yaml.safe_load(f) or {}

    cfg = _deep_merge(_DEFAULTS, user_cfg)

    # Edmingle API key/org id are shared across every ela_datasets/ pipeline, not read from config.yaml.
    script_dir = Path(__file__).resolve().parent
    creds_path = script_dir.parent.parent / "credentials.yaml"
    edmingle = common.edmingle_settings(path=creds_path)
    cfg["api"]["key"]    = edmingle["api_key"]
    cfg["api"]["org_id"] = edmingle["organization_id"]
    cfg["api"].setdefault("url", f"{edmingle['base_url']}/report/csv")

    # Per-pipeline notification config, in this pipeline's own folder.
    if not (common.REPO_ROOT / "attendance" / "notifications.yaml").exists():
        sys.exit(f"\nNotifications file not found: {common.REPO_ROOT / 'attendance' / 'notifications.yaml'}\n")
    cfg["_notifications"] = common.load_notifications("attendance")  # common.send_mail reads the SMTP block
    email_channel = (cfg["_notifications"].get("channels") or {}).get("email") or {}
    for key in ("notify_on_critical", "notify_on_warning", "notify_on_completion"):
        cfg["email"][key] = email_channel.get(key, cfg["email"][key])

    required = [("paths", "output_folder"), ("paths", "log_folder")]
    missing = [f"{a}.{b}" for a, b in required
               if not str(cfg.get(a, {}).get(b, "")).strip()]
    if missing:
        sys.exit(f"\nMissing required config keys: {', '.join(missing)}\n")

    if not str(cfg["api"].get("key", "")).strip():
        sys.exit(f"\nMissing edmingle.api_key in shared credentials file: {creds_path}\n")
    if not str(cfg["api"].get("org_id", "")).strip():
        sys.exit(f"\nMissing edmingle.organization_id in shared credentials file: {creds_path}\n")

    # Anchor relative paths to this script's folder, not the caller's cwd (matters under cron).
    # Absolute paths are left untouched.
    for _key in ("output_folder", "log_folder", "staging_folder", "lock_file"):
        _val = cfg["paths"].get(_key)
        if _val and not Path(_val).is_absolute():
            cfg["paths"][_key] = str((script_dir / _val).resolve())

    out = Path(cfg["paths"]["output_folder"])
    cfg["paths"].setdefault("staging_folder", str(out / "staging"))
    cfg["paths"].setdefault("lock_file",      str(out / "pipeline.lock"))

    return cfg


# ── LOGGING ──

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


# ── LOCK FILE ──

def acquire_lock(path: str, log: logging.Logger):
    """Hold an exclusive lock on the lock file for the life of the process. The OS releases it when the
    process exits, even on a crash, so there is never a stale lock to clean up. Keep the returned handle."""
    handle = open(path, "a+")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.seek(0)
        raise PipelineError(f"Another instance already running (PID {handle.read().strip() or '?'}). Lock: {path}.") from None
    handle.seek(0)
    handle.truncate()
    handle.write(str(os.getpid()))
    handle.flush()
    log.debug(f"Lock acquired (PID {os.getpid()})")
    return handle


# ── EMAIL ──

_SUBJECT_TAGS = {"critical": "CRITICAL", "warning": "WARNING", "completion": "SUCCESS"}


def notify(cfg: dict, kind: str, subject: str, details: dict, log: logging.Logger) -> None:
    """Plain-text email through common.send_mail (never raises). `kind` is critical / warning / completion;
    notifications.yaml switches each on or off (notify_on_<kind>)."""
    if cfg["email"][f"notify_on_{kind}"]:
        body = "\n".join(f"{key}: {value}" for key, value in details.items())
        common.send_mail(cfg["_notifications"], f"[{_SUBJECT_TAGS[kind]}] Edmingle Pipeline -- {subject[:80]}", body, log)


# ── DISK SPACE CHECK ──

def check_disk_space(cfg: dict, log: logging.Logger):
    required_mb = cfg["pipeline"]["min_free_disk_mb"]
    folder      = cfg["paths"]["output_folder"]
    Path(folder).mkdir(parents=True, exist_ok=True)
    free_mb = shutil.disk_usage(folder).free / (1024 ** 2)
    log.info(f"Free disk space: {free_mb:,.0f} MB (required: {required_mb} MB)")
    if free_mb < required_mb:
        raise PipelineError(f"Insufficient disk space: {free_mb:.0f} MB free, {required_mb} MB required in {folder}.")


# ── API LAYER ──

def _day_params(date_str: str, cfg: dict) -> dict:
    day = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=IST)
    return {
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
    Raises FatalAPIError for 401 / 403 / 404 / Edmingle 6002.
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

    api_cfg = cfg["api"]

    def usable(data) -> bool:  # rows, or an Edmingle 6001/6002 code handled below
        return isinstance(data, dict) and (
            "data" in data or str(data.get("error_code") or data.get("code") or "") in ("6001", "6002"))

    try:
        data = common.get_json(
            api_cfg["url"], headers=common.auth_headers(api_cfg["key"], api_cfg["org_id"]),
            params=_day_params(date_str, cfg), session=session, timeout=api_cfg["timeout_seconds"],
            attempts=api_cfg["max_retries"], delay=api_cfg["retry_backoff_base_seconds"],
            max_delay=api_cfg["retry_backoff_max_seconds"], jitter=api_cfg["retry_jitter_seconds"],
            block_seconds=lambda r: common.retry_after(r) + 2 if r.headers.get("Retry-After") else 30,
            validate=usable, label=f"  [{date_str}]", logger=log,
        )
    except common.PermanentAPIError as e:
        if e.status == 400:  # bad params for this date: skip it, don't retry
            log.error(f"{e}. Skipping date.")
            return pd.DataFrame()
        msg = f"{e} -- check edmingle.api_key / organization_id in credentials.yaml and the API url"
        log.critical(msg)
        raise FatalAPIError(msg) from e
    except common.RetriesExhausted as e:
        log.error(str(e))
        raise ValueError(str(e)) from e  # run_pull_loop counts this as a failed date

    err_code = str(data.get("error_code") or data.get("code") or "")
    if err_code == "6001":
        log.error(f"  [{date_str}] Edmingle 6001 (invalid params): {data.get('message', '')}. Skipping.")
        return pd.DataFrame()
    if err_code == "6002":
        msg = f"Edmingle 6002 (auth failure): {data.get('message', '')}"
        log.critical(f"  [{date_str}] {msg}")
        raise FatalAPIError(msg)

    rows = data["data"]
    if not rows:
        log.info(f"  [{date_str}] 0 rows (quiet day / no sessions).")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    log.info(f"  [{date_str}] {len(df):,} rows fetched.")
    return df


# ── DATE HELPERS ──

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


# ── PULL ORCHESTRATION ──

def run_pull_loop(dates: list, session: requests.Session, cfg: dict, log: logging.Logger, dry_run: bool) -> list:
    """Fetch every date that has no staging file yet and return the staging files (old and new).
    A staging file `raw_<date>.csv` is written atomically and doubles as the resume marker; an empty
    `raw_<date>.empty` marks a day the API returned no rows for. A date that fails is simply left without
    a file, so re-running the same command retries exactly the missing dates."""
    staging_dir = Path(cfg["paths"]["staging_folder"])
    staging_dir.mkdir(parents=True, exist_ok=True)
    sleep_s     = cfg["api"]["rate_limit_sleep_seconds"]
    max_consec  = cfg["api"]["max_consecutive_errors"]

    consecutive = n_ok = n_fail = n_quiet = n_resumed = 0
    staging_files = []

    log.info(
        f"{'[DRY RUN] ' if dry_run else ''}"
        f"Pull loop: {len(dates)} date(s), {dates[0]} to {dates[-1]}"
    )

    for i, date_str in enumerate(dates, start=1):
        staged = staging_dir / f"raw_{date_str}.csv"
        quiet  = staging_dir / f"raw_{date_str}.empty"
        if staged.exists() or quiet.exists():
            if staged.exists():
                staging_files.append(str(staged))
            n_resumed += 1
            continue

        if (i - 1) % 25 == 0:
            log.info(
                f"Progress: {i-1}/{len(dates)} ({(i-1) / len(dates) * 100:.0f}%)  "
                f"ok={n_ok} fail={n_fail} quiet={n_quiet} resumed={n_resumed}"
            )

        try:
            df = fetch_one_day(date_str, session, cfg, log, dry_run=dry_run)

        except FatalAPIError as e:
            log.critical(f"Fatal API error: {e}. Run aborted.")
            notify(cfg, "critical", str(e), {
                "Error":    str(e),
                "Date":     date_str,
                "Progress": f"{i-1}/{len(dates)} dates completed",
                "Action":   "Fix config.yaml / credentials.yaml and re-run (finished days are kept; the run will resume).",
            }, log)
            raise PipelineError(f"Fatal API error: {e}")

        except ValueError:
            # All retries exhausted for this date
            consecutive += 1
            n_fail      += 1
            log.error(f"  [{date_str}] Failed. Consecutive failures: {consecutive}/{max_consec}.")
            notify(cfg, "warning", f"Date {date_str} failed after all retries", {
                "Consecutive failures": str(consecutive),
                "Max allowed":          str(max_consec),
            }, log)
            if consecutive >= max_consec:
                msg = f"Circuit breaker: {consecutive} consecutive failures at {date_str}. Stopping pull."
                log.critical(msg)
                notify(cfg, "critical", msg, {
                    "Error":            msg,
                    "Last failed date": date_str,
                    "Progress":         f"{i-1}/{len(dates)} dates",
                    "Action":           "Fix the issue and re-run (finished days are kept).",
                }, log)
                raise PipelineError(msg)
            if i < len(dates):
                time.sleep(sleep_s)
            continue

        consecutive = 0  # a valid API response resets the consecutive-failure counter
        if df.empty:
            quiet.touch()
            n_quiet += 1
        else:
            part = staged.with_name(staged.name + ".part")
            df.to_csv(part, index=False, encoding="utf-8-sig")
            os.replace(part, staged)  # atomic: a half-written file is never mistaken for a finished day
            staging_files.append(str(staged))
            n_ok += 1
        if i < len(dates):
            time.sleep(sleep_s)

    log.info(f"Pull loop done -- ok={n_ok} fail={n_fail} quiet={n_quiet} resumed={n_resumed} | total={len(dates)}")
    return staging_files


# ── SUMMARISE STAGING FILES (memory-bounded) ──

# Columns the cleaning/summary code actually reads. Everything else in a staging file is dropped, but
# folded into _ROW_HASH first so clean_data()'s exact-duplicate removal behaves as if every column were kept.
_NEEDED_COLUMNS = [
    "classDate", "startTime", "batch_Id", "student_Id", "studentBatchStatus", "studentAttendanceStatus",
    "studentRating", "batchName", "bundle_Id", "bundleName", "course_Id", "courseName", "teacher_Id",
    "teacherName",
]
_ROW_HASH = "_row_hash"
_PARTITION_RAW_BYTES = 150 * 1024 ** 2    # raw CSV bytes per partition -- keeps one partition well under 1 GB of RAM
_SPILL_DISK_FRACTION = 0.5                # the pruned spill files are roughly half the size of the raw ones


def _row_hash(raw: pd.DataFrame) -> pd.Series:
    """64-bit hash of the WHOLE original row (as int64 so it survives a CSV round trip)."""
    hashed = pd.util.hash_pandas_object(raw.astype(str), index=False).to_numpy()
    return pd.Series(hashed.view("int64"), index=raw.index)


def _summarise(df: pd.DataFrame, session_col: str, cfg: dict, log: logging.Logger, validate: bool = True):
    """Clean + summarise one frame. Returns (batch summary, session-wise frame or None, statuses seen)."""
    clean    = clean_data(df, session_col, cfg, log)
    observed = set(clean["studentAttendanceStatus"].dropna().unique())
    if validate:
        validate_present_value(clean, cfg)
    summary    = compute_batch_summary(clean, session_col, cfg)
    session_df = (build_session_wise_output(clean, session_col, cfg)
                  if cfg["pipeline"].get("write_session_wise_csv", True) else None)
    return summary, session_df, observed


def summarise_frame(raw_df: pd.DataFrame, cfg: dict, log: logging.Logger):
    """The whole raw dataset is already in memory (--from-file)."""
    session_col = resolve_session_id_column(raw_df, cfg, log)
    summary, session_df, _ = _summarise(raw_df, session_col, cfg, log)
    return summary, session_df


def summarise_staging_files(file_paths: list, cfg: dict, label: str, log: logging.Logger,
                            part_bytes: int = _PARTITION_RAW_BYTES):
    """Same result as concatenating every staging file and summarising the lot, without ever holding
    more than one partition in memory (the full set is ~10 million rows and does not fit in RAM).

    Pass 1 reads each staging file once and spills its rows, reduced to the columns that are used, into
    partition files keyed by batch_Id. Every metric is per batch, so a batch is always summarised whole.
    Pass 2 summarises one partition at a time with the unchanged clean/summary functions and joins the
    (small) results. Returns (batch summary, session-wise frame or None, total raw rows read)."""
    import shutil

    if not file_paths:
        raise PipelineError("No staging files to combine.")
    paths       = sorted(file_paths)
    total_bytes = sum(Path(p).stat().st_size for p in paths)
    n_parts     = max(1, -(-total_bytes // part_bytes))
    out_dir     = Path(cfg["paths"]["output_folder"])
    spill_dir   = out_dir / "_spill"

    reserve = cfg["pipeline"]["min_free_disk_mb"] * 1024 ** 2
    free    = shutil.disk_usage(out_dir).free
    need    = int(total_bytes * _SPILL_DISK_FRACTION) + reserve
    if free < need:
        raise PipelineError(
            f"Not enough disk to summarise {len(paths)} staging files ({total_bytes / 1e9:.1f} GB): "
            f"{free / 1e9:.1f} GB free, need about {need / 1e9:.1f} GB for temporary partition files."
        )

    raw_path = None
    if cfg["pipeline"]["save_combined_raw_csv"]:
        if free >= total_bytes + need:
            ts       = datetime.now(IST).strftime("%Y%m%d_%H%M%S")
            raw_path = out_dir / f"attendance_raw_{label}_{ts}.csv"
        else:
            log.warning(
                f"Skipping the combined raw CSV: it would be ~{total_bytes / 1e9:.1f} GB and only "
                f"{free / 1e9:.1f} GB is free. The per-day files in the staging folder are the raw data."
            )

    log.info(f"Summarising {len(paths)} staging file(s), {total_bytes / 1e9:.2f} GB, in {n_parts} partition(s)...")
    if spill_dir.exists():
        shutil.rmtree(spill_dir)
    spill_dir.mkdir(parents=True)

    total_rows, session_col, cols, raw_fh, wrote_header = 0, None, None, None, False
    try:
        if raw_path:
            raw_fh = open(raw_path, "w", encoding="utf-8-sig", newline="")

        # ── Pass 1: split by batch, keep only the columns that are used ──
        for i, p in enumerate(paths, start=1):
            try:
                raw = pd.read_csv(p, low_memory=False)
            except Exception as e:
                log.warning(f"Could not read {p}: {e}. Skipping.")
                continue
            if session_col is None:
                session_col = resolve_session_id_column(raw, cfg, log)
                cols        = list(dict.fromkeys(_NEEDED_COLUMNS + [session_col]))
            total_rows += len(raw)
            if raw_fh is not None:
                raw.to_csv(raw_fh, index=False, header=not wrote_header)
                wrote_header = True

            kept            = raw.reindex(columns=cols)
            kept[_ROW_HASH] = _row_hash(raw)
            batch_ids       = pd.to_numeric(kept["batch_Id"], errors="coerce").fillna(0).astype("int64")
            for part, chunk in kept.groupby((batch_ids % n_parts).to_numpy()):
                spill = spill_dir / f"part_{int(part):04d}.csv"
                chunk.to_csv(spill, mode="a", header=not spill.exists(), index=False)
            if i % 250 == 0:
                log.info(f"  read {i}/{len(paths)} staging files ({total_rows:,} rows)")

        if session_col is None:
            raise PipelineError("All staging files were unreadable.")
        if raw_fh is not None:
            raw_fh.close()
            raw_fh = None
            log.info(f"Combined raw CSV saved: {raw_path}")
        log.info(f"Read {total_rows:,} rows.")

        # ── Pass 2: one partition at a time ──
        summaries, sessions, observed = [], [], set()
        spills = sorted(spill_dir.glob("part_*.csv"))
        for n, spill in enumerate(spills, start=1):
            log.info(f"Summarising partition {n}/{len(spills)}...")
            part = pd.read_csv(spill, low_memory=False)
            summary, session_df, seen = _summarise(part, session_col, cfg, log, validate=False)
            del part
            summaries.append(summary)
            if session_df is not None:
                sessions.append(session_df)
            observed |= seen
    finally:
        if raw_fh is not None:
            raw_fh.close()
        shutil.rmtree(spill_dir, ignore_errors=True)

    validate_present_value(pd.DataFrame({"studentAttendanceStatus": sorted(observed)}), cfg)
    # Restore the row order a single whole-dataset run produces (batch order), so later sorts tie-break identically.
    summary    = pd.concat(summaries, ignore_index=True).sort_values("batch_Id").reset_index(drop=True)
    session_df = (pd.concat(sessions, ignore_index=True).sort_values(["batch_Id", "session_number"]).reset_index(drop=True)
                  if sessions else None)
    return summary, session_df, total_rows


def remove_staging_files(file_paths: list, log: logging.Logger):
    removed = 0
    for p in file_paths:
        try:
            Path(p).unlink()
            removed += 1
        except OSError:
            pass
    log.info(f"Removed {removed} staging file(s).")


# ── CLEAN + VALIDATE ──

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


# ── BATCH SUMMARY COMPUTATION ──

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


# ── MAIN ──

def main():
    parser = argparse.ArgumentParser(description="Edmingle report_type=55 attendance pipeline")
    parser.add_argument("--config",    default="config.yaml")
    parser.add_argument("--date",      type=str)
    parser.add_argument("--from",      dest="from_date", type=str)
    parser.add_argument("--to",        dest="to_date",   type=str)
    parser.add_argument("--from-file", dest="from_file", type=str)
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--verbose",   action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    log = setup_logging(cfg, verbose=args.verbose)

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
        lock = acquire_lock(cfg["paths"]["lock_file"], log)  # held until the process exits
    except PipelineError as e:
        log.critical(str(e))
        sys.exit(1)

    def _graceful_exit(sig, _frame):
        log.warning(f"Received {signal.Signals(sig).name}. Finished days are kept -- re-run to resume.")
        sys.exit(0)

    signal.signal(signal.SIGINT,  _graceful_exit)
    signal.signal(signal.SIGTERM, _graceful_exit)

    run_start = datetime.now(IST)
    exit_code = 0
    dry_dir   = None  # where a dry run stages its simulated rows (removed at the end)

    try:
        Path(cfg["paths"]["output_folder"]).mkdir(parents=True, exist_ok=True)

        # ── Offline mode ─────────────────────────────────────────────────
        if args.from_file:
            log.info(f"--from-file: loading {args.from_file}")
            raw_df = pd.read_csv(args.from_file, low_memory=False)
            label  = Path(args.from_file).stem
            staging_files = []
            log.info("Running batch attendance summary...")
            summary, session_df = summarise_frame(raw_df, cfg, log)
            total_rows = len(raw_df)

        # ── Online mode ──────────────────────────────────────────────────
        else:
            check_disk_space(cfg, log)
            dates = build_date_list(args, cfg)
            label = dates[0] if len(dates) == 1 else f"{dates[0]}_to_{dates[-1]}"
            if args.dry_run:  # simulated rows must never be mistaken for real staging files later
                dry_dir = Path(cfg["paths"]["staging_folder"]) / "_dry_run"
                cfg["paths"]["staging_folder"] = str(dry_dir)

            staging_files = run_pull_loop(dates, requests.Session(), cfg, log, dry_run=args.dry_run)
            if not staging_files:
                log.warning("No data fetched for any date. Nothing to summarise.")
                sys.exit(0)

            log.info("Running batch attendance summary...")
            summary, session_df, total_rows = summarise_staging_files(staging_files, cfg, label, log)

        # ── Outputs ──────────────────────────────────────────────────────
        summary = summary.sort_values("batchName").reset_index(drop=True)

        ts           = datetime.now(IST).strftime("%Y%m%d_%H%M%S")
        summary_path = (
            Path(cfg["paths"]["output_folder"])
            / f"batch_attendance_summary_{label}_{ts}.csv"
        )
        summary.to_csv(summary_path, index=False, encoding="utf-8-sig")
        log.info(f"Summary: {len(summary):,} batches -- {summary_path}")

        session_path = None
        if session_df is not None:
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

        if cfg["pipeline"]["cleanup_staging_after_combine"] and staging_files:
            remove_staging_files(staging_files, log)

        # ── Completion ────────────────────────────────────────────────────
        elapsed = (datetime.now(IST) - run_start).total_seconds()
        stats = {
            "Date range":          label,
            "Student filter":      ("Active only"
                                    if cfg["pipeline"].get("exclude_inactive_students", True)
                                    else "All statuses"),
            "Batches in summary":  str(len(summary)),
            "Total raw rows":      f"{total_rows:,}",
            "Summary file":        str(summary_path),
            "Session-wise file":   str(session_path) if session_path else "disabled",
            "Elapsed":             f"{elapsed / 60:.1f} minutes",
            "Completed at":        datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
        }
        for k, v in stats.items():
            log.info(f"  {k}: {v}")

        notify(cfg, "completion", "Run Complete", stats, log)
        log.info("Pipeline complete.")

    except PipelineError as e:
        log.critical(f"Pipeline error: {e}")
        notify(cfg, "critical", str(e), {
            "Error":   str(e),
            "Elapsed": f"{(datetime.now(IST) - run_start).total_seconds() / 60:.1f} min",
        }, log)
        exit_code = 1

    except Exception as e:
        tb = traceback.format_exc()
        log.critical(f"Unexpected error: {e}\n{tb}")
        notify(cfg, "critical", f"Unexpected error: {e}", {
            "Error":     f"Unexpected error: {e}",
            "Traceback": tb[:2000],
            "Elapsed":   f"{(datetime.now(IST) - run_start).total_seconds() / 60:.1f} min",
        }, log)
        exit_code = 1

    finally:
        if dry_dir:
            shutil.rmtree(dry_dir, ignore_errors=True)

    sys.exit(exit_code)


if __name__ == "__main__":
    main()
