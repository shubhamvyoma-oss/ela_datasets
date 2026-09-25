#!/usr/bin/env python3
"""
edmingle_export.py -- pulls row-level enrollment data from Edmingle's /reports/enrollment endpoint
(report_details_type=3) for a date range, in <=chunk_days windows (Edmingle blocks large single-shot
ranges), and writes it to one CSV. Each window is downloaded to its own file in
<output>.chunks/ and renamed when complete, so an interrupted run resumes by skipping the finished
windows (at most one window is re-downloaded). When every window is done they are joined into the
final CSV and the chunk files are deleted. See ../ENROLLMENTS_REPORTS.md for the design and known issues.

Usage:
    python3 edmingle_export.py --start-date 01-01-2010 --end-date 06-08-2026

    (--output is optional; if omitted, output goes to the fixed filename
    output/edmingle_enrollment_report.csv, replaced on every completed run --
    pass --output explicitly if you need to keep a specific run's file.)
"""

import argparse
import csv
import io
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

import requests

# Shared bytecode cache across every ela_datasets/ pipeline -- must be set before any local import.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from edmingle_api import PermanentAPIError, fetch_page
from edmingle_constants import DATE_FMT, FIELDS

import common
from common import RollingRateLimiter, format_duration

SCRIPT_DIR = Path(__file__).resolve().parent

# Defaults for every run (there is no config file); no CLI flag -- override at the call site if ever needed.
DEFAULTS = {
    "chunk_days": 30,
    "per_page": 200,
    "max_calls_per_minute": 30,
    "request_timeout_seconds": 30,
    "initial_retry_delay_seconds": 2,
    "maximum_retry_delay_seconds": 60,
    "rate_limit_block_seconds": 300,
}


def send_mail(subject: str, body: str, logger: logging.Logger) -> None:
    # Notifications config lives in this pipeline's own folder (a sibling of scripts/).
    notifications = common.load_notifications("enrollments_reports")
    common.send_mail(notifications, subject, body, logger)


def setup_logging(log_path: Path) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)],
        force=True,
    )
    return logging.getLogger("edmingle_export")


def build_chunks(start_date: str, end_date: str, chunk_days: int) -> list[tuple[str, str]]:
    """Split [start_date, end_date] (DD-MM-YYYY, inclusive) into windows of at most chunk_days days each,
    as (start, end) strings in the DD-MM-YYYY format the API expects."""
    start = datetime.strptime(start_date, DATE_FMT)
    end = datetime.strptime(end_date, DATE_FMT)
    if start > end:
        sys.exit("--start-date must not be after --end-date.")
    chunks = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=chunk_days - 1), end)
        chunks.append((cur.strftime(DATE_FMT), chunk_end.strftime(DATE_FMT)))
        cur = chunk_end + timedelta(days=1)
    return chunks


def count_rows(path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as fh:
        return sum(1 for _ in csv.reader(fh))


class EdmingleExportRun:
    def __init__(
        self,
        start_date: str,
        end_date: str,
        output: str | None = None,
        api_key: str | None = None,
        org_id: int | None = None,
        session: requests.Session | None = None,
        logger=None,
    ) -> None:
        creds = common.edmingle_settings()
        self.config = dict(DEFAULTS)
        self.start_date = start_date
        self.end_date = end_date
        self.api_key = api_key or creds["api_key"]
        self.org_id = org_id or creds["organization_id"]

        self.output_path = Path(output) if output else SCRIPT_DIR.parent / "output" / "edmingle_enrollment_report.csv"
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.chunk_dir = self.output_path.with_suffix(".chunks")
        self.log_path = self.output_path.with_suffix(".log")

        self.logger = logger or setup_logging(self.log_path)
        self.session = session or requests.Session()
        self.rate_limiter = RollingRateLimiter(int(self.config["max_calls_per_minute"]), 60.0, self.logger)

    def _chunk_path(self, chunk_start: str, chunk_end: str) -> Path:
        return self.chunk_dir / f"{chunk_start}_{chunk_end}.csv"

    def _download_chunk(self, chunk_start: str, chunk_end: str, label: str) -> int:
        """Fetch every page of one window into a .part file, then rename it to the chunk file.
        Returns the number of rows."""
        path = self._chunk_path(chunk_start, chunk_end)
        part = path.with_name(path.name + ".part")
        rows_written, page = 0, 1
        with part.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=FIELDS, extrasaction="ignore")
            while True:
                payload = fetch_page(
                    self.session, self.api_key, self.org_id, chunk_start, chunk_end, page, int(self.config["per_page"]),
                    timeout=float(self.config["request_timeout_seconds"]),
                    initial_retry_delay=float(self.config["initial_retry_delay_seconds"]),
                    maximum_retry_delay=float(self.config["maximum_retry_delay_seconds"]),
                    rate_limit_block_seconds=float(self.config["rate_limit_block_seconds"]),
                    rate_limiter=self.rate_limiter, logger=self.logger,
                )
                rows = payload["result"]["studentlist"]
                for row in rows:
                    writer.writerow({k: ("" if row.get(k) is None else row.get(k)) for k in FIELDS})
                rows_written += len(rows)
                self.logger.info(f"[{label} p{page}] wrote {len(rows)} rows ({rows_written:,} in this chunk)")
                if not rows or not payload.get("page_context", {}).get("has_more_page", False):
                    break
                page += 1
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(part, path)
        return rows_written

    def run(self) -> None:
        chunks = build_chunks(self.start_date, self.end_date, int(self.config["chunk_days"]))
        self.logger.info(f"Full range {self.start_date} -> {self.end_date} split into "
                         f"{len(chunks)} chunk(s) of up to {self.config['chunk_days']} days each.")

        self.chunk_dir.mkdir(parents=True, exist_ok=True)
        wanted = {self._chunk_path(*c).name for c in chunks}
        for leftover in self.chunk_dir.iterdir():
            if leftover.name not in wanted:  # another range's chunk, or a half-written one from a crash
                leftover.unlink()
        done = sum(self._chunk_path(*c).exists() for c in chunks)
        is_resume = done > 0

        self.logger.info(f"{'Resuming' if is_resume else 'Starting'} enrollment export: {self.start_date} -> {self.end_date}")
        self.logger.info(f"Output: {self.output_path}  |  per_page={self.config['per_page']}  |  "
                         f"chunk_days={self.config['chunk_days']}  |  max_calls_per_minute={self.config['max_calls_per_minute']}")
        send_mail(
            subject=f"Edmingle export {'resumed' if is_resume else 'started'}",
            body=(f"{'Resumed' if is_resume else 'Started'} pulling enrollment data {self.start_date} -> {self.end_date} "
                  f"in {len(chunks)} chunk(s) of {self.config['chunk_days']} days each.\n"
                  f"Output file: {self.output_path}\n"
                  f"{f'{done}/{len(chunks)} chunks were already downloaded.' if is_resume else ''}"),
            logger=self.logger,
        )

        run_started = time.monotonic()
        fetched = 0
        total_rows = 0
        for i, (chunk_start, chunk_end) in enumerate(chunks, start=1):
            path = self._chunk_path(chunk_start, chunk_end)
            if path.exists():
                total_rows += count_rows(path)
                continue
            self.logger.info(f"--- Chunk {i}/{len(chunks)}: {chunk_start} -> {chunk_end} ---")
            total_rows += self._download_chunk(chunk_start, chunk_end, f"chunk {i}/{len(chunks)}")
            fetched += 1
            if fetched == 1 or i % 10 == 0 or i == len(chunks):
                elapsed = time.monotonic() - run_started
                remaining = elapsed / fetched * (len(chunks) - i)
                self.logger.info(f"Progress: {i}/{len(chunks)} chunks; elapsed {format_duration(elapsed)}; "
                                 f"ETA {format_duration(remaining)}")

        # Join the chunks into the final file. The previous final file stays untouched until this is complete.
        tmp = self.output_path.with_name(self.output_path.name + ".tmp")
        header = io.StringIO()
        csv.writer(header).writerow(FIELDS)
        with tmp.open("wb") as out:
            out.write(header.getvalue().encode("utf-8"))
            for chunk in chunks:
                with self._chunk_path(*chunk).open("rb") as fh:
                    shutil.copyfileobj(fh, out)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, self.output_path)
        shutil.rmtree(self.chunk_dir)

        self.logger.info(f"Done. Wrote {total_rows:,} rows total to {self.output_path}")
        send_mail(
            subject="Edmingle export completed",
            body=(f"Finished pulling enrollment data {self.start_date} -> {self.end_date} "
                  f"across {len(chunks)} chunk(s) of {self.config['chunk_days']} days each.\n"
                  f"Rows written: {total_rows:,}\n"
                  f"Output file: {self.output_path}"),
            logger=self.logger,
        )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-date", help="DD-MM-YYYY")
    parser.add_argument("--end-date", help="DD-MM-YYYY")
    parser.add_argument("--output", default=None,
                        help="Output CSV path (default: output/edmingle_enrollment_report.csv next to this script's folder)")
    parser.add_argument("--api-key", default=None, help="Override api_key from credentials.yaml")
    parser.add_argument("--org-id", default=None, type=int, help="Override organization_id from credentials.yaml")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    if not args.start_date or not args.end_date:
        sys.exit("--start-date and --end-date are required (DD-MM-YYYY).")

    run = EdmingleExportRun(
        args.start_date, args.end_date,
        output=args.output, api_key=args.api_key, org_id=args.org_id,
    )

    try:
        run.run()
    except PermanentAPIError as exc:
        # Already logged with full detail inside fetch_page. Retrying won't help (bad credentials,
        # wrong org id, wrong endpoint) -- stop and notify immediately rather than retrying blindly.
        run.logger.error("Edmingle export stopped: permanent API error")
        send_mail(
            subject="Edmingle export FAILED (permanent error)",
            body=(f"edmingle_export.py stopped and will not retry on its own: {exc}\n\n"
                  f"This is not a transient issue (bad credentials, wrong org id, or a "
                  f"wrong/changed endpoint) -- fix the underlying problem before re-running.\n"
                  f"Check {run.log_path} on the VPS for the full detail."),
            logger=run.logger,
        )
        return 1
    except KeyboardInterrupt:
        run.logger.warning("Run interrupted; the next run will skip the chunks already downloaded.")
        return 130
    except Exception:
        run.logger.exception("Edmingle export run failed")
        send_mail(
            subject="Edmingle export CRASHED",
            body=(f"edmingle_export.py crashed unexpectedly.\n\n"
                  f"Check {run.log_path} on the VPS for the traceback.\n\n"
                  f"To resume: SSH into the VPS, cd into this pipeline's scripts/ folder, and "
                  f"re-run the same command -- it skips the chunks already downloaded "
                  f"rather than starting over."),
            logger=run.logger,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
