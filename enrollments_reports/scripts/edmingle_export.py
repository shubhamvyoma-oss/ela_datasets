#!/usr/bin/env python3
"""
edmingle_export.py

Pulls row-level enrollment data from Edmingle's /reports/enrollment endpoint
(report_details_type=3) for a date range, in <=chunk_days windows (Edmingle
blocks large single-shot ranges — see edmingle_chunker.py), and writes it
to CSV.

Rebuilt around the patterns in edmingle_student_course_sync.py:

  - EdmingleExportRun bundles config/paths/session/logger/rate-limiter as
    instance state, instead of passing a dozen arguments around.
  - Resume uses byte-offset truncation (edmingle_io_utils.truncate_to_offset):
    the checkpoint records the exact CSV file size right after a confirmed,
    flushed+fsynced write. On resume, the CSV is truncated back to that exact
    byte offset before continuing, so any partial write left by a crash is
    discarded rather than trusted.
  - Requests are paced by a RollingRateLimiter (max_calls_per_minute) instead
    of a flat delay, and edmingle_api.fetch_page distinguishes permanent
    errors (stop immediately) from transient ones (retry forever, capped
    backoff) — see edmingle_api.py.
  - Progress is logged with elapsed/ETA using format_duration(), not just a
    running row count.
  - Settings live in edmingle_config.json (with defaults) rather than a long
    list of CLI flags; only --start-date/--end-date genuinely vary per run.

Usage:
    python3 edmingle_export.py --start-date 01-01-2010 --end-date 06-08-2026

    (--output is optional; if omitted, output goes to the fixed filename
    output/edmingle_enrollment_report.csv, overwritten on every run --
    pass --output explicitly if you need to keep a specific run's file.)

Run directly (no watchdog/auto-restart wrapper -- removed 2026-09-23,
matching edmingle_student_course_sync.py's pattern in ela_mis_datasets,
which has never used one). If it crashes or the server restarts, just
re-run the same command: the checkpoint means it resumes cleanly rather
than re-fetching or duplicating data. A permanent error or unexpected
crash emails immediately from inside main()'s own exception handling
before exiting -- see the except blocks below.
"""

import argparse
import csv
import json
import logging
import os
import sys
import time
from pathlib import Path

import requests

# Shared bytecode cache for every ela_datasets/ pipeline -- must be set
# before any local module import below, so this and every module it pulls
# in gets compiled into one shared location instead of a scripts/__pycache__
# folder per pipeline.
sys.pycache_prefix = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", ".pycache")
)

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common import RollingRateLimiter, atomic_write_json

from edmingle_api import PermanentAPIError, fetch_page
from edmingle_chunker import load_or_create_chunk_plan
from edmingle_config import load_config, send_mail
from edmingle_constants import FIELDS
from edmingle_io_utils import format_duration, truncate_to_offset

SCRIPT_DIR = Path(__file__).resolve().parent


def load_checkpoint(path: Path) -> dict | None:
    """Was edmingle_checkpoint.py -- inlined, it was two functions wrapping
    a single atomic_write_json call."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def save_checkpoint(path: Path, data: dict) -> None:
    atomic_write_json(path, data)


def setup_logging(log_path: Path) -> logging.Logger:
    """Was edmingle_logger.py -- inlined; logging.basicConfig covers what
    the hand-rolled handler setup did."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)],
        force=True,
    )
    return logging.getLogger("edmingle_export")


def build_default_output_name(start_date: str, end_date: str) -> str:
    # Fixed filename -- every run overwrites the same output file instead of
    # accumulating a new one per date range (each prior run's full CSV could
    # be 100MB+, so this used to grow storage unbounded). The checkpoint's
    # own start_date/end_date fields (see _resolve_resume_state) already
    # detect a differently-ranged run and correctly start fresh/overwrite,
    # so a static name here is safe: same-range reruns still resume via the
    # checkpoint, differently-ranged runs still get a correct fresh file.
    return "edmingle_enrollment_report.csv"


class EdmingleExportRun:
    def __init__(
        self,
        config_path: Path,
        start_date: str,
        end_date: str,
        output: str | None = None,
        api_key: str | None = None,
        org_id: int | None = None,
        session: requests.Session | None = None,
        sleep=time.sleep,
        logger=None,
    ) -> None:
        self.config_path = Path(config_path).resolve()
        self.config = load_config(self.config_path)
        self.start_date = start_date
        self.end_date = end_date
        self.api_key = api_key or self.config["api_key"]
        self.org_id = org_id or self.config["organization_id"]

        self.output_path = Path(output) if output else self.config_path.parent.parent / "output" / build_default_output_name(
            start_date, end_date)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.checkpoint_path = self.output_path.with_suffix(self.output_path.suffix + ".checkpoint.json")
        self.chunk_plan_path = self.output_path.with_suffix(self.output_path.suffix + ".chunks.json")
        self.log_path = self.output_path.with_suffix(".log")

        self.logger = logger or setup_logging(self.log_path)
        self.session = session or requests.Session()
        self.sleep = sleep
        self.rate_limiter = RollingRateLimiter(
            int(self.config["max_calls_per_minute"]), 60.0, self.logger,
        )

    def _checkpoint_dict(self, chunk_index, last_page_completed, total_written,
                          output_offset, completed) -> dict:
        return {
            "start_date": self.start_date,
            "end_date": self.end_date,
            "chunk_days": self.config["chunk_days"],
            "per_page": self.config["per_page"],
            "chunk_index": chunk_index,
            "last_page_completed": last_page_completed,
            "total_written": total_written,
            "output_offset": output_offset,
            "completed": completed,
        }

    def _resolve_resume_state(self, checkpoint, num_chunks):
        """Returns (is_resume, start_chunk_idx, start_page, total_written,
        output_offset), or None if the checkpoint shows this exact run
        already completed."""
        if not checkpoint:
            return False, 0, 1, 0, None

        same_params = (
            checkpoint.get("start_date") == self.start_date
            and checkpoint.get("end_date") == self.end_date
            and checkpoint.get("chunk_days") == self.config["chunk_days"]
            and checkpoint.get("per_page") == self.config["per_page"]
        )
        if same_params and not checkpoint.get("completed"):
            start_chunk_idx = checkpoint.get("chunk_index", 0)
            start_page = checkpoint.get("last_page_completed", 0) + 1
            total_written = checkpoint.get("total_written", 0)
            output_offset = checkpoint.get("output_offset")
            self.logger.info(f"Resuming from checkpoint: chunk {start_chunk_idx + 1}/{num_chunks}, "
                              f"page {start_page}, {total_written:,} rows already written.")
            return True, start_chunk_idx, start_page, total_written, output_offset

        if checkpoint.get("completed"):
            self.logger.info("Checkpoint shows this exact run already completed. "
                              "Delete the .checkpoint.json file to force a re-run.")
            return None

        self.logger.warning("Checkpoint found but parameters differ from this run; starting fresh "
                             "(existing output file will be overwritten).")
        return False, 0, 1, 0, None

    def run(self) -> None:
        chunks = load_or_create_chunk_plan(self.chunk_plan_path, self.start_date, self.end_date,
                                            int(self.config["chunk_days"]), self.logger)
        self.logger.info(f"Full range {self.start_date} -> {self.end_date} split into "
                          f"{len(chunks)} chunk(s) of up to {self.config['chunk_days']} days each.")

        checkpoint = load_checkpoint(self.checkpoint_path)
        resume_state = self._resolve_resume_state(checkpoint, len(chunks))
        if resume_state is None:
            return
        is_resume, start_chunk_idx, start_page, total_written, output_offset = resume_state

        self.logger.info(f"{'Resuming' if is_resume else 'Starting'} enrollment export: "
                          f"{self.start_date} -> {self.end_date}")
        self.logger.info(f"Output: {self.output_path}  |  per_page={self.config['per_page']}  |  "
                          f"chunk_days={self.config['chunk_days']}  |  "
                          f"max_calls_per_minute={self.config['max_calls_per_minute']}")

        send_mail(
            self.config,
            subject=f"Edmingle export {'resumed' if is_resume else 'started'}",
            body=(f"{'Resumed' if is_resume else 'Started'} pulling enrollment data "
                  f"{self.start_date} -> {self.end_date} in {len(chunks)} chunk(s) of "
                  f"{self.config['chunk_days']} days each.\n"
                  f"Output file: {self.output_path}\n"
                  f"{'Resuming from chunk ' + str(start_chunk_idx + 1) + '/' + str(len(chunks)) + ', page ' + str(start_page) + ', ' + format(total_written, ',') + ' rows already written.' if is_resume else ''}"),
            logger=self.logger,
        )

        if is_resume and self.output_path.exists() and output_offset is not None:
            truncate_to_offset(self.output_path, output_offset)
            self.logger.info(f"Truncated {self.output_path} back to the last confirmed byte "
                              f"offset ({output_offset:,} bytes) before resuming, in case a "
                              f"partial write was left by a crash.")
            file_mode = "a"
        else:
            file_mode = "w"

        run_started = time.monotonic()
        chunks_completed_this_run = 0

        with self.output_path.open(file_mode, newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=FIELDS, extrasaction="ignore")

            if file_mode == "w":
                writer.writeheader()
                fh.flush()
                os.fsync(fh.fileno())
                output_offset = self.output_path.stat().st_size
                save_checkpoint(self.checkpoint_path,
                                 self._checkpoint_dict(0, 0, 0, output_offset, False))

            for chunk_idx in range(start_chunk_idx, len(chunks)):
                chunk_start, chunk_end = chunks[chunk_idx]
                page = start_page if chunk_idx == start_chunk_idx else 1
                self.logger.info(f"--- Chunk {chunk_idx + 1}/{len(chunks)}: "
                                  f"{chunk_start} -> {chunk_end} ---")

                while True:
                    payload = fetch_page(
                        self.session, self.api_key, self.org_id, chunk_start, chunk_end,
                        page, int(self.config["per_page"]),
                        timeout=float(self.config["request_timeout_seconds"]),
                        initial_retry_delay=float(self.config["initial_retry_delay_seconds"]),
                        maximum_retry_delay=float(self.config["maximum_retry_delay_seconds"]),
                        rate_limit_block_seconds=float(self.config["rate_limit_block_seconds"]),
                        rate_limiter=self.rate_limiter,
                        logger=self.logger,
                        sleep_fn=self.sleep,
                    )

                    rows = payload["result"]["studentlist"]
                    page_context = payload.get("page_context", {})

                    if rows:
                        for row in rows:
                            writer.writerow({k: ("" if row.get(k) is None else row.get(k))
                                              for k in FIELDS})
                        fh.flush()
                        os.fsync(fh.fileno())
                        total_written += len(rows)
                        has_more = page_context.get("has_more_page", False)
                    else:
                        has_more = False

                    output_offset = self.output_path.stat().st_size
                    chunk_finished = not has_more
                    save_checkpoint(self.checkpoint_path, self._checkpoint_dict(
                        chunk_idx + 1 if chunk_finished else chunk_idx,
                        0 if chunk_finished else page,
                        total_written, output_offset,
                        chunk_finished and (chunk_idx + 1 == len(chunks)),
                    ))

                    self.logger.info(f"[chunk {chunk_idx + 1}/{len(chunks)} p{page}] "
                                      f"wrote {len(rows)} rows (running total: {total_written:,})")

                    if chunk_finished:
                        break
                    page += 1

                chunks_completed_this_run += 1
                elapsed = time.monotonic() - run_started
                if (chunks_completed_this_run == 1 or (chunk_idx + 1) % 10 == 0
                        or chunk_idx + 1 == len(chunks)):
                    avg = elapsed / chunks_completed_this_run
                    remaining = avg * (len(chunks) - (chunk_idx + 1))
                    self.logger.info(f"Progress: {chunk_idx + 1}/{len(chunks)} chunks; "
                                      f"elapsed {format_duration(elapsed)}; "
                                      f"ETA {format_duration(remaining)}")

        self.logger.info(f"Done. Wrote {total_written:,} rows total to {self.output_path}")

        send_mail(
            self.config,
            subject="Edmingle export completed",
            body=(f"Finished pulling enrollment data {self.start_date} -> {self.end_date} "
                  f"across {len(chunks)} chunk(s) of {self.config['chunk_days']} days each.\n"
                  f"Rows written: {total_written:,}\n"
                  f"Output file: {self.output_path}"),
            logger=self.logger,
        )


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start-date", help="DD-MM-YYYY")
    parser.add_argument("--end-date", help="DD-MM-YYYY")
    parser.add_argument("--output", default=None,
                         help="Output CSV path (default: auto-named from dates, saved next to this script)")
    parser.add_argument("--config", default=str(SCRIPT_DIR / "edmingle_config.json"),
                         help="Path to config JSON (default: %(default)s)")
    parser.add_argument("--api-key", default=None, help="Override api_key from config")
    parser.add_argument("--org-id", default=None, type=int, help="Override organization_id from config")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)

    if not args.start_date or not args.end_date:
        sys.exit("--start-date and --end-date are required (DD-MM-YYYY).")

    run = EdmingleExportRun(
        config_path, args.start_date, args.end_date,
        output=args.output, api_key=args.api_key, org_id=args.org_id,
    )

    try:
        run.run()
    except PermanentAPIError as exc:
        # Already logged with full detail inside fetch_page. Retrying won't
        # help (bad credentials, wrong org id, wrong endpoint) -- stop and
        # notify immediately rather than retrying blindly.
        run.logger.error("Edmingle export stopped: permanent API error")
        send_mail(
            run.config,
            subject="Edmingle export FAILED (permanent error)",
            body=(f"edmingle_export.py stopped and will not retry on its own: {exc}\n\n"
                  f"This is not a transient issue (bad credentials, wrong org id, or a "
                  f"wrong/changed endpoint) -- fix the underlying problem before re-running.\n"
                  f"Check {run.log_path} on the VPS for the full detail."),
            logger=run.logger,
        )
        return 1
    except KeyboardInterrupt:
        run.logger.warning("Run interrupted; the next run will resume from the saved checkpoint.")
        return 130
    except Exception:
        run.logger.exception("Edmingle export run failed")
        send_mail(
            run.config,
            subject="Edmingle export CRASHED",
            body=(f"edmingle_export.py crashed unexpectedly.\n\n"
                  f"Check {run.log_path} on the VPS for the traceback.\n\n"
                  f"To resume: SSH into the VPS, cd into this pipeline's scripts/ folder, and "
                  f"re-run the same command -- it will resume from the last saved checkpoint "
                  f"rather than starting over."),
            logger=run.logger,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
