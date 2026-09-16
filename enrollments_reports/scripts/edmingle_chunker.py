"""
edmingle_chunker.py

One job: turn a (start_date, end_date) range into a list of <=chunk_days
windows, and persist that plan to a .chunks.json file so it's generated
once and reused on every subsequent run/resume instead of being silently
recalculated.
"""

import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path

from edmingle_constants import DATE_FMT
from edmingle_io_utils import atomic_write_json


def build_chunks(start_date: str, end_date: str, chunk_days: int) -> list[tuple[str, str]]:
    """Split [start_date, end_date] (DD-MM-YYYY, inclusive) into windows of
    at most chunk_days days each, returned as a list of (start, end) strings
    in the same DD-MM-YYYY format the API expects."""
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


def load_or_create_chunk_plan(plan_path: Path, start_date: str, end_date: str,
                               chunk_days: int, logger: logging.Logger) -> list[tuple[str, str]]:
    """Chunk plan is generated once and written to <output>.chunks.json.
    Every subsequent run (including resumes after a crash) reads that file
    back instead of recomputing it, so the plan is a fixed, inspectable
    artifact rather than something re-derived from CLI flags each time."""
    if plan_path.exists():
        try:
            data = json.loads(plan_path.read_text())
            same_range = (data.get("start_date") == start_date
                          and data.get("end_date") == end_date
                          and data.get("chunk_days") == chunk_days)
            if same_range:
                chunks = [tuple(c) for c in data["chunks"]]
                logger.info(f"Loaded existing chunk plan: {plan_path} ({len(chunks)} chunks).")
                return chunks
            else:
                logger.warning(f"Chunk plan at {plan_path} was built for a different date range "
                                f"({data.get('start_date')}..{data.get('end_date')}, "
                                f"chunk_days={data.get('chunk_days')}); regenerating it for "
                                f"{start_date}..{end_date}.")
        except Exception as exc:
            logger.warning(f"Could not read existing chunk plan ({exc}); regenerating it.")

    chunks = build_chunks(start_date, end_date, chunk_days)
    atomic_write_json(plan_path, {
        "start_date": start_date,
        "end_date": end_date,
        "chunk_days": chunk_days,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "chunk_count": len(chunks),
        "chunks": chunks,
    })
    logger.info(f"Generated chunk plan: {plan_path} ({len(chunks)} chunks).")
    return chunks
