"""
edmingle_io_utils.py

truncate_to_offset() is genuine to this pipeline (used on resume to cut a
CSV back to the last checkpointed byte offset). utc_now, format_duration,
atomic_write_json, atomic_write_csv and read_csv_rows used to be defined
here too -- they were byte-for-byte identical to the copies in
edmingle_student_course_sync.py, so they now live in common.py and are
re-exported here for backward compatibility with existing imports.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common import (  # noqa: F401 -- re-exported for existing imports
    atomic_write_csv,
    atomic_write_json,
    format_duration,
    read_csv_rows,
    utc_now,
)


def truncate_to_offset(path: Path, offset: int) -> None:
    """Cut a file back to an exact byte offset that's known to correspond to
    a fully-flushed, checkpointed state. Used on resume: if the process died
    mid-write to the CSV after the last checkpoint was saved, this removes
    whatever partial/torn bytes were left dangling, so appending afterwards
    can never produce a corrupt row."""
    with path.open("r+b") as handle:
        handle.truncate(offset)
        handle.flush()
        os.fsync(handle.fileno())
