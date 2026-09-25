"""
edmingle_io_utils.py

truncate_to_offset() is genuine to this pipeline (used on resume to cut a CSV back to the last
checkpointed byte offset). format_duration and atomic_write_json live in common.py and are
re-exported here for the modules that import them from this file.
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common import atomic_write_json, format_duration  # noqa: F401 -- re-exported


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
