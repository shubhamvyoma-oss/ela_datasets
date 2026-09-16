"""
edmingle_checkpoint.py

Reads and writes the <output>.checkpoint.json resume pointer. Writes go
through atomic_write_json (temp file + fsync + os.replace), so a crash or
power-loss mid-write can never leave a truncated or corrupt checkpoint —
it either has the old contents or the new ones, never something in between.
"""

import json
from pathlib import Path

from edmingle_io_utils import atomic_write_json


def load_checkpoint(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def save_checkpoint(path: Path, data: dict) -> None:
    atomic_write_json(path, data)
