"""Refuses to rotate the Edmingle API key while another ela_datasets pipeline is running.

Rotation revokes the old key immediately, and every pipeline loads the key once at startup, so
rotating mid-run would make the running pipeline fail with invalid credentials. Linux only
(reads /proc); anywhere else the check is skipped with a warning. It can only see processes on
the machine it runs on. A pipeline that starts in the instant after this check is not caught.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
OWN_PIPELINE = "edmingle_api_key_generator"


class RotationBlockedError(RuntimeError):
    """Another pipeline is running, so rotating the key now would break it."""


def _script_of(args: list[str]) -> str | None:
    """The .py file a `python <script>` command line runs; None for -m/-c runs and non-Python commands."""
    if not args or not os.path.basename(args[0]).startswith("python"):
        return None
    for arg in args[1:]:
        if arg in ("-m", "-c"):
            return None
        if arg.startswith("-"):
            continue
        return arg if arg.endswith(".py") else None
    return None


def find_running_pipelines(
    repo_root: Path = REPO_ROOT, proc_root: Path = Path("/proc")
) -> list[tuple[int, str]] | None:
    """[(pid, 'attendance/scripts/attendance.py'), ...] for every other pipeline script currently
    running, or None when /proc is unavailable and the check cannot be done."""
    if not proc_root.is_dir():
        return None
    root = repo_root.resolve()
    found = []
    for entry in proc_root.iterdir():
        if not entry.name.isdigit() or int(entry.name) == os.getpid():
            continue
        try:
            args = (entry / "cmdline").read_bytes().decode("utf-8", "replace").split("\0")
            cwd = Path(os.readlink(entry / "cwd"))
        except OSError:
            continue
        script = _script_of(args)
        if script is None:
            continue
        path = Path(script) if os.path.isabs(script) else cwd / script
        path = Path(os.path.normpath(path))
        if (path.parent.name == "scripts" and path.parent.parent.parent == root
                and path.parent.parent.name != OWN_PIPELINE):
            found.append((int(entry.name), str(path.relative_to(root))))
    return sorted(found)


def ensure_no_pipeline_running(force: bool = False, **kwargs) -> None:
    running = find_running_pipelines(**kwargs)
    if running is None:
        print("WARNING: cannot check for running pipelines on this system (no /proc); "
              "make sure none are running before rotating.", file=sys.stderr)
        return
    if not running:
        return
    listing = ", ".join(f"{script} (PID {pid})" for pid, script in running)
    if force:
        print(f"WARNING: rotating anyway (--force) while running: {listing}. "
              f"They will fail with invalid credentials until restarted.", file=sys.stderr)
        return
    raise RotationBlockedError(
        f"Not rotating: these pipelines are running and would break the moment the old key is "
        f"revoked: {listing}. Wait for them to finish and run again, or use --force if you "
        f"accept that they will fail."
    )
