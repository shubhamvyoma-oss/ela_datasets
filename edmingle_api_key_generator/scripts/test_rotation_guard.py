import contextlib
import io
import os
import tempfile
import unittest
from pathlib import Path

import edmingle_rotation_guard as guard


class FakeMachine:
    """A throwaway repo plus a fake /proc, so no real process is looked at."""

    def __init__(self):
        self.tmp = tempfile.TemporaryDirectory()
        base = Path(self.tmp.name)
        self.proc = base / "proc"
        self.proc.mkdir()
        self.repo = base / "repo"
        for name in ("attendance", "enrollments_reports", "edmingle_api_key_generator"):
            (self.repo / name / "scripts").mkdir(parents=True)
        self.elsewhere = base / "elsewhere"
        self.elsewhere.mkdir()

    def process(self, pid, args, cwd):
        directory = self.proc / str(pid)
        directory.mkdir()
        (directory / "cmdline").write_bytes(("\0".join(args) + "\0").encode())
        os.symlink(cwd, directory / "cwd")

    def scan(self):
        return guard.find_running_pipelines(repo_root=self.repo, proc_root=self.proc)

    def close(self):
        self.tmp.cleanup()


class RotationGuardTests(unittest.TestCase):
    def setUp(self):
        self.machine = FakeMachine()
        self.addCleanup(self.machine.close)
        self.scripts = self.machine.repo / "attendance" / "scripts"

    def test_finds_a_pipeline_started_with_a_relative_script_name(self):
        self.machine.process(101, ["python3", "attendance.py", "--from", "2026-01-01"], self.scripts)
        self.assertEqual(self.machine.scan(), [(101, os.path.join("attendance", "scripts", "attendance.py"))])

    def test_finds_a_pipeline_started_from_another_directory_with_a_venv_python(self):
        script = str(self.machine.repo / "enrollments_reports" / "scripts" / "edmingle_export.py")
        self.machine.process(102, ["/x/.venv/bin/python3", "-u", script], self.machine.elsewhere)
        self.assertEqual([pid for pid, _ in self.machine.scan()], [102])

    def test_ignores_things_that_only_mention_a_pipeline_script(self):
        self.machine.process(201, ["grep", "-n", "x", "attendance.py"], self.scripts)
        self.machine.process(202, ["python3", "-m", "py_compile", "attendance.py"], self.scripts)
        self.machine.process(203, ["python3", "-c", "print(1)", "attendance.py"], self.scripts)
        self.machine.process(204, ["vim", "attendance.py"], self.scripts)
        self.assertEqual(self.machine.scan(), [])

    def test_ignores_the_generator_itself_and_scripts_outside_the_repo(self):
        own = self.machine.repo / "edmingle_api_key_generator" / "scripts"
        self.machine.process(301, ["python3", "edmingle_generate_api_key.py"], own)
        self.machine.process(302, ["python3", "attendance.py"], self.machine.elsewhere)
        self.assertEqual(self.machine.scan(), [])

    def test_never_counts_its_own_process(self):
        self.machine.process(os.getpid(), ["python3", "attendance.py"], self.scripts)
        self.assertEqual(self.machine.scan(), [])

    def test_returns_none_when_proc_is_unavailable(self):
        missing = self.machine.proc / "nope"
        self.assertIsNone(guard.find_running_pipelines(repo_root=self.machine.repo, proc_root=missing))

    def test_refuses_to_rotate_and_names_the_running_pipeline(self):
        self.machine.process(101, ["python3", "attendance.py"], self.scripts)
        with self.assertRaisesRegex(guard.RotationBlockedError, r"attendance.py \(PID 101\)"):
            guard.ensure_no_pipeline_running(repo_root=self.machine.repo, proc_root=self.machine.proc)

    def test_force_allows_rotation_but_warns(self):
        self.machine.process(101, ["python3", "attendance.py"], self.scripts)
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            guard.ensure_no_pipeline_running(force=True, repo_root=self.machine.repo, proc_root=self.machine.proc)
        self.assertIn("--force", stderr.getvalue())

    def test_allows_rotation_when_nothing_is_running(self):
        guard.ensure_no_pipeline_running(repo_root=self.machine.repo, proc_root=self.machine.proc)

    def test_cannot_check_is_a_warning_not_a_crash(self):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            guard.ensure_no_pipeline_running(repo_root=self.machine.repo, proc_root=self.machine.proc / "nope")
        self.assertIn("cannot check", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
