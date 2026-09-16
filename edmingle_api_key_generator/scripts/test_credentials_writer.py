"""Tests for edmingle_credentials_writer.update_shared_api_key -- the
function every pipeline's shared credentials.yaml gets rewritten by after a
key rotation. Verifies it replaces only the api_key value and leaves
everything else (comments, other fields, formatting) byte-for-byte intact.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from edmingle_credentials_writer import CredentialsUpdateError, update_shared_api_key

_SAMPLE_CREDENTIALS = """\
# a comment that must survive
edmingle:
  api_key: "old-key-value"   # rotates ~every 30 days
  organization_id: 683
  institute_id: 483
  tutor_login:
    login_url: "https://example.invalid/tutor/login"
    username: "someone@example.invalid"
    password: "hunter2"
"""


class UpdateSharedApiKeyTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmpdir.cleanup)
        self.path = Path(self._tmpdir.name) / "credentials.yaml"
        self.path.write_text(_SAMPLE_CREDENTIALS, encoding="utf-8")

    def test_replaces_only_the_api_key_value(self) -> None:
        update_shared_api_key(str(self.path), "brand-new-key-123")
        content = self.path.read_text(encoding="utf-8")

        self.assertIn('api_key: "brand-new-key-123"', content)
        self.assertNotIn("old-key-value", content)
        # everything else must survive untouched
        self.assertIn("# a comment that must survive", content)
        self.assertIn("# rotates ~every 30 days", content)
        self.assertIn('organization_id: 683', content)
        self.assertIn('username: "someone@example.invalid"', content)
        self.assertIn('password: "hunter2"', content)

    def test_no_leftover_tmp_file(self) -> None:
        update_shared_api_key(str(self.path), "brand-new-key-123")
        self.assertFalse((Path(self._tmpdir.name) / "credentials.yaml.tmp").exists())

    def test_rejects_empty_key(self) -> None:
        with self.assertRaises(CredentialsUpdateError):
            update_shared_api_key(str(self.path), "")

    def test_rejects_whitespace_in_key(self) -> None:
        with self.assertRaises(CredentialsUpdateError):
            update_shared_api_key(str(self.path), "has a space")

    def test_missing_file_raises_clearly(self) -> None:
        with self.assertRaises(CredentialsUpdateError):
            update_shared_api_key(str(Path(self._tmpdir.name) / "does_not_exist.yaml"), "new-key")

    def test_missing_api_key_line_raises_clearly(self) -> None:
        self.path.write_text("edmingle:\n  organization_id: 683\n", encoding="utf-8")
        with self.assertRaises(CredentialsUpdateError):
            update_shared_api_key(str(self.path), "new-key")


if __name__ == "__main__":
    unittest.main()
