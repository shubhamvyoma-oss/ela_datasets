import json
import unittest
from datetime import UTC, datetime
from unittest.mock import patch

import edmingle_api_key_email as emailmod
import edmingle_api_key_settings as settings
import edmingle_generate_api_key as keymod


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


class FakeSMTP:
    instances = []

    def __init__(self, host, port, timeout):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.logged_in = None
        self.sent = None
        self.__class__.instances.append(self)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def ehlo(self):
        return None

    def starttls(self, context):
        return None

    def login(self, sender, password):
        self.logged_in = (sender, password)

    def send_message(self, message, from_addr, to_addrs):
        self.sent = (message, from_addr, to_addrs)


class ApiKeyGeneratorTests(unittest.TestCase):
    def test_exact_multipart_request_and_key_extraction(self):
        session = FakeSession(FakeResponse(200, {
            "code": 200,
            "message": "Login successful",
            "user": {"apikey": "a" * 32, "username": "ELATEAM", "role": 2},
        }))
        result = keymod.generate_api_key("ELATEAM", "secret", session=session)
        self.assertEqual(result, "a" * 32)
        self.assertEqual(len(session.calls), 1)
        url, options = session.calls[0]
        self.assertEqual(url, settings.EDMINGLE_LOGIN_URL)
        self.assertNotIn("data", options)
        multipart_value = options["files"]["JSONString"]
        self.assertIsNone(multipart_value[0])
        self.assertEqual(
            json.loads(multipart_value[1]),
            {"username": "ELATEAM", "password": "secret"},
        )

    def test_rejected_login_does_not_return_a_key(self):
        session = FakeSession(FakeResponse(200, {
            "code": 400, "message": "invalid.credentials",
        }))
        with self.assertRaisesRegex(keymod.ApiKeyGenerationError, "invalid.credentials"):
            keymod.generate_api_key("ELATEAM", "wrong", session=session)
        self.assertEqual(len(session.calls), 1)

    def test_missing_key_is_rejected(self):
        with self.assertRaisesRegex(keymod.ApiKeyGenerationError, "rejected"):
            keymod.extract_api_key({"code": 200, "message": "Login successful", "user": {}})

    def test_email_contains_full_key_and_no_login_password(self):
        subject, body = emailmod.build_api_key_email(
            "b" * 32,
            "ELATEAM",
            datetime(2026, 8, 25, 10, 0, tzinfo=UTC),
        )
        self.assertIn("New API Key", subject)
        self.assertIn("b" * 32, body)
        self.assertIn("ELATEAM", body)
        self.assertNotIn("secret-login-password", body)

    def test_smtp_delivery_uses_configured_recipients(self):
        FakeSMTP.instances = []
        with patch.object(emailmod.smtplib, "SMTP", FakeSMTP):
            emailmod.send_api_key_email("c" * 32, "ELATEAM", "app-password")
        smtp = FakeSMTP.instances[0]
        self.assertEqual(smtp.logged_in, (settings.EMAIL_FROM, "app-password"))
        self.assertEqual(smtp.sent[2], list(settings.EMAIL_TO))
        self.assertIn("c" * 32, smtp.sent[0].get_content())


if __name__ == "__main__":
    unittest.main()
