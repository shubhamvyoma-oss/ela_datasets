import argparse
import contextlib
import io
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

    def test_email_says_verified_only_when_it_was(self):
        _, plain = emailmod.build_api_key_email("b" * 32, "ELATEAM")
        _, checked = emailmod.build_api_key_email("b" * 32, "ELATEAM", verified=True)
        self.assertNotIn("Verified", plain)
        self.assertIn("Verified", checked)

    def test_notice_email_goes_to_the_configured_recipients(self):
        FakeSMTP.instances = []
        with patch.object(emailmod.smtplib, "SMTP", FakeSMTP):
            emailmod.send_notice_email("[Vyoma Edmingle] SKIPPED", "attendance is running", "app-password")
        message, _, recipients = FakeSMTP.instances[0].sent
        self.assertEqual(recipients, list(settings.EMAIL_TO))
        self.assertIn("attendance is running", message.get_content())


class FakeGetSession:
    def __init__(self, *outcomes):
        self.outcomes = list(outcomes)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


GOOD = lambda: FakeResponse(200, {"code": "200", "message": "Success"})  # noqa: E731
REJECTED = lambda: FakeResponse(400, {"code": 10002, "message": "invalid.credentials"})  # noqa: E731


class VerifyApiKeyTests(unittest.TestCase):
    KEY = "k" * 32

    def test_accepts_a_key_edmingle_answers_200_to_and_sends_it_in_the_headers(self):
        session = FakeGetSession(GOOD())
        keymod.verify_api_key(self.KEY, session=session, sleep=lambda s: None)
        url, options = session.calls[0]
        self.assertTrue(url.endswith("/short/masterbatch"))
        self.assertEqual(options["headers"]["apikey"], self.KEY)
        self.assertEqual(options["params"]["per_page"], 1)

    def test_retries_in_case_a_new_key_needs_a_moment(self):
        session, slept = FakeGetSession(REJECTED(), GOOD()), []
        keymod.verify_api_key(self.KEY, session=session, sleep=slept.append, delay_seconds=5)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(slept, [5])

    def test_rejected_key_raises_after_all_attempts_and_never_shows_the_key(self):
        session, slept = FakeGetSession(REJECTED(), REJECTED(), REJECTED()), []
        with self.assertRaises(keymod.ApiKeyVerificationError) as raised:
            keymod.verify_api_key(self.KEY, session=session, sleep=slept.append)
        self.assertIn("invalid.credentials", str(raised.exception))
        self.assertNotIn(self.KEY, str(raised.exception))
        self.assertEqual(len(session.calls), 3)
        self.assertEqual(len(slept), 2)

    def test_network_error_then_success_is_accepted(self):
        session = FakeGetSession(keymod.requests.ConnectionError("down"), GOOD())
        keymod.verify_api_key(self.KEY, session=session, sleep=lambda s: None)

    def test_http_200_with_an_error_code_in_the_body_is_not_accepted(self):
        session = FakeGetSession(*[FakeResponse(200, {"code": "400", "message": "bad"})] * 3)
        with self.assertRaises(keymod.ApiKeyVerificationError):
            keymod.verify_api_key(self.KEY, session=session, sleep=lambda s: None)


class MainFlowTests(unittest.TestCase):
    """main() with every network/file/email step replaced, to check ORDER and what is (not) sent."""

    KEY = "n" * 32

    def run_main(self, *, blocked=False, verify_fails=False, force=False, verify_only=False):
        self.calls, self.notices, self.guard_force = [], [], None
        args = argparse.Namespace(check_config=False, verify_only=verify_only, force=force)

        def guard(force=False):
            self.calls.append("guard")
            self.guard_force = force
            if blocked:
                raise keymod.RotationBlockedError("attendance.py (PID 1) is running")

        def verify(key, **kwargs):
            self.calls.append("verify")
            if verify_fails:
                raise keymod.ApiKeyVerificationError("HTTP 400, code 10002, message invalid.credentials")

        def record(name, result=None):
            return lambda *a, **k: self.calls.append(name) or result

        with patch.multiple(
            keymod, parse_args=lambda: args, validate_settings=lambda: None, ensure_no_pipeline_running=guard,
            read_credentials=lambda: ("ELATEAM", "pw"), generate_api_key=record("generate", self.KEY),
            update_shared_api_key=record("save"), verify_api_key=verify,
            send_api_key_email=record("email_key"),
            send_notice_email=lambda subject, body, password: self.notices.append((subject, body)),
        ), patch.object(keymod.settings, "EMAIL_APP_PASSWORD", "app-password"), \
                contextlib.redirect_stderr(io.StringIO()), contextlib.redirect_stdout(io.StringIO()):
            return keymod.main()

    def test_happy_path_runs_guard_first_then_save_then_verify_then_email(self):
        self.assertEqual(self.run_main(), 0)
        self.assertEqual(self.calls, ["guard", "generate", "save", "verify", "email_key"])
        self.assertEqual(self.notices, [])

    def test_a_running_pipeline_stops_everything_before_the_login_call(self):
        self.assertEqual(self.run_main(blocked=True), 2)
        self.assertEqual(self.calls, ["guard"])
        self.assertIn("SKIPPED", self.notices[0][0])

    def test_force_is_passed_to_the_guard(self):
        self.run_main(force=True)
        self.assertTrue(self.guard_force)

    def test_a_rejected_new_key_is_not_emailed_and_the_notice_never_contains_it(self):
        self.assertEqual(self.run_main(verify_fails=True), 1)
        self.assertEqual(self.calls, ["guard", "generate", "save", "verify"])
        self.assertIn("not accepted", self.notices[0][0])
        self.assertNotIn(self.KEY, self.notices[0][1])

    def test_verify_only_never_touches_the_guard_or_rotates(self):
        self.assertEqual(self.run_main(verify_only=True), 0)
        self.assertEqual(self.calls, ["verify"])
        self.assertEqual(self.run_main(verify_only=True, verify_fails=True), 1)
        self.assertEqual(self.notices, [])


if __name__ == "__main__":
    unittest.main()
