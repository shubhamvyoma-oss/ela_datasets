"""Dedicated email construction and delivery for generated Edmingle API keys."""

from __future__ import annotations

import smtplib
import ssl
from datetime import datetime
from email.message import EmailMessage

import edmingle_api_key_settings as settings


class EmailDeliveryError(RuntimeError):
    """The generated API key could not be delivered."""


def build_api_key_email(
    api_key: str,
    username: str,
    generated_at: datetime | None = None,
    verified: bool = False,
) -> tuple[str, str]:
    timestamp = (generated_at or datetime.now().astimezone()).astimezone()
    subject = "[Vyoma Edmingle] New API Key Generated"
    lines = [
        "A new Edmingle API key was generated successfully.",
        "",
        f"Username     : {username}",
        f"Generated at : {timestamp.strftime('%Y-%m-%d %H:%M:%S %Z')}",
        f"API key      : {api_key}",
    ]
    if verified:
        lines.append("Verified     : the new key was accepted by Edmingle right after it was saved")
    return subject, "\n".join([*lines, ""])


def validate_email_settings(email_password: str) -> list[str]:
    recipients = [str(item).strip() for item in settings.EMAIL_TO if str(item).strip()]
    if not settings.EMAIL_FROM or not recipients:
        raise ValueError("Email sender and at least one recipient are required")
    if not settings.SMTP_HOST or settings.SMTP_PORT <= 0:
        raise ValueError("Valid SMTP host and port are required")
    if settings.SMTP_TIMEOUT_SECONDS <= 0:
        raise ValueError("SMTP_TIMEOUT_SECONDS must be positive")
    if not email_password:
        raise ValueError("Gmail app password is required")
    return recipients


def _deliver(subject: str, body: str, recipients: list[str], email_password: str) -> None:
    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = settings.EMAIL_FROM
    message["To"] = ", ".join(recipients)
    message.set_content(body)
    try:
        context = ssl.create_default_context()
        with smtplib.SMTP(
            settings.SMTP_HOST,
            settings.SMTP_PORT,
            timeout=settings.SMTP_TIMEOUT_SECONDS,
        ) as smtp:
            smtp.ehlo()
            smtp.starttls(context=context)
            smtp.ehlo()
            smtp.login(settings.EMAIL_FROM, email_password)
            smtp.send_message(
                message,
                from_addr=settings.EMAIL_FROM,
                to_addrs=recipients,
            )
    except (OSError, smtplib.SMTPException) as error:
        raise EmailDeliveryError(
            f"Email delivery failed: {type(error).__name__}"
        ) from error


def send_api_key_email(api_key: str, username: str, email_password: str, verified: bool = False) -> None:
    """Send the full key once; never print or persist it."""
    recipients = validate_email_settings(email_password)
    subject, body = build_api_key_email(api_key, username, verified=verified)
    _deliver(subject, body, recipients, email_password)


def send_notice_email(subject: str, body: str, email_password: str) -> None:
    """Status email for a skipped or failed rotation. Must never contain the API key."""
    _deliver(subject, body, validate_email_settings(email_password), email_password)
