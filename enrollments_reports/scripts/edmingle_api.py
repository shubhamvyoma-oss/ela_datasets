"""
edmingle_api.py

Fetches a single (chunk, page) from Edmingle's enrollment report endpoint.
Adapted from the request_json() pattern in edmingle_student_course_sync.py:

  - Paced by a RollingRateLimiter (max_calls_per_minute) rather than a flat
    delay between calls.
  - Permanent errors (400/401/403/404) raise PermanentAPIError immediately —
    a bad API key or wrong org id won't be fixed by retrying, so we stop
    and let the operator fix it rather than retrying forever.
  - HTTP 429 triggers a long rate_limit_block_seconds cool-down (bigger than
    normal backoff) and resets the rate limiter, since a 429 usually means
    Edmingle wants a real pause, not just a shorter gap between calls.
  - Transient errors (408/429/5xx), network errors, invalid JSON, and an
    unexpected response shape are all retried forever with exponential
    backoff capped at maximum_retry_delay — the external watchdog is the
    safety net for a truly stuck process, not a retry counter in here.
  - The response is validated (code == 200, result.studentlist is a list)
    before being trusted, instead of assuming any 200 is well-formed.
"""

import json
import logging
import time
from typing import Any, Callable

import os
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from common import RollingRateLimiter

from edmingle_constants import BASE_URL, PERMANENT_HTTP_STATUSES, TRANSIENT_HTTP_STATUSES


class PermanentAPIError(RuntimeError):
    """Raised when retrying cannot fix the request (bad credentials, bad
    endpoint, etc). Callers should stop the run rather than retry."""


def fetch_page(
    session: requests.Session,
    api_key: str,
    org_id: int,
    chunk_start: str,
    chunk_end: str,
    page: int,
    per_page: int,
    *,
    timeout: float,
    initial_retry_delay: float,
    maximum_retry_delay: float,
    rate_limit_block_seconds: float,
    rate_limiter: RollingRateLimiter,
    logger: logging.Logger,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    params = {
        "start_date": chunk_start,
        "end_date": chunk_end,
        "time_step": 1,
        "report_details_type": 3,
        "page": page,
        "per_page": per_page,
        "sort_order": "D",
        "sort_by": "date_of_enrolment",
        "currency_id": 1,
    }
    headers = {"apikey": api_key, "orgid": str(org_id)}
    context = f"chunk {chunk_start}..{chunk_end} page {page}"

    delay = initial_retry_delay
    attempt = 0
    while True:
        attempt += 1
        rate_limiter.acquire()
        try:
            resp = session.get(BASE_URL, params=params, headers=headers, timeout=timeout)
        except requests.RequestException as exc:
            logger.warning(f"[{context}] network error ({type(exc).__name__}); "
                            f"retrying in {delay:.1f}s (attempt {attempt})")
            sleep_fn(delay)
            delay = min(delay * 2, maximum_retry_delay)
            continue

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After", "")
            try:
                retry_after_seconds = float(retry_after)
            except (TypeError, ValueError):
                retry_after_seconds = 0.0
            block_seconds = max(rate_limit_block_seconds, retry_after_seconds)
            logger.warning(f"[{context}] rate limited (429) on attempt {attempt}; "
                            f"cooling down for {block_seconds:.0f}s")
            sleep_fn(block_seconds)
            rate_limiter.reset()
            delay = initial_retry_delay
            continue

        if resp.status_code in PERMANENT_HTTP_STATUSES:
            message = f"[{context}] permanent HTTP {resp.status_code}: {resp.text[:300]}"
            logger.error(message)
            raise PermanentAPIError(message)

        if resp.status_code != 200:
            classification = "transient" if resp.status_code in TRANSIENT_HTTP_STATUSES else "unexpected"
            logger.warning(f"[{context}] {classification} HTTP {resp.status_code} on attempt "
                            f"{attempt}; retrying in {delay:.1f}s")
            sleep_fn(delay)
            delay = min(delay * 2, maximum_retry_delay)
            continue

        try:
            data = resp.json()
        except (ValueError, json.JSONDecodeError) as exc:
            logger.warning(f"[{context}] invalid JSON on attempt {attempt} ({exc}); "
                            f"retrying in {delay:.1f}s")
            sleep_fn(delay)
            delay = min(delay * 2, maximum_retry_delay)
            continue

        if data.get("code") != 200 or not isinstance(
            data.get("result", {}).get("studentlist"), list
        ):
            logger.warning(f"[{context}] unexpected response shape on attempt {attempt}; "
                            f"retrying in {delay:.1f}s")
            sleep_fn(delay)
            delay = min(delay * 2, maximum_retry_delay)
            continue

        return data
