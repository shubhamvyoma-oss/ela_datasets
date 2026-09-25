"""
edmingle_api.py -- fetches a single (chunk, page) from Edmingle's enrollment report endpoint through
common.get_json: permanent errors (400/401/403/404) raise immediately; a 429 waits
`rate_limit_block_seconds` and resets the rate limiter; everything else transient retries forever with
capped exponential backoff (a permanent error stops the run with an email, and the checkpoint makes a
restart cheap).
"""

import logging
import sys
from pathlib import Path
from typing import Any

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from edmingle_constants import ENROLLMENT_PATH

from common import PermanentAPIError, RollingRateLimiter, edmingle_settings, get_json  # noqa: F401 -- PermanentAPIError is re-exported

ENROLLMENT_URL = edmingle_settings()["base_url"] + ENROLLMENT_PATH


def _valid(data: Any) -> bool:
    return (isinstance(data, dict) and data.get("code") == 200
            and isinstance(data.get("result", {}).get("studentlist"), list))


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
    return get_json(
        ENROLLMENT_URL, headers={"apikey": api_key, "orgid": str(org_id)}, params=params, session=session,
        timeout=timeout, delay=initial_retry_delay, max_delay=maximum_retry_delay,
        block_seconds=rate_limit_block_seconds, rate_limiter=rate_limiter, validate=_valid,
        label=f"[chunk {chunk_start}..{chunk_end} page {page}]", logger=logger,
    )
