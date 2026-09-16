from datetime import datetime, timezone

import pandas as pd

from attendance_crossvalidation import (
    to_unix,
    unix_to_ist,
    parse_retry_after_seconds,
    sessions_to_dataframe,
)


def test_to_unix_treats_date_as_ist_midnight():
    # IST midnight = UTC midnight minus 5.5 hours.
    expected = int(datetime(2023, 9, 21, tzinfo=timezone.utc).timestamp() - 5.5 * 3600)
    assert to_unix("2023-09-21") == expected


def test_unix_to_ist_round_trips_a_known_instant():
    # 2023-09-21 17:00:00 IST -> unix -> back to IST string.
    ts = to_unix("2023-09-21") + 17 * 3600
    assert unix_to_ist(ts) == "2023-09-21 17:00:00"


def test_unix_to_ist_handles_none():
    assert unix_to_ist(None) is None


def test_parse_retry_after_seconds_parses_minutes():
    assert parse_retry_after_seconds("Try after 2 minutes") == 2 * 60 + 15


def test_parse_retry_after_seconds_default_when_unparseable():
    assert parse_retry_after_seconds("no timing info here", default_seconds=999) == 999


def _session(id_, class_id, master_batch_id, gmt_start, gmt_end, total, present, status):
    return {
        "id": id_,
        "class_id": class_id,
        "class_name": "Test Subject",
        "master_batch_id": master_batch_id,
        "master_batch_name": " Test Batch ",
        "class_date": gmt_start,
        "gmt_start_time": gmt_start,
        "gmt_end_time": gmt_end,
        "total": total,
        "present": present,
        "not_marked": max(total - present, 0),
        "taken_by_name": "Tutor A",
        "signin_by_name": "Tutor A",
        "signout_by_name": "Tutor A",
        "individual_batch_attendance": 1,
        "status": status,
    }


def test_sessions_to_dataframe_empty_input_returns_empty_df():
    df = sessions_to_dataframe([])
    assert df.empty


def test_sessions_to_dataframe_computes_expected_fields():
    base = to_unix("2023-09-21")
    classes = [
        _session(1, 100, 500, base, base + 3600, total=200, present=50, status=1),   # SignedIn
        _session(2, 100, 500, base + 86400, base + 86400 + 3600, total=0, present=0, status=3),  # Cancelled
    ]
    df = sessions_to_dataframe(classes)

    assert len(df) == 2
    # session_conducted: status 3 (Cancelled) is in NOT_CONDUCTED_STATUSES.
    conducted_by_session_id = dict(zip(df["session_id"], df["session_conducted"]))
    assert bool(conducted_by_session_id[1]) is True
    assert bool(conducted_by_session_id[2]) is False

    # attendance_pct doesn't crash (becomes NaN, pandas' None) when total is 0.
    pct_by_session_id = dict(zip(df["session_id"], df["attendance_pct"]))
    assert pct_by_session_id[1] == 25.0
    assert pd.isna(pct_by_session_id[2])

    # session_number increments per master_batch_id in chronological order.
    assert list(df.sort_values("session_start_ist")["session_number"]) == [1, 2]

    # master_batch_name is stripped of surrounding whitespace.
    assert (df["master_batch_name"] == "Test Batch").all()


def test_sessions_to_dataframe_unknown_status_conducted():
    base = to_unix("2023-09-21")
    classes = [_session(1, 100, 500, base, base + 3600, total=10, present=5, status=99)]
    df = sessions_to_dataframe(classes)
    assert bool(df.iloc[0]["session_conducted"]) is True  # only 2 (Postponed) / 3 (Cancelled) count as not-conducted
