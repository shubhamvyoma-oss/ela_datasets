from datetime import datetime, timezone

import pandas as pd

from build_session_attendance import to_unix, load_already_processed, append_df_to_csv


def test_to_unix_treats_date_as_ist_midnight():
    expected = int(datetime(2023, 9, 21, tzinfo=timezone.utc).timestamp() - 5.5 * 3600)
    assert to_unix("2023-09-21") == expected


def test_load_already_processed_returns_empty_set_when_file_missing(tmp_path):
    assert load_already_processed(tmp_path / "missing.csv") == set()


def test_load_already_processed_reads_class_ids(tmp_path):
    out_path = tmp_path / "session_wise_attendance_data.csv"
    df = pd.DataFrame([
        {"class_id": 27255, "session_id": 1},
        {"class_id": 27255, "session_id": 2},
        {"class_id": 27256, "session_id": 3},
    ])
    df.to_csv(out_path, index=False)
    assert load_already_processed(out_path) == {27255, 27256}


def test_append_df_to_csv_writes_header_once_then_appends(tmp_path):
    out_path = tmp_path / "out.csv"
    append_df_to_csv(pd.DataFrame([{"class_id": 1, "present": 5}]), out_path)
    append_df_to_csv(pd.DataFrame([{"class_id": 2, "present": 6}]), out_path)

    result = pd.read_csv(out_path, encoding="utf-8-sig")
    assert list(result["class_id"]) == [1, 2]
