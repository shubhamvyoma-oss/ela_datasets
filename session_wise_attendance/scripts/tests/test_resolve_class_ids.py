import pandas as pd

from resolve_class_ids import (
    parse_retry_after_seconds,
    courses_array_to_records,
    load_already_processed,
    append_rows_to_csv,
    OUTPUT_COLUMNS,
)


def test_parse_retry_after_seconds_parses_minutes_with_buffer():
    text = "You are blocked. Try after 29.93 minutes."
    assert parse_retry_after_seconds(text) == int(29.93 * 60) + 15


def test_parse_retry_after_seconds_falls_back_when_unparseable():
    from resolve_class_ids import DEFAULT_BLOCK_WAIT_SECONDS
    assert parse_retry_after_seconds("some unrelated error body") == DEFAULT_BLOCK_WAIT_SECONDS


def test_courses_array_to_records_maps_fields_and_joins_associated_masterbatches():
    courses_array = [{
        "class_id": 27255,
        "tutor_name": "Dr. X",
        "tutor_id": 111,
        "total_classes": 10,
        "completed": 4,
        "cancelled": 1,
        "num_users": 250,
        "associated_masterbatches": [12443, 70503],
    }]
    records = courses_array_to_records(courses_array)
    assert len(records) == 1
    r = records[0]
    assert r["class_id"] == 27255
    assert r["tutor_name"] == "Dr. X"
    assert r["completed_classes"] == 4
    assert r["cancelled_classes"] == 1
    assert r["associated_masterbatches"] == "12443,70503"


def test_courses_array_to_records_handles_empty_list():
    assert courses_array_to_records([]) == []


def test_courses_array_to_records_handles_non_list_associated_masterbatches():
    courses_array = [{"class_id": 1, "associated_masterbatches": None}]
    records = courses_array_to_records(courses_array)
    assert records[0]["associated_masterbatches"] is None


def test_load_already_processed_returns_empty_set_when_file_missing(tmp_path):
    missing = tmp_path / "does_not_exist.csv"
    assert load_already_processed(missing) == set()


def test_load_already_processed_reads_batch_ids(tmp_path):
    out_path = tmp_path / "class_id_lookup.csv"
    df = pd.DataFrame([
        {"batch_id": 100, "class_id": 1},
        {"batch_id": 100, "class_id": 2},  # same batch, two subjects
        {"batch_id": 200, "class_id": 3},
    ])
    df.to_csv(out_path, index=False)
    assert load_already_processed(out_path) == {100, 200}


def test_append_rows_to_csv_writes_header_once_then_appends(tmp_path):
    out_path = tmp_path / "out.csv"
    row1 = [{col: None for col in OUTPUT_COLUMNS}]
    row1[0]["batch_id"] = 1
    row2 = [{col: None for col in OUTPUT_COLUMNS}]
    row2[0]["batch_id"] = 2

    append_rows_to_csv(row1, out_path)
    append_rows_to_csv(row2, out_path)

    result = pd.read_csv(out_path, encoding="utf-8-sig")
    assert list(result["batch_id"]) == [1, 2]
    assert list(result.columns) == OUTPUT_COLUMNS
