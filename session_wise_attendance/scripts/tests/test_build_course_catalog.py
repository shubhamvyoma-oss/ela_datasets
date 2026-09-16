import pandas as pd

from build_course_catalog import (
    is_excluded_batch_id,
    mark_latest_batch,
    apply_business_logic,
    compute_bundle_enrollment,
    BATCH_IDS_TO_EXCLUDE,
)


def test_is_excluded_batch_id_true_for_listed_id():
    listed = next(iter(BATCH_IDS_TO_EXCLUDE))
    assert is_excluded_batch_id(listed) is True


def test_is_excluded_batch_id_false_for_unlisted_id():
    assert is_excluded_batch_id(999999999) is False


def test_is_excluded_batch_id_false_for_non_numeric_input():
    assert is_excluded_batch_id("not-a-number") is False
    assert is_excluded_batch_id(None) is False


def test_mark_latest_batch_picks_highest_start_date_per_bundle():
    df = pd.DataFrame([
        {"bundle_id": 1, "batch_id": 10, "start_date": "1000"},
        {"bundle_id": 1, "batch_id": 11, "start_date": "2000"},  # newer -> latest
        {"bundle_id": 2, "batch_id": 20, "start_date": "500"},
    ])
    result = mark_latest_batch(df)
    latest = result[result["Is_Latest_Batch"] == 1].set_index("batch_id")
    assert set(latest.index) == {11, 20}


def test_mark_latest_batch_tiebreaks_by_highest_batch_id():
    df = pd.DataFrame([
        {"bundle_id": 1, "batch_id": 10, "start_date": "1000"},
        {"bundle_id": 1, "batch_id": 15, "start_date": "1000"},  # same date, higher id -> latest
    ])
    result = mark_latest_batch(df)
    latest_ids = result.loc[result["Is_Latest_Batch"] == 1, "batch_id"].tolist()
    assert latest_ids == [15]


def test_apply_business_logic_latest_batch_with_valid_status():
    df = pd.DataFrame([{"Status": "Ongoing", "Is_Latest_Batch": 1}])
    result = apply_business_logic(df)
    assert result.loc[0, "Final_Status"] == "Ongoing"


def test_apply_business_logic_latest_batch_with_invalid_status_is_blank():
    df = pd.DataFrame([{"Status": "SomeOtherStatus", "Is_Latest_Batch": 1}])
    result = apply_business_logic(df)
    assert result.loc[0, "Final_Status"] == ""


def test_apply_business_logic_non_latest_batch_always_completed():
    df = pd.DataFrame([{"Status": "Ongoing", "Is_Latest_Batch": 0}])
    result = apply_business_logic(df)
    assert result.loc[0, "Final_Status"] == "Completed"


def test_compute_bundle_enrollment_sums_per_bundle_and_broadcasts():
    df = pd.DataFrame([
        {"bundle_id": 1, "batch_enrollment_count": 100},
        {"bundle_id": 1, "batch_enrollment_count": 50},
        {"bundle_id": 2, "batch_enrollment_count": 30},
    ])
    result = compute_bundle_enrollment(df)
    assert result.loc[result["bundle_id"] == 1, "bundle_enrollment_count"].tolist() == [150, 150]
    assert result.loc[result["bundle_id"] == 2, "bundle_enrollment_count"].tolist() == [30]
