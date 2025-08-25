import pandas as pd

from activity_tracker.pipeline import visit


def test_visit_dates_increasing():
    """Test that visit dates are always increasing for each subject"""
    # Fetch actual visit data
    df_visit = visit.Visit.fetch(format="frame").reset_index()
    df_sorted = df_visit.sort_values(["subject_id", "date"])
    date_diffs = df_sorted.groupby("subject_id")["date"].diff()

    # Check for negative differences (decreasing dates)
    negative_diffs = date_diffs[date_diffs < pd.Timedelta(0)]

    # Assert that there are no decreasing dates
    assert (
        len(negative_diffs) == 0
    ), f"Found {len(negative_diffs)} decreasing date pairs"

    if len(negative_diffs) > 0:
        problematic_indices = negative_diffs.index
        for idx in problematic_indices:
            subject = df_sorted.loc[idx, "subject_id"]
            current_date = df_sorted.loc[idx, "date"]
            prev_date = df_sorted.loc[idx - 1, "date"] if idx > 0 else None

            if prev_date and idx > 0:
                print(f"Subject {subject}: {prev_date} -> {current_date} (decreasing)")


if __name__ == "__main__":
    test_visit_dates_increasing()
