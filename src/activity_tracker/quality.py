import pathlib

import datajoint as dj
import pandas as pd


def calculate_missing_days_per_subject(df_measurement):
    """Calculate the number of missing days per subject in measurement data."""

    data_dir = pathlib.Path(dj.config["custom"]["root_data_dir"])
    data_path = data_dir / "raw/daily/dailyActivity_merged.csv"
    df_measurement = pd.read_csv(data_path)
    missing_days_per_subject = {}

    for subject_id in df_measurement["Id"].unique():
        subject_data = df_measurement[df_measurement["Id"] == subject_id]

        start_date = subject_data["ActivityDate"].min()
        end_date = subject_data["ActivityDate"].max()

        date_range = pd.date_range(start=start_date, end=end_date, freq="D")
        actual_dates = set(subject_data["ActivityDate"].dt.date)
        expected_dates = set(date_range.date)

        missing_dates = expected_dates - actual_dates
        missing_days_per_subject[subject_id] = len(missing_dates)

    missing_df = pd.DataFrame(
        list(missing_days_per_subject.items()), columns=["Subject", "MissingDays"]
    )
    missing_df = missing_df.sort_values("MissingDays", ascending=False)

    return missing_df
