import pathlib
from datetime import datetime

import datajoint as dj
import pandas as pd

from activity_tracker import utils
from activity_tracker.pipeline.measurement import DailyMeasurement, Frailty
from activity_tracker.pipeline.models import Feature
from activity_tracker.pipeline.subject import Subject
from activity_tracker.pipeline.visit import Visit


def ingest_visit_and_frailty():
    """
    Ingest visit and frailty data from Excel files into DataJoint tables.
    """

    data_dir = pathlib.Path(dj.config["custom"]["root_data_dir"])
    excel_path = data_dir / "raw/MDE clinical data.xlsx"
    mapper_path = data_dir / "data_mapper.yml"
    study_log_path = data_dir / "raw/mde_study_log.csv"  # Add study log path

    # Load data mapper
    data_mapper = utils.load_data_mapper(mapper_path)

    # Load data using the new approach
    xls = pd.ExcelFile(excel_path)

    df_visits_control = utils.stack_visits(pd.read_excel(xls, sheet_name=0), "control")
    df_visits_exercise = utils.stack_visits(
        pd.read_excel(xls, sheet_name=1), "exercise"
    )

    # Process frailty scores
    ffp_cols = ["wt_loss", "weak", "slow", "exhaust", "phys_act"]
    for df in [df_visits_control, df_visits_exercise]:
        # Calculate FFP score - only if ALL components are not null
        df["ffp_score"] = df[ffp_cols].apply(
            lambda row: row.sum() if row.notna().all() else pd.NA, axis=1
        )
        df["gait"] = 4 / df["walk"].replace(0, pd.NA)

    # Combine dataframes
    df = pd.concat([df_visits_control, df_visits_exercise], ignore_index=True)
    df = df.sort_values(by=["subject_id", "visit_id"]).reset_index(drop=True)

    # Convert data types as in notebook
    df["visit_id"] = df["visit_id"].astype("category")
    df["ffp_status"] = (
        df["ffp_status"]
        .apply(lambda x: int(x) if pd.notnull(x) else pd.NA)
        .astype("category")
    )
    # Keep FFP score as is - don't convert pd.NA to float NaN yet
    # df["ffp_score"] = df["ffp_score"].astype(float)

    # Normalize subject IDs
    df["subject_id"] = df["subject_id"].apply(utils.normalize_subject_id)

    # Apply data mapping for ffp_status
    df["ffp_status"] = df["ffp_status"].astype(float).map(data_mapper["ffp_status"])

    # Create binary frailty status (combine 'prefrail' and 'robust' into 'no_frail')
    df["ffp_status_binary"] = df["ffp_status"].replace(
        {"prefrail": "no_frail", "robust": "no_frail"}
    )

    # --- Process visit dates from study log ---
    study_log_df = pd.read_csv(study_log_path)

    # Normalize subject IDs in study log
    study_log_df["subject_id"] = (
        study_log_df["PID"].astype(str).str.replace("-", "").str.upper()
    )
    study_log_df["subject_id"] = study_log_df["subject_id"].apply(
        utils.normalize_subject_id
    )

    # Define visit date column mapping
    visit_date_cols = {
        "Screening": 1,
        "Week 4 (V2)": 2,
        "Week 8 (V3)": 3,
        "Week 12 (V4)": 4,
        "Week 24 (EOS)": 5,
    }

    # Extract visit dates
    visit_dates_df = study_log_df[["subject_id"] + list(visit_date_cols.keys())].rename(
        columns=visit_date_cols
    )
    visit_dates_df = visit_dates_df.dropna(
        subset=list(visit_date_cols.values()), how="all"
    )  # Removes rows with all NaN values

    # Convert all visit date columns to datetime safely
    for col in visit_date_cols.values():
        # Extract only the date part - remove any non-date text
        cleaned_dates = (
            visit_dates_df[col]
            .astype(str)
            .str.strip()
            .str.extract(
                r"(\d{1,2}/\d{1,2}/\d{2,4})", expand=False
            )  # Extract date pattern (2 or 4-digit year)
            .replace("OS", pd.NA)
        )
        # Handle both M/D/YY and M/D/YYYY formats
        visit_dates_df[col] = pd.to_datetime(cleaned_dates, errors="coerce")

    visit_dates_df = visit_dates_df.melt(
        id_vars="subject_id", var_name="visit_id", value_name="date"
    )
    df = df.merge(visit_dates_df, on=["subject_id", "visit_id"], how="left")

    # --- Prepare combined data for insertion ---
    df["date"] = df["date"].dt.date
    df["visit_id"] = df["visit_id"].astype(int)

    # Filter out rows with missing frailty measurements
    df_combined = df[
        df["ffp_score"].notna() & df["ffp_status"].notna()
    ]  # Remove rows with missing frailty data

    # Convert FFP score to float
    df_combined["ffp_score"] = df_combined["ffp_score"].astype(float)

    # --- Insert into database using the same dataframe ---
    Visit.insert(df_combined, skip_duplicates=True, ignore_extra_fields=True)
    Frailty.insert(
        df_combined,
        skip_duplicates=True,
        allow_direct_insert=True,
        ignore_extra_fields=True,
    )
    print(f"Inserted visit and frailty records from {len(df_combined)} rows")


def ingest_daily_measurements():
    """
    Ingest daily measurement data from CSV files into measurement.DailyMeasurement table.
    """

    # Load data
    data_dir = pathlib.Path(dj.config["custom"]["root_data_dir"])
    data_path = data_dir / "raw/daily/dailyActivity_merged.csv"
    df_measurement = pd.read_csv(data_path)

    # Column name sanitization
    df_measurement.columns = [
        utils.camel_to_snake(col) for col in df_measurement.columns
    ]
    df_measurement = df_measurement.rename({"id": "subject_id", "day": "date"}, axis=1)
    df_measurement["subject_id"] = df_measurement["subject_id"].apply(
        utils.normalize_subject_id
    )
    df_measurement["activity_date"] = pd.to_datetime(df_measurement["activity_date"])
    df_measurement.rename(columns={"activity_date": "date"}, inplace=True)
    df_measurement.sort_values(by=["subject_id", "date"], inplace=True)

    # Select only the columns that match the DailyMeasurement table schema
    df_measurement = df_measurement[
        [
            "subject_id",
            "date",
            "total_steps",
            "total_distance",
            "sedentary_minutes",
            "calories_bmr",
        ]
    ]
    # Convert multiple columns to numeric in one line
    df_measurement[
        ["total_steps", "total_distance", "sedentary_minutes", "calories_bmr"]
    ] = df_measurement[
        ["total_steps", "total_distance", "sedentary_minutes", "calories_bmr"]
    ].astype(
        float
    )

    # Sort by subject_id and date
    df_measurement = df_measurement.sort_values(["subject_id", "date"])

    df_measurement = df_measurement.where(pd.notnull(df_measurement), None)

    # Insert into database
    DailyMeasurement.insert(
        df_measurement, skip_duplicates=True, allow_direct_insert=True
    )
    print(f"Inserted daily measurement records from {len(df_measurement)} rows")


def ingest_features(feature_id: int, feature_description: str = ""):

    if feature_id in Feature.fetch("feature_id"):
        print(f"Feature {feature_id} already exists")
        return

    # Load data from datajoint tables
    df_subject = Subject.fetch(format="frame").reset_index()
    df_visit = (Visit.join(Frailty)).fetch(format="frame").reset_index()
    df_measurement = DailyMeasurement.fetch(format="frame").reset_index()

    df_measurement = df_measurement.sort_values(["subject_id", "date"])

    df_measurement[
        ["total_steps", "total_distance", "sedentary_minutes", "calories_bmr"]
    ] = df_measurement[
        ["total_steps", "total_distance", "sedentary_minutes", "calories_bmr"]
    ].apply(
        pd.to_numeric, errors="coerce"
    )

    # Compute features for each visit interval
    visit_ids = sorted(df_visit["visit_id"].unique())
    ml_features = []

    for i in range(len(visit_ids) - 1):
        v_start = visit_ids[i]
        v_end = visit_ids[i + 1]

        # Get visit data for current and next visit
        visit_n = df_visit[df_visit["visit_id"] == v_start][
            ["subject_id", "date", "ffp_status_binary"]
        ].rename(columns={"date": "start_date", "ffp_status_binary": "prev_ffp_status"})
        visit_np1 = df_visit[df_visit["visit_id"] == v_end][
            ["subject_id", "date", "ffp_status_binary"]
        ].rename(
            columns={
                "date": "measurement_date",
                "ffp_status_binary": "target_ffp_status",
            }
        )
        visit_window = visit_n.merge(visit_np1, on="subject_id", how="inner")

        filtered_all = []
        for _, row in visit_window.iterrows():
            # Get measurements between visits
            subset = df_measurement[
                (df_measurement["subject_id"] == row["subject_id"])
                & (df_measurement["date"] >= row["start_date"])
                & (df_measurement["date"] < row["measurement_date"])
            ].copy()

            if not subset.empty:
                subset["measurement_date"] = row["measurement_date"]
                subset["start_date"] = row["start_date"]
                subset["prev_ffp_status"] = row["prev_ffp_status"]
                subset["target_ffp_status"] = row["target_ffp_status"]
                filtered_all.append(subset)

        if not filtered_all:
            continue

        filtered = pd.concat(filtered_all, ignore_index=True)

        # Helper function for longest active streak
        def longest_active_streak(steps):
            streak = max_streak = 0
            for s in steps:
                if s > 0:
                    streak += 1
                    max_streak = max(max_streak, streak)
                else:
                    streak = 0
            return max_streak

        # Group by subject and compute features
        grouped = filtered.groupby(
            [
                "subject_id",
                "start_date",
                "measurement_date",
                "target_ffp_status",
                "prev_ffp_status",
            ]
        )["total_steps"]

        features = grouped.agg(
            total_steps_sum="sum",
            total_steps_mean="mean",
            total_steps_std="std",
            days_exercised=lambda x: (x > 0).sum(),
            interval_length="count",
            longest_active_streak=longest_active_streak,
        ).reset_index()

        # Calculate proportion of days exercised
        features["prop_days_exercised"] = features.apply(
            lambda row: (
                row["days_exercised"] / row["interval_length"]
                if row["interval_length"] > 0
                else 0
            ),
            axis=1,
        )
        features["visit_interval"] = f"{v_start}_to_{v_end}"

        # Add intervention variable - initially set to 0 for all
        features["intervention"] = 0

        ml_features.append(features)

    # Combine all features
    ml_feature_matrix = pd.concat(ml_features, ignore_index=True)
    ml_feature_matrix = ml_feature_matrix.sort_values(
        ["subject_id", "measurement_date"]
    )

    # Add cumulative steps
    ml_feature_matrix["cumulative_steps"] = ml_feature_matrix.groupby("subject_id")[
        "total_steps_sum"
    ].cumsum()

    # Merge subject-level features
    ml_feature_matrix = ml_feature_matrix.merge(df_subject, on="subject_id", how="left")

    ml_feature_matrix["intervention"] = ml_feature_matrix.apply(
        lambda row: (
            1
            if (
                # Exercise group during intervention period (visits 1-4)
                row["group"] == "exercise"
                and row["visit_interval"] in ["1_to_2", "2_to_3", "3_to_4"]
            )
            else 0
        ),
        axis=1,
    )

    # Insert
    Feature.insert1(
        {
            "feature_id": feature_id,
            "feature_matrix": {
                "columns": ml_feature_matrix.columns.tolist(),
                "data": ml_feature_matrix.to_numpy(),
            },
            "ingestion_date": datetime.now(),
            "feature_description": feature_description,
        },
        allow_direct_insert=True,
    )


if __name__ == "__main__":
    # ingest_visit_and_frailty()
    # ingest_daily_measurements()
    ingest_features(feature_id=1)
