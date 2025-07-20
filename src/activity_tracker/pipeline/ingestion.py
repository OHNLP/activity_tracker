import pathlib

import datajoint as dj
import pandas as pd

from activity_tracker import utils
from activity_tracker.pipeline.measurement import DailyMeasurement, Frailty
from activity_tracker.pipeline.visit import Visit


def ingest_visit_and_frailty():
    """
    Ingest visit and frailty data from Excel files into DataJoint tables.
    """

    data_dir = pathlib.Path(dj.config["custom"]["root_data_dir"])
    excel_path = data_dir / "raw/MDE clinical data.xlsx"
    mapper_path = data_dir / "raw/data_mapper.yml"
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

    # Keep all columns - DataJoint will filter what it needs

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
                r"(\d{1,2}/\d{1,2}/\d{2})", expand=False
            )  # Extract date pattern (2-digit year)
            .replace("OS", pd.NA)
        )
        # All dates are in M/D/YY format with 2-digit years
        visit_dates_df[col] = pd.to_datetime(
            cleaned_dates, format="%m/%d/%y", errors="coerce"
        )

    # Melt visit dates to long format
    visit_dates_df = visit_dates_df.melt(
        id_vars="subject_id", var_name="visit_id", value_name="date"
    )

    # Merge visit dates with main dataframe
    df = df.merge(visit_dates_df, on=["subject_id", "visit_id"], how="left")

    # --- Prepare combined data for insertion ---
    df["date"] = df["date"].dt.date
    df["visit_id"] = df["visit_id"].astype(int)

    # Handle missing values first - replace NaN with None for database insertion
    df_combined = df.where(pd.notnull(df), None)

    # Convert FFP score carefully after handling NaN - preserve None for missing values
    df_combined["ffp_score"] = df_combined["ffp_score"].apply(
        lambda x: float(x) if x is not None and pd.notna(x) else None
    )
    df_combined = df_combined[df_combined["date"].notna()]  # drop rows with no date

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
    df_measurement = df_measurement.where(pd.notnull(df_measurement), None)

    # Insert into database
    DailyMeasurement.insert(
        df_measurement, skip_duplicates=True, allow_direct_insert=True
    )
    print(f"Inserted daily measurement records from {len(df_measurement)} rows")


if __name__ == "__main__":
    ingest_visit_and_frailty()
    ingest_daily_measurements()
