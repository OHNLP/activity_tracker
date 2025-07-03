import pathlib

import datajoint as dj
import pandas as pd

from .. import utils
from . import get_schema_name

# Define schema
schema = dj.schema(get_schema_name("subject"))


@schema
class Subject(dj.Manual):
    """
    Subject information table for the activity tracker study.

    This table contains demographic and baseline information for study participants.
    """

    definition = """
    # Study participants
    subject_id: varchar(10)  # Subject identifier (e.g., MDE001)
    ---
    group=null: enum('control', 'exercise')  # Study group assignment
    sex=null: enum('m', 'f', '')                 # Sex assigned at birth
    age=null: float                # Age in years at enrollment
    ethnicity=null: enum('hisp', 'non_hisp', '') # Hispanic ethnicity
    race=null: enum('white', 'black', 'am_in_al_na', '')  # Race category
    monthly_income=null: float           # Monthly income in dollars
    education=null: float                # Years of education
    marital_status=null: enum('married', 'divorced', 'widowed', 'separated', 'never_married', '')  # Marital status
    living_situation=null: enum('alone', 'with_spouse', 'with_family', 'with_others', '')  # Living situation
    """

    @classmethod
    def ingest(cls) -> None:
        """
        Populate the Subject table from the Excel file and data mapper, using paths from dj.config.
        """
        data_dir = pathlib.Path(dj.config["custom"]["root_data_dir"])
        excel_path = data_dir / "raw/MDE clinical data.xlsx"
        mapper_path = data_dir / "raw/data_mapper.yml"

        xls = pd.ExcelFile(excel_path)
        df_control = pd.read_excel(xls, sheet_name=xls.sheet_names[0])
        df_exercise = pd.read_excel(xls, sheet_name=xls.sheet_names[1])
        df_control["group"] = "control"
        df_exercise["group"] = "exercise"
        df_combined = pd.concat([df_control, df_exercise], ignore_index=True)

        subject_columns = [
            "Participant ID",
            "group",
            "Sex",
            "Age",
            "Eth",
            "Race",
            "Mth Inc",
            "Educ",
            "Mari",
            "Liv Sit",
        ]
        df_subject = df_combined[subject_columns].copy()
        df_subject.columns = (
            df_subject.columns.str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace(".", "_", regex=False)
        )

        # Rename columns
        rename_map = {
            "participant_id": "subject_id",
            "eth": "ethnicity",
            "mth_inc": "monthly_income",
            "educ": "education",
            "mari": "marital_status",
            "liv_sit": "living_situation",
        }
        df_subject = df_subject.rename(columns=rename_map)
        df_subject = df_subject.sort_values(by="subject_id").reset_index(drop=True)

        # Apply data dictionary to map integer values to strings
        data_dictionary = utils.load_data_mapper(mapper_path)
        for column in data_dictionary:
            if column in df_subject.columns:
                df_subject[column] = df_subject[column].map(data_dictionary[column])
        df_subject["subject_id"] = df_subject["subject_id"].apply(
            utils.normalize_subject_id
        )  # pad subject_id with zeros

        # Replace all 'unknown' with '' (empty string) for all columns
        df_subject = df_subject.replace("unknown", "")

        # Replace NaN with None for SQL NULLs
        df_subject = df_subject.where(pd.notnull(df_subject), None)

        # Insert into database
        cls.insert(df_subject)
        print(f"Inserted {len(df_subject)} subject records")
