import os
import pathlib
import re
from collections import defaultdict

import pandas as pd

# import pyodbc
import yaml
from dotenv import load_dotenv

# from pyspark.sql import DataFrame, SparkSession

# Load environment variables once
load_dotenv()
# SERVER = os.getenv("SERVER")
# DATABASE = os.getenv("DATABASE")
# USERNAME = os.getenv("USERNAME")
# PASSWORD = os.getenv("PASSWORD")
# DOMAIN = os.getenv("DOMAIN")
# JDBC_DRIVER = os.getenv("JDBC_DRIVER")
# JDBC_URL = f"jdbc:jtds:sqlserver://{SERVER}:1433/{DATABASE};domain={DOMAIN}"


# def get_spark_session(app_name: str = "SQLServer-JTDS-Query") -> SparkSession:
#     """Initialize and return a Spark session configured for JDBC with JTDS."""
#     return (
#         SparkSession.builder.appName(app_name)
#         .config("spark.driver.extraClassPath", JDBC_DRIVER)
#         .getOrCreate()
#     )


# def query_with_spark(query: str, spark: SparkSession = None) -> DataFrame:
#     """Run a SQL query against SQL Server using PySpark via JTDS JDBC driver."""
#     if not spark:
#         spark = get_spark_session()
#     query = f"({query}) AS sub"

#     return (
#         spark.read.format("jdbc")
#         .option("url", JDBC_URL)
#         .option("dbtable", query)
#         .option("user", USERNAME)
#         .option("password", PASSWORD)
#         .option("driver", "net.sourceforge.jtds.jdbc.Driver")
#         .load()
#     )


# def read_csv_with_spark(
#     file_path: str | pathlib.Path, spark: SparkSession = None
# ) -> DataFrame:
#     """Read a CSV file using Spark with header and inferred schema."""
#     if not spark:
#         spark = get_spark_session()
#     return (
#         spark.read.option("header", "true")
#         .option("inferSchema", "true")
#         .csv(str(file_path))
#     )


# def create_connection() -> pyodbc.Connection:
#     """Create a domain-authenticated connection to SQL Server using FreeTDS."""
#     return pyodbc.connect(
#         f"DRIVER=FreeTDS;"
#         f"SERVER={SERVER};"
#         f"DATABASE={DATABASE};"
#         f"UID={DOMAIN}\\{USERNAME};"
#         f"PWD={PASSWORD};"
#         f"PORT=1433;"
#         f"ntlmv2*=yes;"
#     )


def load_data_dictionary(excel_path: str | pathlib.Path) -> dict:

    # Load the Excel file (MDE clinical data.xlsx)
    xls = pd.ExcelFile(excel_path)
    df_dict = pd.read_excel(xls, sheet_name=xls.sheet_names[2])
    df_dict.columns = ["variable", "code_label"]
    df_dict["variable"] = df_dict["variable"].ffill()
    df_dict = df_dict.dropna(subset=["code_label"])

    # Store key-value pairs in a dictionary
    data_dictionary = defaultdict(dict)
    for _, row in df_dict.iterrows():
        variable = str(row["variable"]).strip().lower().replace(" ", "_")
        match = re.match(r"^(\d+)\s*=\s*(.+)$", str(row["code_label"]).strip())
        if match:
            code, label = match.groups()
            data_dictionary[variable][int(code)] = (
                label.strip().lower().replace(" ", "_")
            )

    return dict(data_dictionary)


def load_data_mapper(yaml_path):
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)


def normalize_subject_id(subject_id: str) -> str:
    """Normalize subject ID by padding the number with zeros.

    Args:
        subject_id: Subject ID string (e.g. "MDE1" or "MDE01")

    Returns:
        Normalized subject ID with padded zeros (e.g. "MDE001")
    """
    return re.sub(r"MDE(\d+)", lambda m: f"MDE{int(m.group(1)):03d}", subject_id)


def make_unique_columns(cols):
    """Make column names unique by appending numbers to duplicates."""
    seen = {}
    result = []
    for col in cols:
        base = col
        if base not in seen:
            seen[base] = 1
            result.append(base)
        else:
            count = seen[base]
            new_col = f"{base}_{count}"
            while new_col in seen:
                count += 1
                new_col = f"{base}_{count}"
            seen[base] = count + 1
            seen[new_col] = 1
            result.append(new_col)
    return result


def sanitize_column(col):
    """Clean column names by removing parentheses, normalizing spacing, and removing special characters."""
    base = col.split(".")[0]
    # Remove anything in parentheses (and the parentheses themselves)
    base = re.sub(r"\(.*?\)", "", base)
    # Normalize spacing and other characters
    name = "_".join(base.strip().split()).lower()
    # Remove remaining special characters (slashes, dashes, percent signs)
    name = name.replace("/", "").replace("-", "").replace("%", "")
    return name


def stack_visits(df, group_label):
    """Process Excel data by stacking visits into long format."""
    df = df.rename(columns={"Participant ID": "subject_id"})
    visit_markers = sorted(
        [
            col
            for col in df.columns
            if col.startswith("V") and len(col) == 2 and col[1].isdigit()
        ],
        key=lambda x: int(x[1:]),
    )

    all_visits = []

    for i, marker in enumerate(visit_markers):
        start = df.columns.get_loc(marker) + 1
        end = (
            df.columns.get_loc(visit_markers[i + 1])
            if i + 1 < len(visit_markers)
            else len(df.columns)
        )
        visit_cols = df.columns[start:end].tolist()

        visit_df = df[["subject_id"] + visit_cols].copy()
        cleaned_cols = [sanitize_column(col) for col in visit_cols]
        visit_df.columns = ["subject_id"] + make_unique_columns(cleaned_cols)

        visit_df.insert(1, "group", group_label)
        visit_df.insert(2, "visit_id", int(marker[1:]))

        all_visits.append(visit_df)

    return pd.concat(all_visits, ignore_index=True)
