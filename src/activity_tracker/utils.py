import os
import pathlib
import re
from collections import defaultdict

import pandas as pd
import pyodbc
import yaml
from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession

# Load environment variables once
load_dotenv()
SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
DOMAIN = os.getenv("DOMAIN")
JDBC_DRIVER = os.getenv("JDBC_DRIVER")
JDBC_URL = f"jdbc:jtds:sqlserver://{SERVER}:1433/{DATABASE};domain={DOMAIN}"


def get_spark_session(app_name: str = "SQLServer-JTDS-Query") -> SparkSession:
    """Initialize and return a Spark session configured for JDBC with JTDS."""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.extraClassPath", JDBC_DRIVER)
        .getOrCreate()
    )


def query_with_spark(query: str, spark: SparkSession = None) -> DataFrame:
    """Run a SQL query against SQL Server using PySpark via JTDS JDBC driver."""
    if not spark:
        spark = get_spark_session()
    query = f"({query}) AS sub"

    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", query)
        .option("user", USERNAME)
        .option("password", PASSWORD)
        .option("driver", "net.sourceforge.jtds.jdbc.Driver")
        .load()
    )


def read_csv_with_spark(
    file_path: str | pathlib.Path, spark: SparkSession = None
) -> DataFrame:
    """Read a CSV file using Spark with header and inferred schema."""
    if not spark:
        spark = get_spark_session()
    return (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(str(file_path))
    )


def create_connection() -> pyodbc.Connection:
    """Create a domain-authenticated connection to SQL Server using FreeTDS."""
    return pyodbc.connect(
        f"DRIVER=FreeTDS;"
        f"SERVER={SERVER};"
        f"DATABASE={DATABASE};"
        f"UID={DOMAIN}\\{USERNAME};"
        f"PWD={PASSWORD};"
        f"PORT=1433;"
        f"ntlmv2*=yes;"
    )


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
