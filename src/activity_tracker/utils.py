import os

import pyodbc
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Load environment variables once
load_dotenv()

SERVER = os.getenv("SERVER")
DATABASE = os.getenv("DATABASE")
USERNAME = os.getenv("USERNAME")
PASSWORD = os.getenv("PASSWORD")
DOMAIN = os.getenv("DOMAIN")
JDBC_DRIVER = os.getenv("JDBC_DRIVER")
JDBC_URL = f"jdbc:jtds:sqlserver://{SERVER}:1433/{DATABASE};domain={DOMAIN}"


def query_with_spark(query: str):
    """Run a SQL query against SQL Server using PySpark via JTDS JDBC driver."""
    query = f"({query}) AS sub"

    spark = (
        SparkSession.builder.appName("SQLServer-JTDS-Query")
        .config("spark.driver.extraClassPath", JDBC_DRIVER)
        .getOrCreate()
    )

    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", query)
        .option("user", USERNAME)
        .option("password", PASSWORD)
        .option("driver", "net.sourceforge.jtds.jdbc.Driver")
        .load()
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
