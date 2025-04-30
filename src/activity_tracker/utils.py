import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession


def query_with_spark(query: str):
    """
    Run a SQL query against SQL Server using PySpark via the JTDS JDBC driver.
    """
    query = f"({query}) AS sub"

    load_dotenv()
    SERVER = os.getenv("SERVER")
    DATABASE = os.getenv("DATABASE")
    USERNAME = os.getenv("USERNAME")
    PASSWORD = os.getenv("PASSWORD")
    DOMAIN = os.getenv("DOMAIN")
    JDBC_DRIVER = os.getenv("JDBC_DRIVER")
    JDBC_URL = f"jdbc:jtds:sqlserver://{SERVER}:1433/{DATABASE};domain={DOMAIN}"

    # Start Spark session
    spark = (
        SparkSession.builder.appName("SQLServer-JTDS-Query")
        .config("spark.driver.extraClassPath", JDBC_DRIVER)
        .getOrCreate()
    )

    # Execute query
    df = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", query)
        .option("user", USERNAME)
        .option("password", PASSWORD)
        .option("driver", "net.sourceforge.jtds.jdbc.Driver")
        .load()
    )

    return df
