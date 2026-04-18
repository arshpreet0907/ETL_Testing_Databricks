"""
utils/get_data.py
-----------------
Databricks version — two data sources:
  1. Raw source CSV from DBFS/Volumes (pre-extracted from MySQL locally)
  2. Snowflake target via native Spark-Snowflake connector

No JDBC MySQL reads. No partitioning logic.
"""

import json
import os
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType

from utils.logger import get_logger

logger = get_logger(__name__)


def get_data_from_storage(
    spark: SparkSession,
    csv_path: str,
    schema_json_path: str = None,
) -> DataFrame:
    """
    Read data from cloud storage (DBFS, ADLS, Volumes) with schema from .schema.json.

    Used on Databricks for MySQL source data that was extracted locally and uploaded.

    Parameters
    ----------
    spark : SparkSession
    csv_path : str
        Path to CSV file. Supports:
        - "dbfs:/FileStore/..." (DBFS)
        - "/Volumes/catalog/schema/volume/..." (Unity Catalog Volumes)
    schema_json_path : str, optional
        Path to .schema.json file. Auto-detected if None.
    """
    logger.info("Loading data from storage: %s", csv_path)

    # Auto-detect schema JSON path
    if schema_json_path is None:
        # For DBFS paths, convert dbfs:/ to /dbfs/ for Python file I/O
        if csv_path.startswith("dbfs:"):
            local_path = "/dbfs" + csv_path[5:]
        else:
            local_path = csv_path
        schema_json_path = os.path.splitext(local_path)[0] + ".schema.json"

    # Load schema from JSON
    schema = None
    try:
        with open(schema_json_path, "r", encoding="utf-8") as f:
            schema = StructType.fromJson(json.loads(f.read()))
        logger.info("Schema loaded from: %s (%d fields)", schema_json_path, len(schema.fields))
    except FileNotFoundError:
        logger.warning("No schema file found at %s — using inferSchema", schema_json_path)

    # Read CSV — read with inferSchema first, then cast using schema
    # (workaround for PySpark 3.5 bug where .schema() + .csv() + .cache() fails)
    reader = spark.read.option("header", True).option("nullValue", "")
    if schema:
        # Read with inferred types first, then select with proper casts
        df = reader.option("inferSchema", True).csv(csv_path)
        from pyspark.sql.functions import col
        cast_exprs = []
        for field in schema.fields:
            if field.name in df.columns:
                cast_exprs.append(col(field.name).cast(field.dataType).alias(field.name))
        # Keep only columns present in schema
        df = df.select(cast_exprs)
    else:
        df = reader.option("inferSchema", True).csv(csv_path)

    # [SERVERLESS] Cache disabled — uncomment for dedicated cluster
    # df.cache()
    start_time = time.time()
    row_count = df.count()
    load_time = time.time() - start_time

    logger.info(
        "Loaded from storage: %d rows, %d columns in %.2fs",
        row_count, len(df.columns), load_time,
    )
    logger.info("Columns: %s", df.columns)

    return df


def _is_databricks() -> bool:
    """Return True if running on Databricks."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def get_data_from_snowflake(
    spark: SparkSession,
    query: str,
    sf_options: dict,
) -> DataFrame:
    """
    Read from Snowflake.

    On Databricks: uses native Spark-Snowflake connector (no session bug there).
    Locally: uses snowflake-connector-python + Pandas to avoid the session-null
    NullPointerException bug with snowflake-jdbc 3.18 + spark-snowflake 3.1.0.

    Parameters
    ----------
    spark : SparkSession
    query : str
        SQL query to execute on Snowflake.
    sf_options : dict
        Snowflake connector options from get_target_connection().
    """
    if _is_databricks():
        return _read_snowflake_spark_connector(spark, query, sf_options)
    else:
        return _read_snowflake_python_connector(spark, query, sf_options)


def _read_snowflake_spark_connector(
    spark: SparkSession,
    query: str,
    sf_options: dict,
) -> DataFrame:
    """Read via native Spark-Snowflake connector (Databricks)."""
    logger.info("Extracting data from Snowflake (native Spark connector)")
    logger.debug("Query (%d chars): %s", len(query), query[:200])

    df = (
        spark.read
        .format("snowflake")
        .options(**sf_options)
        .option("query", query)
        .load()
    )

    # [SERVERLESS] Cache disabled — uncomment for dedicated cluster
    # df.cache()
    start_time = time.time()
    row_count = df.count()
    extract_time = time.time() - start_time

    logger.info("Snowflake extract: %.2fs | Row count: %d", extract_time, row_count)
    logger.info("Extraction complete: %d rows, %d columns", row_count, len(df.columns))
    logger.info("Columns: %s", df.columns)
    return df


def _read_snowflake_python_connector(
    spark: SparkSession,
    query: str,
    sf_options: dict,
) -> DataFrame:
    """Read via snowflake-connector-python + Pandas (local)."""
    import snowflake.connector

    logger.info("Extracting data from Snowflake (Python connector → Pandas → Spark)")
    logger.debug("Query (%d chars): %s", len(query), query[:200])

    # Map Spark-Snowflake connector keys to snowflake-connector-python keys
    account = sf_options.get("sfURL", "").replace(".snowflakecomputing.com", "")
    if not account:
        account = sf_options.get("sfAccount", "")

    conn_params = {
        "account":   account,
        "user":      sf_options["sfUser"],
        "password":  sf_options["sfPassword"],
        "database":  sf_options["sfDatabase"],
        "schema":    sf_options.get("sfSchema", "PUBLIC"),
        "warehouse": sf_options["sfWarehouse"],
        "role":      sf_options.get("sfRole"),
    }
    conn_params = {k: v for k, v in conn_params.items() if v is not None}

    start_time = time.time()

    conn = snowflake.connector.connect(**conn_params)
    try:
        cur = conn.cursor()
        cur.execute(query)
        pdf = cur.fetch_pandas_all()
    finally:
        conn.close()

    extract_time = time.time() - start_time

    logger.info("Snowflake extract: %.2fs | Row count: %d", extract_time, len(pdf))

    # Replace Pandas NaN/NaT with None so Spark sees proper nulls
    import numpy as np
    pdf = pdf.replace({np.nan: None})
    pdf = pdf.where(pdf.notna(), None)

    df = spark.createDataFrame(pdf)

    logger.info("Extraction complete: %d rows, %d columns", len(pdf), len(df.columns))
    logger.info("Columns: %s", df.columns)

    return df

