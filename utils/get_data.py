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

    # Read CSV
    reader = spark.read.option("header", True).option("nullValue", "")
    if schema:
        df = reader.schema(schema).csv(csv_path)
    else:
        df = reader.option("inferSchema", True).csv(csv_path)

    # Cache and materialize
    df.cache()
    start_time = time.time()
    row_count = df.count()
    load_time = time.time() - start_time

    logger.info(
        "Loaded from storage: %d rows, %d columns in %.2fs",
        row_count, len(df.columns), load_time,
    )
    logger.info("Columns: %s", df.columns)

    return df


def get_data_from_snowflake(
    spark: SparkSession,
    query: str,
    sf_options: dict,
) -> DataFrame:
    """
    Read from Snowflake using native Spark-Snowflake connector.

    Parameters
    ----------
    spark : SparkSession
    query : str
        SQL query to execute on Snowflake.
    sf_options : dict
        Snowflake connector options from get_target_connection().
    """
    logger.info("Extracting data from Snowflake (native connector)")
    logger.debug("Query (%d chars): %s", len(query), query[:200])

    df = (
        spark.read
        .format("net.snowflake.spark.snowflake")
        .options(**sf_options)
        .option("query", query)
        .load()
    )

    # Cache and materialize
    df.cache()
    start_time = time.time()
    df.foreach(lambda _: None)  # force full materialization
    extract_time = time.time() - start_time

    count_start = time.time()
    row_count = df.count()
    count_time = time.time() - count_start

    logger.info(
        "Snowflake + cache: %.2fs | Row count: %d in %.2fs",
        extract_time, row_count, count_time,
    )
    logger.info(
        "Extraction complete: %d rows, %d columns",
        row_count, len(df.columns),
    )
    logger.info("Columns: %s", df.columns)

    return df

