"""
utils/csv_writer.py
--------------------
Databricks version — saves a PySpark DataFrame as a single named CSV file.
Handles Spark coalesce(1) → part-file rename on DBFS or local filesystem.

On Databricks, only used for writing the diff report CSV.
"""

import glob
import json
import logging
import os
import shutil

from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def save_dataframe_as_csv(df: DataFrame, file_path: str) -> None:
    """
    Write a PySpark DataFrame to a single CSV file at `file_path`.

    Uses coalesce(1) to produce one part file, then renames it.
    Works on both local filesystem and DBFS (/dbfs/ prefix paths).
    """
    file_path = os.path.normpath(file_path)
    parent_dir = os.path.dirname(file_path) or "."
    os.makedirs(parent_dir, exist_ok=True)

    tmp_dir = file_path + "_tmp_spark"

    logger.info("Writing DataFrame to temporary Spark directory: %s", tmp_dir)

    col_count = len(df.columns)
    row_count = 0

    is_cached = df.is_cached
    if is_cached:
        df.coalesce(1).write.mode("overwrite").option("header", "true").option("nullValue", "").csv(tmp_dir)
    else:
        df_cached = df.coalesce(1).cache()
        row_count = df_cached.count()
        df_cached.write.mode("overwrite").option("header", "true").option("nullValue", "").csv(tmp_dir)
        df_cached.unpersist()

    # Locate the single part file
    part_files = glob.glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        part_files = glob.glob(os.path.join(tmp_dir, "part-*"))

    if not part_files:
        raise FileNotFoundError(
            f"Spark produced no part file in {tmp_dir}. Check Spark logs."
        )

    if len(part_files) > 1:
        raise RuntimeError(
            f"Expected 1 part file after coalesce(1), found {len(part_files)}: {part_files}"
        )

    shutil.move(part_files[0], file_path)

    if is_cached:
        with open(file_path, "r", encoding="utf-8") as f:
            row_count = max(sum(1 for _ in f) - 1, 0)

    logger.info("CSV saved: %s (%d rows, %d columns)", file_path, row_count, col_count)

    # Save schema alongside CSV
    schema_path = os.path.splitext(file_path)[0] + ".schema.json"
    with open(schema_path, "w", encoding="utf-8") as sf:
        sf.write(json.dumps(json.loads(df.schema.json()), indent=2))
    logger.info("Schema saved: %s", schema_path)

    shutil.rmtree(tmp_dir, ignore_errors=True)

