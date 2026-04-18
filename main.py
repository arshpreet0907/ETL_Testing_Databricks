"""
main.py — Azure Databricks Job Entry Point
--------------------------------------------
ETL validation pipeline running on Azure Databricks with dedicated clusters.

Source: CSV + schema JSON from Azure Blob Storage (wasbs://)
Target: Snowflake via native Spark-Snowflake connector
Output: diff_report.csv written back to Azure Blob Storage
Secrets: Azure Key Vault via Databricks secret scope "etl-secrets"
"""

import logging
import os
import sys
import time
from datetime import timedelta

logging.basicConfig(
    format="[%(asctime)s] %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
_log = logging.getLogger("etl_pipeline")

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION — Job Parameters via Databricks Widgets
# ═══════════════════════════════════════════════════════════════

from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

spark = SparkSession.getActiveSession() or SparkSession.builder.appName("ETL_Validation").getOrCreate()
dbutils = DBUtils(spark)

# Define widgets with defaults (Databricks Jobs pass values as parameters)
dbutils.widgets.text("TABLE_NAME", "warranty_claims")
dbutils.widgets.text("SUB_PATH", "xl")
dbutils.widgets.text("STORAGE_ACCOUNT", "etlstorage0907")
dbutils.widgets.text("CONTAINER", "etl-source-data")
dbutils.widgets.text("VERIFY_SCHEMA", "true")
dbutils.widgets.text("PK_FILTER_MODE", "full")
dbutils.widgets.text("DATE_WATERMARK_MODE", "full")
dbutils.widgets.text("PK_RANGE_LOWER", "")
dbutils.widgets.text("PK_RANGE_UPPER", "")
dbutils.widgets.text("DATE_FROM", "")
dbutils.widgets.text("DATE_FROM_COL", "")
dbutils.widgets.text("DATE_TO", "")
dbutils.widgets.text("DATE_TO_COL", "")

TABLE_NAME = dbutils.widgets.get("TABLE_NAME")
SUB_PATH = dbutils.widgets.get("SUB_PATH")
STORAGE_ACCOUNT = dbutils.widgets.get("STORAGE_ACCOUNT")
CONTAINER = dbutils.widgets.get("CONTAINER")
VERIFY_SCHEMA = dbutils.widgets.get("VERIFY_SCHEMA").lower() == "true"
PK_FILTER_MODE = dbutils.widgets.get("PK_FILTER_MODE")
DATE_WATERMARK_MODE = dbutils.widgets.get("DATE_WATERMARK_MODE")

# PK range (optional)
_pk_lower = dbutils.widgets.get("PK_RANGE_LOWER")
_pk_upper = dbutils.widgets.get("PK_RANGE_UPPER")
PK_RANGE = {
    "lower": int(_pk_lower) if _pk_lower else None,
    "upper": int(_pk_upper) if _pk_upper else None,
}
PK_SET = set()

# Date watermark (optional)
DATE_FROM = dbutils.widgets.get("DATE_FROM") or None
DATE_FROM_COL = dbutils.widgets.get("DATE_FROM_COL") or None
DATE_TO = dbutils.widgets.get("DATE_TO") or None
DATE_TO_COL = dbutils.widgets.get("DATE_TO_COL") or None

EXCLUDE_COLS = ["load_ts", "batch_id"]

# ═══════════════════════════════════════════════════════════════
# AZURE BLOB STORAGE — Configure Spark for wasbs:// access
# ═══════════════════════════════════════════════════════════════

_blob_key = dbutils.secrets.get("etl-secrets", "blob-storage-key")
spark.conf.set(
    f"spark.hadoop.fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net",
    _blob_key,
)

BLOB_BASE = f"wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net"

SOURCE_CSV_PATH = f"{BLOB_BASE}/{TABLE_NAME}/{SUB_PATH}/source_raw.csv"
SOURCE_SCHEMA_JSON = f"{BLOB_BASE}/{TABLE_NAME}/{SUB_PATH}/source_raw.schema.json"
SOURCE_DB_SCHEMA_JSON = f"{BLOB_BASE}/{TABLE_NAME}/{SUB_PATH}/source_db_schema.json"

OUTPUT_DIR = f"{BLOB_BASE}/output/{TABLE_NAME}/{SUB_PATH}"
REPORT_CSV = f"{OUTPUT_DIR}/diff_report.csv"

_log.info("Table        : %s", TABLE_NAME)
_log.info("Sub-path     : %s", SUB_PATH)
_log.info("Storage      : %s/%s", STORAGE_ACCOUNT, CONTAINER)
_log.info("Source CSV   : %s", SOURCE_CSV_PATH)
_log.info("Output dir   : %s", OUTPUT_DIR)

# ═══════════════════════════════════════════════════════════════
# SPARK CONFIG
# ═══════════════════════════════════════════════════════════════

try:
    spark.conf.set("spark.sql.debug.maxToStringFields", 500)
except Exception:
    pass
try:
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
except Exception:
    pass

# ═══════════════════════════════════════════════════════════════
# ADD PROJECT TO PYTHON PATH (for Repos-based execution)
# ═══════════════════════════════════════════════════════════════

# When running as a Databricks Job with Git source, the repo root is
# automatically on sys.path. For Workspace Repos, add explicitly.
_repo_dir = os.path.dirname(os.path.abspath(__file__))
if _repo_dir not in sys.path:
    sys.path.insert(0, _repo_dir)

# ═══════════════════════════════════════════════════════════════
# BUILD PIPELINE CONTEXT
# ═══════════════════════════════════════════════════════════════

from utils.auto_config import get_table_config
from utils.logger import get_logger
from utils.custom_execution_utils import (
    step_0_verify_source_schema,
    step_1_extract_source,
    step_2_transform,
    step_3_5_verify_target_schema,
    step_4_extract_target,
    step_5_compare,
    build_load_filters,
)

logger = get_logger("etl_pipeline")

config = get_table_config(TABLE_NAME, target_mode="snowflake")

_log.info("Auto-config loaded for: %s", TABLE_NAME)
_log.info("  Source table : %s", config.get("source_table"))
_log.info("  Target table : %s", config.get("target_table"))
_log.info("  Primary Keys : %s", config["primary_keys"])
_log.info("  Transform    : %s", os.path.basename(config["transform_file"]))

SOURCE_FILTER, TARGET_FILTER = build_load_filters(
    config=config,
    pk_filter_mode=PK_FILTER_MODE,
    pk_range=PK_RANGE,
    pk_set=PK_SET,
    date_mode=DATE_WATERMARK_MODE,
    date_from=DATE_FROM,
    date_from_col=DATE_FROM_COL,
    date_to=DATE_TO,
    date_to_col=DATE_TO_COL,
)

_log.info("  Source filter: %s", SOURCE_FILTER["description"])
_log.info("  Target filter: %s", TARGET_FILTER["description"])

pipeline_ctx = dict(
    config=config,
    target_mode="snowflake",
    verify_schema=VERIFY_SCHEMA,
    source_query=None,
    source_query_file=None,
    target_query=None,
    target_query_file=config["target_query_file"],
    transform_file=config["transform_file"],
    primary_keys=config["primary_keys"],
    exclude_cols=EXCLUDE_COLS,
    compare_cols=None,
    source_ddl=config["source_ddl"],
    target_ddl=config["target_ddl"],
    report_csv=REPORT_CSV,
    source_filter=SOURCE_FILTER,
    target_filter=TARGET_FILTER,
    source_csv_path=SOURCE_CSV_PATH,
    source_schema_json=SOURCE_SCHEMA_JSON,
    source_db_schema_json=SOURCE_DB_SCHEMA_JSON,
)


# ═══════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════

def main() -> int:
    """
    Run ETL validation pipeline on Azure Databricks.

    Flow: step 0 → 1 → 2 → 3.5 → 4 → 5
      0   : Verify source schema (schema.json vs DDL)
      1   : Load raw source CSV from Azure Blob Storage
      2   : Transform (cache result, unpersist raw)
      3.5 : Verify target schema (Snowflake live)
      4   : Extract target from Snowflake
      5   : Compare & generate diff_report.csv
    """
    start_time = time.time()

    try:
        _log.info("=" * 60)
        _log.info("ETL Validation Pipeline (Azure Databricks)")
        _log.info("  Table  : %s", TABLE_NAME)
        _log.info("  Target : Snowflake")
        _log.info("  Source : Azure Blob Storage (wasbs://)")
        _log.info("  Source filter : %s", SOURCE_FILTER["description"])
        _log.info("  Target filter : %s", TARGET_FILTER["description"])
        _log.info("=" * 60)

        # Step 0: optional source schema check
        if not step_0_verify_source_schema(spark, pipeline_ctx):
            _log.error("Exiting due to source schema verification failure")
            return 2

        # Step 1: load source from Azure Blob Storage CSV
        source_df = step_1_extract_source(spark, pipeline_ctx)

        # Step 2: transform (caches result, unpersists raw)
        transformed_df = step_2_transform(source_df, pipeline_ctx, target_mode="snowflake")

        # Step 3.5: optional target schema check
        if not step_3_5_verify_target_schema(spark, pipeline_ctx):
            _log.error("Exiting due to target schema verification failure")
            return 2

        # Step 4: extract target from Snowflake (cached inside)
        target_df = step_4_extract_target(spark, pipeline_ctx)

        # Step 5: compare & generate diff report
        t0 = time.time()
        exit_code = step_5_compare(spark, transformed_df, target_df, pipeline_ctx)
        _log.info("Compare and Report time: %.2fs", time.time() - t0)

        # Cache cleanup
        _log.info("Ensuring cached DataFrames are released...")
        if transformed_df.is_cached:
            transformed_df.unpersist()
        if target_df.is_cached:
            target_df.unpersist()
        _log.info("Cache cleanup complete.")

        elapsed = timedelta(seconds=int(time.time() - start_time))
        minutes = elapsed.seconds // 60
        seconds = elapsed.seconds % 60
        _log.info("=" * 60)
        _log.info("Pipeline complete. Exit code: %d", exit_code)
        _log.info("Total time: %d min %d sec", minutes, seconds)
        _log.info("=" * 60)

        return exit_code

    except (FileNotFoundError, ValueError) as exc:
        _log.error("Configuration error: %s", exc)
        return 1
    except KeyboardInterrupt:
        _log.info("Interrupted by user.")
        return 1
    except Exception as exc:
        _log.exception("Unexpected error: %s", exc)
        return 99


if __name__ == "__main__":
    sys.exit(main())

