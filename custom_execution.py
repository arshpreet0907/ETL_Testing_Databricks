"""
custom_execution.py (Local Execution)
--------------------------------------
ETL validation pipeline — runs locally with PySpark local[*].

Source: Enriched CSV from output/{TABLE_NAME}/{SUB_PATH}/
Target: Snowflake via native Spark-Snowflake connector
Output: diff_report.csv in the same output sub-path

Snowflake credentials are loaded from .env file.

FILTER QUICK REFERENCE
======================
PK_FILTER_MODE: "full" | "pk_range" | "pk_set"
DATE_WATERMARK_MODE: "full" | "range"
"""

import os
import sys
import time
from datetime import timedelta
from typing import Literal, Optional, Set

from dotenv import load_dotenv

load_dotenv()  # Load .env for Snowflake credentials

from utils.connections.spark_session import get_spark_session
from utils.connections.target_connection import set_snowflake_config
from utils.auto_config import get_table_config, list_available_tables
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

logger = get_logger(__name__)


# ============================================================================
# SECTION 0 — SNOWFLAKE CREDENTIALS (from .env)
# ============================================================================

set_snowflake_config({
    "sfAccount":   os.getenv("SF_ACCOUNT"),
    "sfUser":      os.getenv("SF_USER"),
    "sfPassword":  os.getenv("SF_PASSWORD"),
    "sfDatabase":  os.getenv("SF_DATABASE"),
    "sfSchema":    os.getenv("SF_SCHEMA", "PUBLIC"),
    "sfWarehouse": os.getenv("SF_WAREHOUSE"),
    "sfRole":      os.getenv("SF_ROLE"),
})


# ============================================================================
# SECTION 1 — TABLE & MODE CONFIGURATION
# ============================================================================

TABLE_NAME = "warranty_claims"  # Available: cost_ledger, employee_master,
                                # engine_assembly_log, logistics_shipments,
                                # paint_shop_log, parts_inventory,
                                # production_orders, quality_inspections,
                                # sales_orders, supplier_master,
                                # vehicle_master, warranty_claims

SUB_PATH = "xxl"                 # Sub-path under output/{TABLE_NAME}/ (e.g. "xl", "xxl")
target_mode="snowflake"
VERIFY_SCHEMA = True

COMPARE_COLS = None             # None = auto-detect all non-PK columns

EXCLUDE_COLS = ["load_ts", "batch_id"]

# ┌─────────────────────────────────────────────────────────────────────────┐
# │ SOURCE DATA PATHS (local)                                               │
# │ Source files from output/{TABLE_NAME}/{SUB_PATH}/                        │
# │   - source_enriched.csv          : enriched source data (with joins)     │
# │   - source_enriched.schema.json  : schema of enriched CSV               │
# │   - source_db_schema.json        : original DB schema for validation     │
# └─────────────────────────────────────────────────────────────────────────┘

SOURCE_CSV_PATH       = f"output/{TABLE_NAME}/{SUB_PATH}/source_raw.csv"
SOURCE_SCHEMA_JSON    = f"output/{TABLE_NAME}/{SUB_PATH}/source_raw.schema.json"
SOURCE_DB_SCHEMA_JSON = f"output/{TABLE_NAME}/{SUB_PATH}/source_db_schema.json"

# ============================================================================
# SECTION 2 — PK FILTER
# ============================================================================

PK_FILTER_MODE: Literal["full", "pk_range", "pk_set"] = "full"

PK_RANGE: dict = {"lower": 90000000, "upper": 90000004}
PK_SET: Set = set()


# ============================================================================
# SECTION 3 — DATE WATERMARK FILTER
# ============================================================================

DATE_WATERMARK_MODE: Literal["full", "range"] = "full"

DATE_FROM: Optional[str] = None
DATE_FROM_COL: Optional[str] = None
DATE_TO: Optional[str] = None
DATE_TO_COL: Optional[str] = None


# ============================================================================
# AUTO-CONFIGURATION LOADER
# ============================================================================

_config: dict = {}

if TABLE_NAME:
    logger.info("Using AUTO-CONFIGURATION for table: %s", TABLE_NAME)
    try:
        _config = get_table_config(TABLE_NAME, target_mode=target_mode)

        TARGET_QUERY_FILE = _config["target_query_file"]
        TRANSFORM_FILE    = _config["transform_file"]
        PRIMARY_KEYS      = _config["primary_keys"]
        SOURCE_DDL        = _config["source_ddl"]
        TARGET_DDL        = _config["target_ddl"]
        OUTPUT_DIR        = os.path.join("output", TABLE_NAME, SUB_PATH)

        logger.info("Auto-configuration loaded:")
        logger.info("  Source table : %s", _config.get("source_table"))
        logger.info("  Target table : %s", _config.get("target_table"))
        logger.info("  Primary Keys : %s", PRIMARY_KEYS)
        logger.info("  Transform    : %s", os.path.basename(TRANSFORM_FILE))

    except Exception as e:
        logger.error("Auto-configuration failed: %s", e)
        logger.info("Available tables: %s", ", ".join(list_available_tables()))
        sys.exit(1)

# Only diff report CSV
REPORT_CSV = os.path.join(OUTPUT_DIR, "diff_report.csv")


# ============================================================================
# FILTER BUILDER
# ============================================================================

try:
    SOURCE_FILTER, TARGET_FILTER = build_load_filters(
        config=_config,
        pk_filter_mode=PK_FILTER_MODE,
        pk_range=PK_RANGE,
        pk_set=PK_SET,
        date_mode=DATE_WATERMARK_MODE,
        date_from=DATE_FROM,
        date_from_col=DATE_FROM_COL,
        date_to=DATE_TO,
        date_to_col=DATE_TO_COL,
    )
except (ValueError, FileNotFoundError) as exc:
    logger.error("Filter configuration error: %s", exc)
    sys.exit(1)

logger.info("Source filter: %s", SOURCE_FILTER["description"])
logger.info("Target filter: %s", TARGET_FILTER["description"])


# ============================================================================
# PIPELINE CONTEXT
# ============================================================================

pipeline_ctx = dict(
    config=_config,
    target_mode=target_mode,
    verify_schema=VERIFY_SCHEMA,
    source_query=None,
    source_query_file=None,  # Not used — source comes from CSV
    target_query=None,
    target_query_file=TARGET_QUERY_FILE,
    transform_file=TRANSFORM_FILE,
    primary_keys=PRIMARY_KEYS,
    exclude_cols=EXCLUDE_COLS,
    compare_cols=COMPARE_COLS,
    source_ddl=SOURCE_DDL,
    target_ddl=TARGET_DDL,
    report_csv=REPORT_CSV,
    source_filter=SOURCE_FILTER,
    target_filter=TARGET_FILTER,
    # Databricks-specific: source data paths
    source_csv_path=SOURCE_CSV_PATH,
    source_schema_json=SOURCE_SCHEMA_JSON,                  # DDL-based schema for validation
     source_db_schema_json=SOURCE_DB_SCHEMA_JSON
)


# ============================================================================
# MAIN
# ============================================================================

def main() -> int:
    """
    Run ETL validation pipeline on Databricks.

    Flow: step 0 → 1 → 2 → 3.5 → 4 → 5
      0   : Verify source schema (.schema.json vs DDL)
      1   : Load raw source CSV from storage
      2   : Transform (cache result, unpersist raw)
      3.5 : Verify target schema (Snowflake live)
      4   : Extract target from Snowflake
      5   : Compare & generate diff_report.csv
    """
    start_time = time.time()

    try:
        logger.info("=" * 60)
        logger.info("ETL Validation Pipeline (Local)")
        logger.info("  Table  : %s", TABLE_NAME)
        logger.info("  Target : snowflake")
        logger.info("  Source : storage CSV")
        logger.info("  Source filter : %s", SOURCE_FILTER["description"])
        logger.info("  Target filter : %s", TARGET_FILTER["description"])
        logger.info("=" * 60)

        os.makedirs(OUTPUT_DIR, exist_ok=True)

        spark = get_spark_session(app_name="ETL_Databricks")
        try:
            spark.conf.set("spark.sql.debug.maxToStringFields", 500)
        except Exception:
            pass
        try:
            spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        except Exception:
            pass
        try:
            spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
        except Exception:
            pass

        # Step 0: optional source schema check
        if not step_0_verify_source_schema(spark, pipeline_ctx):
            logger.error("Exiting due to source schema verification failure")
            return 2

        # Step 1: load source from storage CSV
        source_df = step_1_extract_source(spark, pipeline_ctx)

        # Step 2: transform
        transformed_df = step_2_transform(source_df, pipeline_ctx, target_mode=target_mode)
        logger.info("Using enriched source CSV — transform step skipped")

        # Step 3.5: optional target schema check
        if not step_3_5_verify_target_schema(spark, pipeline_ctx):
            logger.error("Exiting due to target schema verification failure")
            return 2

        # Step 4: extract target from Snowflake
        # Data is cached inside get_data_from_snowflake() to avoid session-null bug
        target_df = step_4_extract_target(spark, pipeline_ctx)

        # Step 5: compare & generate diff report
        t0 = time.time()
        exit_code = step_5_compare(spark, transformed_df, target_df, pipeline_ctx)
        logger.info("Compare and Report time: %.2fs", time.time() - t0)

        # [SERVERLESS] Cache cleanup disabled — uncomment for dedicated cluster
        # logger.info("Ensuring cached DataFrames are released...")
        # if transformed_df.is_cached:
        #     transformed_df.unpersist()
        # if target_df.is_cached:
        #     target_df.unpersist()
        # logger.info("Cache cleanup complete.")

        elapsed = timedelta(seconds=int(time.time() - start_time))
        minutes = elapsed.seconds // 60
        seconds = elapsed.seconds % 60
        logger.info("=" * 60)
        logger.info("Pipeline complete. Exit code: %d", exit_code)
        logger.info("Total time: %d min %d sec", minutes, seconds)
        logger.info("=" * 60)

        return exit_code

    except (FileNotFoundError, ValueError) as exc:
        logger.error("Configuration error: %s", exc)
        return 1
    except KeyboardInterrupt:
        logger.info("Interrupted by user.")
        return 1
    except Exception as exc:
        logger.exception("Unexpected error: %s", exc)
        return 99


if __name__ == "__main__":
    sys.exit(main())

