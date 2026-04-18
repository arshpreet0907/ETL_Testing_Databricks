# Databricks notebook source
# MAGIC %md
# MAGIC # ETL Validation Pipeline — Databricks Runner
# MAGIC
# MAGIC **How to use:**
# MAGIC 1. Clone this repo via **Repos → Add Repo** (or upload files to Workspace)
# MAGIC 2. Create this notebook in **Workspace** (not inside Repos — Repos are read-only)
# MAGIC 3. Run cells in order
# MAGIC
# MAGIC **Snowflake:** Uses widget for password (Community Edition has no Secrets API)

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 1: CONFIGURATION
# ═══════════════════════════════════════════════════════════════

import logging, time as _time

logging.basicConfig(
    format="[%(asctime)s] %(levelname)-7s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
_log = logging.getLogger("databricks_runner")

# ── Your Databricks username (for Repos path) ─────────────────
# Find it: top-right corner of Databricks UI → your email
# Replace <your-username> below with your actual username/email

DATABRICKS_USERNAME = "arshpreet0907singh@gmail.com"  # e.g. "arshpreet.singh@example.com"
REPO_NAME = "ETL_Testing_Databricks"

# ── Table to validate ─────────────────────────────────────────
TABLE_NAME = "warranty_claims"

# ── Source file paths (Volumes) ───────────────────────────────
BASE_SOURCE_PATH=f"/Volumes/etl_testing/raw_data/source_files/{TABLE_NAME}/"
BASE_SOURCE_PATH_SUB="xl"

SOURCE_CSV_PATH    = BASE_SOURCE_PATH+BASE_SOURCE_PATH_SUB+"/source_raw.csv"
SOURCE_SCHEMA_JSON = BASE_SOURCE_PATH+BASE_SOURCE_PATH_SUB+"/source_raw.schema.json"
SOURCE_DB_SCHEMA_JSON=BASE_SOURCE_PATH+BASE_SOURCE_PATH_SUB+"/source_db_schema.json"
# ── Output path (writable — NOT inside Repos) ────────────────
OUTPUT_BASE = f"/Volumes/etl_testing/raw_data/source_files/output"

# ── Schema verification ──────────────────────────────────────
VERIFY_SCHEMA = True

# ── Columns to exclude from comparison ────────────────────────
EXCLUDE_COLS = ["load_ts", "batch_id"]

# ── Filters (full load by default) ───────────────────────────
# PK_FILTER_MODE: "full" | "pk_range" | "pk_set"
PK_FILTER_MODE = "full"
PK_RANGE = {"lower": None, "upper": None}   # Used when PK_FILTER_MODE = "pk_range"
PK_SET = set()                                # Used when PK_FILTER_MODE = "pk_set"

# DATE_WATERMARK_MODE: "full" | "range"
DATE_WATERMARK_MODE = "full"
DATE_FROM = None          # e.g. "2025-01-01"
DATE_FROM_COL = None      # e.g. "created_at"
DATE_TO = None            # e.g. "2025-12-31"
DATE_TO_COL = None        # e.g. "created_at"

_log.info(f"Table        : {TABLE_NAME}")
_log.info(f"Source CSV   : {SOURCE_CSV_PATH}")
_log.info(f"Schema JSON  : {SOURCE_SCHEMA_JSON}")
_log.info(f"Db Schema JSON  : {SOURCE_DB_SCHEMA_JSON}")
_log.info(f"Output base  : {OUTPUT_BASE}")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 2: ADD PROJECT TO PYTHON PATH
# ═══════════════════════════════════════════════════════════════

import sys, os

REPO_PATH = f"/Workspace/Users/{DATABRICKS_USERNAME}/{REPO_NAME}"

# Verify repo exists
if not os.path.isdir(REPO_PATH):
    _log.error(f"Repo not found at {REPO_PATH}")
    _log.error(f"  → Check DATABRICKS_USERNAME and REPO_NAME in Cell 1")
    _log.error(f"  → Make sure you cloned the repo via Repos → Add Repo")
    raise FileNotFoundError(f"Repo not found: {REPO_PATH}")

# Add to Python path so we can import utils/
if REPO_PATH not in sys.path:
    sys.path.insert(0, REPO_PATH)

_log.info(f"✅ Repo found: {REPO_PATH}")
_log.info(f"   Contents: {os.listdir(REPO_PATH)}")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 3: SNOWFLAKE CREDENTIALS (Community Edition)
# ═══════════════════════════════════════════════════════════════
# On Azure Databricks with Secrets, skip this cell entirely —
# target_connection.py will use dbutils.secrets automatically.

dbutils.widgets.text("sf_password", "", "Snowflake Password")

from utils.connections.target_connection import set_snowflake_config

set_snowflake_config({
    "sfAccount":   "RPDEFQT-SJ73076",
    "sfUser":      "ARSHPREETSINGH98",
    "sfPassword":  dbutils.widgets.get("sf_password"),
    "sfDatabase":  "ETL_OUTPUT_SNOWFLAKE_TARGET_JOINS",
    "sfSchema":    "PUBLIC",
    "sfWarehouse": "ETL_WH",
    "sfRole":      "ACCOUNTADMIN",
})

_log.info("✅ Snowflake config set (password from widget)")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 4: BUILD PIPELINE CONTEXT
# ═══════════════════════════════════════════════════════════════

import time
from utils.auto_config import get_table_config
from utils.logger import get_logger
from utils.custom_execution_utils import build_load_filters

logger = get_logger("databricks_runner")

# Auto-detect table config from repo files
config = get_table_config(TABLE_NAME, target_mode="snowflake")

# Override output dir to writable location

OUTPUT_DIR = os.path.join(OUTPUT_BASE, TABLE_NAME,BASE_SOURCE_PATH_SUB)
os.makedirs(OUTPUT_DIR, exist_ok=True)
REPORT_CSV = os.path.join(OUTPUT_DIR, "diff_report.csv")

_log.info(f"✅ Auto-config loaded for: {TABLE_NAME}")
_log.info(f"   Source table : {config.get('source_table')}")
_log.info(f"   Target table : {config.get('target_table')}")
_log.info(f"   Primary Keys : {config['primary_keys']}")
_log.info(f"   Transform    : {os.path.basename(config['transform_file'])}")
_log.info(f"   Output dir   : {OUTPUT_DIR}")
_log.info(f"   Report CSV   : {REPORT_CSV}")

# Build filters from configuration
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

_log.info(f"   Source filter: {SOURCE_FILTER['description']}")
_log.info(f"   Target filter: {TARGET_FILTER['description']}")

# Build pipeline context
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
    source_db_schema_json=SOURCE_DB_SCHEMA_JSON
)

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 5: STEP 0 — VERIFY SOURCE SCHEMA (.schema.json vs DDL)
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_0_verify_source_schema

try:
    spark.conf.set("spark.sql.debug.maxToStringFields", 500)
except Exception:
    pass  # Not available in newer Spark/DBR versions
try:
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
except Exception:
    pass
try:
    spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
except Exception:
    pass

_t0 = _time.time()
passed = step_0_verify_source_schema(spark, pipeline_ctx)
if passed:
    _log.info(f"✅ Source schema verification PASSED ({_time.time()-_t0:.1f}s)")
else:
    _log.warning(f"❌ Source schema verification FAILED ({_time.time()-_t0:.1f}s) — check logs above")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 6: STEP 1 — LOAD SOURCE CSV FROM VOLUMES
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_1_extract_source

_t0 = _time.time()
source_df = step_1_extract_source(spark, pipeline_ctx)
_log.info(f"✅ Source loaded: {source_df.count()} rows, {len(source_df.columns)} columns ({_time.time()-_t0:.1f}s)")
display(source_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 7: STEP 2 — TRANSFORM (on cluster, in-memory)
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_2_transform

_t0 = _time.time()
transformed_df = step_2_transform(source_df, pipeline_ctx, "snowflake")
_log.info(f"✅ Transformed: {transformed_df.count()} rows, {len(transformed_df.columns)} columns ({_time.time()-_t0:.1f}s)")
_log.info(f"   Columns: {transformed_df.columns}")
display(transformed_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 8: STEP 3.5 — VERIFY TARGET SCHEMA (Snowflake live)
# ═══════════════════════════════════════════════════════════════
# Requires Snowflake connector library installed on cluster:
#   Maven: net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5
#   Maven: net.snowflake:snowflake-jdbc:3.18.0

from utils.custom_execution_utils import step_3_5_verify_target_schema

_t0 = _time.time()
passed = step_3_5_verify_target_schema(spark, pipeline_ctx)
if passed:
    _log.info(f"✅ Target schema verification PASSED ({_time.time()-_t0:.1f}s)")
else:
    _log.warning(f"❌ Target schema verification FAILED ({_time.time()-_t0:.1f}s) — check logs above")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 9: STEP 4 — EXTRACT TARGET FROM SNOWFLAKE
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_4_extract_target

_t0 = _time.time()
target_df = step_4_extract_target(spark, pipeline_ctx)
_log.info(f"✅ Target loaded: {target_df.count()} rows, {len(target_df.columns)} columns ({_time.time()-_t0:.1f}s)")
display(target_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 10: STEP 5 — COMPARE & GENERATE DIFF REPORT
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_5_compare

_t0 = _time.time()
exit_code = step_5_compare(spark, transformed_df, target_df, pipeline_ctx)
elapsed = _time.time() - _t0

if exit_code == 0:
    _log.info(f"🟢 PASS — No differences found! ({elapsed:.1f}s)")
else:
    _log.warning(f"🔴 FAIL — Differences found. ({elapsed:.1f}s)")
    _log.warning(f"   Report: {REPORT_CSV}")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 11: VIEW RESULTS
# ═══════════════════════════════════════════════════════════════

if os.path.isfile(REPORT_CSV):
    report_df = spark.read.option("header", True).csv(REPORT_CSV)
    _log.info(f"Diff report: {report_df.count()} rows")
    display(report_df)
else:
    _log.info("No diff report file — either PASS or report path issue")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 12: CLEANUP
# ═══════════════════════════════════════════════════════════════

# [SERVERLESS] Cache cleanup disabled — uncomment for dedicated cluster
# if transformed_df.is_cached:
#     transformed_df.unpersist()
# if target_df.is_cached:
#     target_df.unpersist()
_log.info("✅ Cleanup complete (caching disabled on serverless)")
