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

# ── Your Databricks username (for Repos path) ─────────────────
# Find it: top-right corner of Databricks UI → your email
# Replace <your-username> below with your actual username/email

DATABRICKS_USERNAME = "arshpreet0907singh@gmail.com"  # e.g. "arshpreet.singh@example.com"
REPO_NAME = "ETL_Testing_Databricks"

# ── Table to validate ─────────────────────────────────────────
TABLE_NAME = "warranty_claims"

# ── Source file paths (Volumes) ───────────────────────────────
SOURCE_CSV_PATH    = f"/Volumes/etl_testing/raw_data/source_files/{TABLE_NAME}/source_raw.csv"
SOURCE_SCHEMA_JSON = f"/Volumes/etl_testing/raw_data/source_files/{TABLE_NAME}/source_raw.schema.json"
SOURCE_DB_SCHEMA_JSON=f"/Volumes/etl_testing/raw_data/source_files/{TABLE_NAME}/source_db_schema.json"
# ── Output path (writable — NOT inside Repos) ────────────────
OUTPUT_BASE = f"/Volumes/etl_testing/raw_data/source_files/output"

# ── Schema verification ──────────────────────────────────────
VERIFY_SCHEMA = True

# ── Columns to exclude from comparison ────────────────────────
EXCLUDE_COLS = ["load_ts", "batch_id"]

# ── Filters (full load by default) ───────────────────────────
PK_FILTER_MODE = "full"
DATE_WATERMARK_MODE = "full"

print(f"Table        : {TABLE_NAME}")
print(f"Source CSV   : {SOURCE_CSV_PATH}")
print(f"Schema JSON  : {SOURCE_SCHEMA_JSON}")
print(f"Db Schema JSON  : {SOURCE_DB_SCHEMA_JSON}")
print(f"Output base  : {OUTPUT_BASE}")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 2: ADD PROJECT TO PYTHON PATH
# ═══════════════════════════════════════════════════════════════

import sys, os

REPO_PATH = f"/Workspace/Users/{DATABRICKS_USERNAME}/{REPO_NAME}"

# Verify repo exists
if not os.path.isdir(REPO_PATH):
    print(f"ERROR: Repo not found at {REPO_PATH}")
    print(f"  → Check DATABRICKS_USERNAME and REPO_NAME in Cell 1")
    print(f"  → Make sure you cloned the repo via Repos → Add Repo")
    raise FileNotFoundError(f"Repo not found: {REPO_PATH}")

# Add to Python path so we can import utils/
if REPO_PATH not in sys.path:
    sys.path.insert(0, REPO_PATH)

print(f"✅ Repo found: {REPO_PATH}")
print(f"   Contents: {os.listdir(REPO_PATH)}")

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

print("✅ Snowflake config set (password from widget)")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 4: BUILD PIPELINE CONTEXT
# ═══════════════════════════════════════════════════════════════

import time
from utils.auto_config import get_table_config
from utils.logger import get_logger

logger = get_logger("databricks_runner")

# Auto-detect table config from repo files
config = get_table_config(TABLE_NAME, target_mode="snowflake")

# Override output dir to writable location
OUTPUT_DIR = os.path.join(OUTPUT_BASE, TABLE_NAME)
os.makedirs(OUTPUT_DIR, exist_ok=True)
REPORT_CSV = os.path.join(OUTPUT_DIR, "diff_report.csv")

print(f"✅ Auto-config loaded for: {TABLE_NAME}")
print(f"   Source table : {config.get('source_table')}")
print(f"   Target table : {config.get('target_table')}")
print(f"   Primary Keys : {config['primary_keys']}")
print(f"   Transform    : {os.path.basename(config['transform_file'])}")
print(f"   Output dir   : {OUTPUT_DIR}")
print(f"   Report CSV   : {REPORT_CSV}")

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
    source_filter={"where_clause": "", "description": "full load (no filters)"},
    target_filter={"where_clause": "", "description": "full load (no filters)"},
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

passed = step_0_verify_source_schema(spark, pipeline_ctx)
if passed:
    print("✅ Source schema verification PASSED")
else:
    print("❌ Source schema verification FAILED — check logs above")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 6: STEP 1 — LOAD SOURCE CSV FROM VOLUMES
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_1_extract_source

source_df = step_1_extract_source(spark, pipeline_ctx)
print(f"✅ Source loaded: {source_df.count()} rows, {len(source_df.columns)} columns")
display(source_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 7: STEP 2 — TRANSFORM (on cluster, in-memory)
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_2_transform

t0 = time.time()
transformed_df = step_2_transform(source_df, pipeline_ctx, "snowflake")
print(f"✅ Transformed: {transformed_df.count()} rows in {time.time()-t0:.1f}s")
print(f"   Columns: {transformed_df.columns}")
display(transformed_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 8: STEP 3.5 — VERIFY TARGET SCHEMA (Snowflake live)
# ═══════════════════════════════════════════════════════════════
# Requires Snowflake connector library installed on cluster:
#   Maven: net.snowflake:spark-snowflake_2.12:2.16.0-spark_3.5
#   Maven: net.snowflake:snowflake-jdbc:3.18.0

from utils.custom_execution_utils import step_3_5_verify_target_schema

passed = step_3_5_verify_target_schema(spark, pipeline_ctx)
if passed:
    print("✅ Target schema verification PASSED")
else:
    print("❌ Target schema verification FAILED — check logs above")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 9: STEP 4 — EXTRACT TARGET FROM SNOWFLAKE
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_4_extract_target

target_df = step_4_extract_target(spark, pipeline_ctx)
print(f"✅ Target loaded: {target_df.count()} rows, {len(target_df.columns)} columns")
display(target_df.limit(5))

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 10: STEP 5 — COMPARE & GENERATE DIFF REPORT
# ═══════════════════════════════════════════════════════════════

from utils.custom_execution_utils import step_5_compare

t0 = time.time()
exit_code = step_5_compare(spark, transformed_df, target_df, pipeline_ctx)
elapsed = time.time() - t0

if exit_code == 0:
    print(f"\n🟢 PASS — No differences found! ({elapsed:.1f}s)")
else:
    print(f"\n🔴 FAIL — Differences found. ({elapsed:.1f}s)")
    print(f"   Report: {REPORT_CSV}")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 11: VIEW RESULTS
# ═══════════════════════════════════════════════════════════════

# Show diff report if it exists
if os.path.isfile(REPORT_CSV):
    report_df = spark.read.option("header", True).csv(REPORT_CSV)
    print(f"Diff report: {report_df.count()} rows")
    display(report_df)
else:
    print("No diff report file — either PASS or report path issue")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# CELL 12: CLEANUP
# ═══════════════════════════════════════════════════════════════

# [SERVERLESS] Cache cleanup disabled — uncomment for dedicated cluster
# if transformed_df.is_cached:
#     transformed_df.unpersist()
# if target_df.is_cached:
#     target_df.unpersist()
print("✅ Cleanup complete (caching disabled on serverless)")

