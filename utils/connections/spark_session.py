"""
connections/spark_session.py
-----------------------------
Unified SparkSession provider.

Set SPARK_MODE = "databricks" when running on Databricks (uses pre-configured session).
Set SPARK_MODE = "local" when running locally (creates local[*] session with JARs, env setup).
"""

import glob
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

# ── Switch this flag ──────────────────────────────────────────────────────────
# "databricks" → returns the active Databricks SparkSession
# "local"      → creates a local[*] SparkSession with JDBC JARs, env config
SPARK_MODE = "local"
# ──────────────────────────────────────────────────────────────────────────────

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_spark_session(app_name: str = "ETLValidator"):
    if SPARK_MODE == "databricks":
        return _get_databricks_session(app_name)
    else:
        return _get_local_session(app_name)


# ---------------------------------------------------------------------------
# Databricks mode
# ---------------------------------------------------------------------------
def _get_databricks_session(app_name: str):
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
    logger.info("SparkSession ready — Databricks (app=%s)", app_name)
    return spark


# ---------------------------------------------------------------------------
# Local mode
# ---------------------------------------------------------------------------
def _get_local_session(app_name: str):
    _set_env_from_config()
    _set_python_executable()
    jars_csv = _discover_jars()

    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.kryoserializer.buffer.max", "512m")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.python.worker.reuse", "true")
        .config("spark.python.worker.timeout", "120")
    )

    if jars_csv:
        builder = builder.config("spark.jars", jars_csv)
        logger.info("Spark JARs loaded: %s", jars_csv)
    else:
        logger.warning(
            "No JARs found in %s — JDBC reads will fail unless JARs are on the classpath.",
            os.path.join(_PROJECT_ROOT, "jars"),
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession started (app=%s, master=local[*])", app_name)
    return spark


# ---------------------------------------------------------------------------
# Private helpers (local mode only)
# ---------------------------------------------------------------------------

def _set_env_from_config() -> None:
    try:
        from utils.config_loader import load_config
        config_path = os.path.join(_PROJECT_ROOT, "config", "pipeline_config.yaml")
        cfg = load_config(config_path)
    except FileNotFoundError:
        logger.debug("pipeline_config.yaml not found — skipping env injection.")
        return
    except Exception as exc:
        logger.warning("Could not load pipeline_config.yaml: %s", exc)
        return

    java_home: Optional[str] = cfg.get("java_home")
    hadoop_home: Optional[str] = cfg.get("hadoop_home")

    if java_home and not os.environ.get("JAVA_HOME"):
        os.environ["JAVA_HOME"] = java_home
        logger.debug("JAVA_HOME set from config: %s", java_home)

    if hadoop_home and not os.environ.get("HADOOP_HOME"):
        os.environ["HADOOP_HOME"] = hadoop_home
        logger.debug("HADOOP_HOME set from config: %s", hadoop_home)


def _set_python_executable() -> None:
    import sys
    python_exe = sys.executable
    if not os.environ.get("PYSPARK_PYTHON"):
        os.environ["PYSPARK_PYTHON"] = python_exe
        logger.debug("PYSPARK_PYTHON set to: %s", python_exe)
    if not os.environ.get("PYSPARK_DRIVER_PYTHON"):
        os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe
        logger.debug("PYSPARK_DRIVER_PYTHON set to: %s", python_exe)


def _discover_jars() -> str:
    jars_dir = os.path.join(_PROJECT_ROOT, "jars")
    if not os.path.isdir(jars_dir):
        return ""

    jar_paths = []
    for p in glob.glob(os.path.join(jars_dir, "*.jar")):
        abs_path = os.path.abspath(p)
        if os.name == 'nt':
            file_uri = "file:///" + abs_path.replace("\\", "/")
        else:
            file_uri = "file://" + abs_path
        jar_paths.append(file_uri)

    return ",".join(jar_paths)

