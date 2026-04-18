"""
connections/target_connection.py
---------------------------------
Databricks version — Snowflake only.

Supports two auth modes:
  1. Databricks Secrets (production / Azure Databricks)
  2. Direct config dict (Community Edition — set via set_snowflake_config())

Provides two connection formats:
  1. Spark-Snowflake connector options (for data reads via net.snowflake.spark.snowflake)
  2. JDBC options (for schema verification via INFORMATION_SCHEMA)
"""

import logging

logger = logging.getLogger(__name__)

# ── Direct config override (Community Edition) ─────────────────────────────
# Call set_snowflake_config({...}) before pipeline steps to bypass Secrets API.
_SF_DIRECT_CONFIG: dict = {}


def set_snowflake_config(config: dict) -> None:
    """
    Set Snowflake credentials directly (for Community Edition without Secrets API).

    Parameters
    ----------
    config : dict
        Must contain keys: sfAccount, sfUser, sfPassword, sfDatabase,
        sfWarehouse, sfRole.  sfSchema defaults to "PUBLIC" if absent.

    Example
    -------
        set_snowflake_config({
            "sfAccount":   "RPDEFQT-SJ73076",
            "sfUser":      "ARSHPREETSINGH98",
            "sfPassword":  dbutils.widgets.get("sf_password"),
            "sfDatabase":  "ETL_OUTPUT_SNOWFLAKE_TARGET_JOINS",
            "sfWarehouse": "ETL_WH",
            "sfRole":      "ACCOUNTADMIN",
        })
    """
    global _SF_DIRECT_CONFIG
    required = {"sfAccount", "sfUser", "sfPassword", "sfDatabase", "sfWarehouse", "sfRole"}
    missing = required - set(config.keys())
    if missing:
        raise ValueError(f"Missing required Snowflake config keys: {missing}")
    _SF_DIRECT_CONFIG = dict(config)
    logger.info("Snowflake direct config set (account=%s, db=%s)",
                config["sfAccount"], config["sfDatabase"])


def _get_dbutils():
    """Get dbutils reference on Databricks."""
    from pyspark.sql import SparkSession
    spark = SparkSession.getActiveSession()
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        # Fallback for some Databricks runtimes
        import IPython
        return IPython.get_ipython().user_ns.get("dbutils")


def get_target_connection(mode: str = "snowflake") -> dict:
    """
    Return Snowflake connection options.

    Uses direct config if set via set_snowflake_config(),
    otherwise falls back to Databricks Secrets.

    Parameters
    ----------
    mode : str
        Only "snowflake" is supported on Databricks.

    Returns
    -------
    dict
        Spark-Snowflake connector options dict.
    """
    if mode != "snowflake":
        raise ValueError(
            f"Only 'snowflake' target mode is supported on Databricks, got: {mode!r}"
        )

    return _build_snowflake_connector_opts()


def get_snowflake_jdbc_opts() -> dict:
    """
    Return JDBC options for Snowflake schema verification.
    Used by verify_schema.py to query INFORMATION_SCHEMA.COLUMNS.
    """
    if _SF_DIRECT_CONFIG:
        account = _SF_DIRECT_CONFIG["sfAccount"]
        database = _SF_DIRECT_CONFIG["sfDatabase"]
        schema = _SF_DIRECT_CONFIG.get("sfSchema", "PUBLIC")
        jdbc_url = f"jdbc:snowflake://{account}.snowflakecomputing.com/?db={database}&schema={schema}"
        logger.info("Snowflake JDBC URL (direct config): %s", jdbc_url)
        return {
            "url": jdbc_url,
            "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
            "user": _SF_DIRECT_CONFIG["sfUser"],
            "password": _SF_DIRECT_CONFIG["sfPassword"],
            "sfWarehouse": _SF_DIRECT_CONFIG["sfWarehouse"],
            "sfDatabase": database,
            "sfSchema": schema,
            "sfRole": _SF_DIRECT_CONFIG["sfRole"],
        }

    dbutils = _get_dbutils()
    scope = "etl-secrets"

    account = dbutils.secrets.get(scope, "sf-account")
    database = dbutils.secrets.get(scope, "sf-database")
    schema = "PUBLIC"

    jdbc_url = f"jdbc:snowflake://{account}.snowflakecomputing.com/?db={database}&schema={schema}"

    logger.info("Snowflake JDBC URL: %s", jdbc_url)

    return {
        "url": jdbc_url,
        "driver": "net.snowflake.client.jdbc.SnowflakeDriver",
        "user": dbutils.secrets.get(scope, "sf-user"),
        "password": dbutils.secrets.get(scope, "sf-password"),
        "sfWarehouse": dbutils.secrets.get(scope, "sf-warehouse"),
        "sfDatabase": database,
        "sfSchema": schema,
        "sfRole": dbutils.secrets.get(scope, "sf-role"),
    }


def _build_snowflake_connector_opts() -> dict:
    """Build Spark-Snowflake connector options — direct config or Secrets."""
    if _SF_DIRECT_CONFIG:
        account = _SF_DIRECT_CONFIG["sfAccount"]
        database = _SF_DIRECT_CONFIG["sfDatabase"]
        sf_url = f"{account}.snowflakecomputing.com"
        opts = {
            "sfURL": sf_url,
            "url": f"https://{sf_url}", # [SERVERLESS]
            "sfUser": _SF_DIRECT_CONFIG["sfUser"],
            "sfPassword": _SF_DIRECT_CONFIG["sfPassword"],
            "sfDatabase": database,
            "sfSchema": _SF_DIRECT_CONFIG.get("sfSchema", "PUBLIC"),
            "sfWarehouse": _SF_DIRECT_CONFIG["sfWarehouse"],
            "sfRole": _SF_DIRECT_CONFIG["sfRole"],
            "sfTimezone": "UTC",
            "preActions": "ALTER SESSION SET TIMEZONE = 'UTC'",
        }
        logger.info("Snowflake connector opts built from direct config (account=%s, db=%s)",
                     account, database)
        return opts

    dbutils = _get_dbutils()
    scope = "etl-secrets"

    account = dbutils.secrets.get(scope, "sf-account")
    database = dbutils.secrets.get(scope, "sf-database")

    opts = {
        "sfURL": f"{account}.snowflakecomputing.com",
        "url": f"https://{account}.snowflakecomputing.com",  # [SERVERLESS]
        "sfUser": dbutils.secrets.get(scope, "sf-user"),
        "sfPassword": dbutils.secrets.get(scope, "sf-password"),
        "sfDatabase": database,
        "sfSchema": "PUBLIC",
        "sfWarehouse": dbutils.secrets.get(scope, "sf-warehouse"),
        "sfRole": dbutils.secrets.get(scope, "sf-role"),
        "sfTimezone": "UTC",
        "preActions": "ALTER SESSION SET TIMEZONE = 'UTC'",
    }

    logger.info(
        "Snowflake connector opts built (account=%s, db=%s)",
        account, database,
    )
    return opts

