"""
connections/spark_session.py
-----------------------------
Databricks version — returns the pre-configured SparkSession.
No JAVA_HOME, HADOOP_HOME, JAR discovery, or local[*] needed.
"""

import logging

logger = logging.getLogger(__name__)


def get_spark_session(app_name: str = "ETLValidator"):
    """
    Return SparkSession — on Databricks, returns the active session.
    Falls back to creating a new session for non-notebook contexts (e.g. unit tests).
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    if spark is None:
        spark = SparkSession.builder.appName(app_name).getOrCreate()

    logger.info("SparkSession ready (app=%s)", app_name)
    return spark

