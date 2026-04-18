"""
04_transform.py  —  production_orders  ->  fact_production
Generated : 2026-04-19 00:43

Public API
----------
    from 04_transform import apply_transforms
    df_out = apply_transforms(df_source, dialect='mysql')
    df_out = apply_transforms(df_source, dialect='snowflake')

Parameters
----------
    df_source : pyspark.sql.DataFrame
        Raw source extract DataFrame. Column names must match
        the src_col_name values in the mapping spec.
        The input DataFrame is always assumed to be MySQL dialect
        (e.g. TINYINT(1) as 0/1, CHAR columns space-padded,
        zero-dates as '0000-00-00', TIME as timedelta, etc.).
        Must also include join-fetched columns (aliased as
        <join_alias>_<column>) from the extract SQL.

    dialect : Literal['mysql', 'snowflake']
        Target dialect for the returned DataFrame.
        - 'mysql'     : values left in MySQL-native form (default).
        - 'snowflake' : applies dialect coercions so the frame is
                        ready for write_pandas / Snowflake ingestion.

Returns
-------
    pyspark.sql.DataFrame
        Transformed frame shaped to fact_production target schema,
        with values coerced to the requested dialect.
"""
from __future__ import annotations
import logging
from typing import Literal
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import (
    BooleanType, DecimalType, DoubleType,
    IntegerType, LongType, StringType, TimestampType,
)

logger = logging.getLogger(__name__)


def apply_transforms(
    df: DataFrame,
    dialect: Literal['mysql', 'snowflake'] = 'mysql',
) -> DataFrame:
    """
    Apply all column-level transforms for production_orders.

    The input DataFrame must contain every source field referenced in
    the mapping spec. Extra columns in the input are silently ignored.
    Input values are always assumed to be MySQL-dialect.
    Pass dialect='snowflake' to coerce values for Snowflake ingestion.

    Join columns expected in input DataFrame:
        [veh] vehicle_master: vin_number, model_nm, variant_cd
    """
    if dialect not in ('mysql', 'snowflake'):
        raise ValueError(f"dialect must be 'mysql' or 'snowflake', got {dialect!r}")
    logger.info("=" * 70)
    logger.info('START TRANSFORM | production_orders -> fact_production | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: prod_order_id -> production_order_key
    logger.debug('  [direct   ] %s -> production_order_key', 'prod_order_id')
    df = df.withColumn('production_order_key', F.col('prod_order_id'))

    # RENAME: vehicle_id -> vehicle_key  # FK to dim_vehicle
    logger.debug('  [rename   ] %s -> vehicle_key', 'vehicle_id')
    df = df.withColumn('vehicle_key', F.col('vehicle_id'))

    # RENAME: plant_cd -> plant_code
    logger.debug('  [rename   ] %s -> plant_code', 'plant_cd')
    df = df.withColumn('plant_code', F.col('plant_cd'))

    # RENAME: order_dt -> order_date
    logger.debug('  [rename   ] %s -> order_date', 'order_dt')
    df = df.withColumn('order_date', F.col('order_dt'))

    # RENAME: planned_start_dt -> planned_start_date
    logger.debug('  [rename   ] %s -> planned_start_date', 'planned_start_dt')
    df = df.withColumn('planned_start_date', F.col('planned_start_dt'))

    # RENAME: actual_start_dt -> actual_start_date
    logger.debug('  [rename   ] %s -> actual_start_date', 'actual_start_dt')
    df = df.withColumn('actual_start_date', F.col('actual_start_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: planned_end_dt -> planned_end_date
    logger.debug('  [rename   ] %s -> planned_end_date', 'planned_end_dt')
    df = df.withColumn('planned_end_date', F.col('planned_end_dt'))

    # RENAME: actual_end_dt -> actual_end_date
    logger.debug('  [rename   ] %s -> actual_end_date', 'actual_end_dt')
    df = df.withColumn('actual_end_date', F.col('actual_end_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: qty_planned -> planned_quantity
    logger.debug('  [rename   ] %s -> planned_quantity', 'qty_planned')
    df = df.withColumn('planned_quantity', F.col('qty_planned'))
    df = df.fillna({'planned_quantity': 0})

    # RENAME: qty_produced -> produced_quantity
    logger.debug('  [rename   ] %s -> produced_quantity', 'qty_produced')
    df = df.withColumn('produced_quantity', F.col('qty_produced'))
    df = df.fillna({'produced_quantity': 0})

    # RENAME: qty_rejected -> rejected_quantity
    logger.debug('  [rename   ] %s -> rejected_quantity', 'qty_rejected')
    df = df.withColumn('rejected_quantity', F.col('qty_rejected'))
    df = df.fillna({'rejected_quantity': 0})

    # RENAME: shift_cd -> shift_name
    logger.debug('  [rename   ] %s -> shift_name', 'shift_cd')
    df = df.withColumn('shift_name', F.col('shift_cd'))

    # RENAME: line_no -> production_line
    logger.debug('  [rename   ] %s -> production_line', 'line_no')
    df = df.withColumn('production_line', F.col('line_no'))

    # RENAME: supervisor_emp_id -> supervisor_key  # FK to dim_employee
    logger.debug('  [rename   ] %s -> supervisor_key', 'supervisor_emp_id')
    df = df.withColumn('supervisor_key', F.col('supervisor_emp_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: order_status_cd -> order_status
    logger.debug('  [rename   ] %s -> order_status', 'order_status_cd')
    df = df.withColumn('order_status', F.col('order_status_cd'))

    # RENAME: priority_lvl -> priority_level
    logger.debug('  [rename   ] %s -> priority_level', 'priority_lvl')
    df = df.withColumn('priority_level', F.col('priority_lvl'))

    # RENAME: downtime_mins -> downtime_minutes
    logger.debug('  [rename   ] %s -> downtime_minutes', 'downtime_mins')
    df = df.withColumn('downtime_minutes', F.col('downtime_mins'))
    df = df.fillna({'downtime_minutes': 0})

    # RENAME: scrap_cost_amt -> scrap_cost
    logger.debug('  [rename   ] %s -> scrap_cost', 'scrap_cost_amt')
    df = df.withColumn('scrap_cost', F.col('scrap_cost_amt'))
    df = df.fillna({'scrap_cost': 0})

    # RENAME: rework_hrs -> rework_hours
    logger.debug('  [rename   ] %s -> rework_hours', 'rework_hrs')
    df = df.withColumn('rework_hours', F.col('rework_hrs'))
    df = df.fillna({'rework_hours': 0})

    # RENAME: efficiency_pct -> efficiency_percent
    logger.debug('  [rename   ] %s -> efficiency_percent', 'efficiency_pct')
    df = df.withColumn('efficiency_percent', F.col('efficiency_pct'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: qty_planned -> yield_rate_pct  # Derived: produced/planned %
    logger.debug('  [derived  ] %s -> yield_rate_pct', 'qty_planned')
    df = df.withColumn('yield_rate_pct', F.when(F.col('qty_planned') > F.lit(0), F.col('ROUND((qty_produced/qty_planned)*100,2)')).otherwise(F.lit(0)))
    df = df.fillna({'yield_rate_pct': 0})

    # RENAME: target_takt_secs -> target_takt_seconds  # Standard takt time in seconds for the line
    logger.debug('  [rename   ] %s -> target_takt_seconds', 'target_takt_secs')
    df = df.withColumn('target_takt_seconds', F.col('target_takt_secs'))

    # DERIVED: actual_takt_secs -> takt_variance_pct  # Takt time deviation %
    logger.debug('  [derived  ] %s -> takt_variance_pct', 'actual_takt_secs')
    df = df.withColumn('takt_variance_pct', F.expr("ROUND(((actual_takt_secs-target_takt_secs)/target_takt_secs)*100,2)"))
    df = df.fillna({'takt_variance_pct': 0})

    # DIRECT: created_at -> created_at
    logger.debug('  [direct   ] %s -> created_at', 'created_at')
    df = df.withColumn('created_at', F.col('created_at'))

    # DIRECT: updated_at -> updated_at
    logger.debug('  [direct   ] %s -> updated_at', 'updated_at')
    df = df.withColumn('updated_at', F.col('updated_at'))

    # DROP: batch_flag — excluded from target  # Internal flag – not loaded
    logger.debug('  [drop]      batch_flag')

    # CONSTANT: (no src) -> load_ts
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # DERIVED: (no src) -> vehicle_vin  # VIN from vehicle_master
    logger.debug('  [derived  ] %s -> vehicle_vin', '')
    df = df.withColumn('vehicle_vin', F.col('veh_vin_number'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> vehicle_model  # Model name from vehicle_master
    logger.debug('  [derived  ] %s -> vehicle_model', '')
    df = df.withColumn('vehicle_model', F.col('veh_model_nm'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> vehicle_variant  # Variant from vehicle_master
    logger.debug('  [derived  ] %s -> vehicle_variant', '')
    df = df.withColumn('vehicle_variant', F.col('veh_variant_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # Reorder to target schema
    _exp  = ['production_order_key', 'vehicle_key', 'plant_code', 'order_date', 'planned_start_date', 'actual_start_date', 'planned_end_date', 'actual_end_date', 'planned_quantity', 'produced_quantity', 'rejected_quantity', 'shift_name', 'production_line', 'supervisor_key', 'order_status', 'priority_level', 'downtime_minutes', 'scrap_cost', 'rework_hours', 'efficiency_percent', 'yield_rate_pct', 'target_takt_seconds', 'takt_variance_pct', 'created_at', 'updated_at', 'load_ts', 'vehicle_vin', 'vehicle_model', 'vehicle_variant']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['production_order_key', 'vehicle_key', 'plant_code', 'order_date', 'planned_start_date', 'planned_end_date', 'shift_name', 'production_line', 'order_status', 'priority_level', 'target_takt_seconds', 'created_at', 'updated_at', 'load_ts']
    _null_exprs = [F.count(F.when(F.col(c).isNull(), 1)).alias(f'_null_{c}') for c in _nn_cols]
    _null_exprs.append(F.count('*').alias('_total_rows'))
    _null_result = df.select(*_null_exprs).first()
    _null_violations = {c: _null_result[f'_null_{c}'] for c in _nn_cols if _null_result[f'_null_{c}'] > 0}
    if _null_violations:
        for _col, _cnt in _null_violations.items():
            logger.error("  NULL in non-nullable '%s': %d rows", _col, _cnt)
        raise ValueError(f'NULL values in non-nullable columns: {_null_violations}')
    logger.info('  Null check passed for %d non-nullable columns', len(_nn_cols))
    logger.info('  Output rows : %d', _null_result['_total_rows'])

    # ── Dialect coercion: MySQL -> Snowflake value fixes ──────────────
    # Input is always MySQL dialect; coercions run only for 'snowflake'.
    if dialect == 'snowflake':
        df = df.withColumn('order_date', F.when(F.col('order_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('order_date')))
        logger.debug('  [sf-coerce] order_date: date -> null zero-dates')
        df = df.withColumn('planned_start_date', F.when(F.col('planned_start_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('planned_start_date')))
        logger.debug('  [sf-coerce] planned_start_date: date -> null zero-dates')
        df = df.withColumn('actual_start_date', F.when(F.col('actual_start_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('actual_start_date')))
        logger.debug('  [sf-coerce] actual_start_date: date -> null zero-dates')
        df = df.withColumn('planned_end_date', F.when(F.col('planned_end_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('planned_end_date')))
        logger.debug('  [sf-coerce] planned_end_date: date -> null zero-dates')
        df = df.withColumn('actual_end_date', F.when(F.col('actual_end_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('actual_end_date')))
        logger.debug('  [sf-coerce] actual_end_date: date -> null zero-dates')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('created_at').cast(TimestampType())))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType')
        df = df.withColumn('updated_at', F.when(F.col('updated_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('updated_at').cast(TimestampType())))
        logger.debug('  [sf-coerce] updated_at: datetime/timestamp -> TimestampType')
        df = df.withColumn('load_ts', F.when(F.col('load_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('load_ts').cast(TimestampType())))
        logger.debug('  [sf-coerce] load_ts: datetime/timestamp -> TimestampType')
    logger.info('  Output cols : %s', df.columns)
    logger.info('END TRANSFORM | dialect=%s', dialect)
    logger.info('=' * 70)
    return df
