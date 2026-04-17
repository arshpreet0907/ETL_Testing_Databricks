"""
04_transform.py  —  engine_assembly_log  ->  fact_engine_assembly
Generated : 2026-04-17 15:17

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
        Transformed frame shaped to fact_engine_assembly target schema,
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
    Apply all column-level transforms for engine_assembly_log.

    The input DataFrame must contain every source field referenced in
    the mapping spec. Extra columns in the input are silently ignored.
    Input values are always assumed to be MySQL-dialect.
    Pass dialect='snowflake' to coerce values for Snowflake ingestion.

    Join columns expected in input DataFrame:
        [ord] production_orders: order_dt, planned_end_dt, order_status_cd
    """
    if dialect not in ('mysql', 'snowflake'):
        raise ValueError(f"dialect must be 'mysql' or 'snowflake', got {dialect!r}")
    logger.info("=" * 70)
    logger.info('START TRANSFORM | engine_assembly_log -> fact_engine_assembly | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: assembly_log_id -> assembly_log_key
    logger.debug('  [direct   ] %s -> assembly_log_key', 'assembly_log_id')
    df = df.withColumn('assembly_log_key', F.col('assembly_log_id'))

    # RENAME: prod_order_id -> production_order_key  # FK to fact_production
    logger.debug('  [rename   ] %s -> production_order_key', 'prod_order_id')
    df = df.withColumn('production_order_key', F.col('prod_order_id'))

    # RENAME: engine_serial_no -> engine_serial
    logger.debug('  [rename   ] %s -> engine_serial', 'engine_serial_no')
    df = df.withColumn('engine_serial', F.col('engine_serial_no'))

    # RENAME: vehicle_id -> vehicle_key
    logger.debug('  [rename   ] %s -> vehicle_key', 'vehicle_id')
    df = df.withColumn('vehicle_key', F.col('vehicle_id'))

    # RENAME: engine_type_cd -> engine_type
    logger.debug('  [rename   ] %s -> engine_type', 'engine_type_cd')
    df = df.withColumn('engine_type', F.col('engine_type_cd'))

    # RENAME: plant_cd -> plant_code
    logger.debug('  [rename   ] %s -> plant_code', 'plant_cd')
    df = df.withColumn('plant_code', F.col('plant_cd'))

    # RENAME: assembly_line_no -> assembly_line
    logger.debug('  [rename   ] %s -> assembly_line', 'assembly_line_no')
    df = df.withColumn('assembly_line', F.col('assembly_line_no'))

    # RENAME: shift_cd -> shift_name
    logger.debug('  [rename   ] %s -> shift_name', 'shift_cd')
    df = df.withColumn('shift_name', F.col('shift_cd'))

    # RENAME: operator_emp_id -> operator_key  # FK to dim_employee
    logger.debug('  [rename   ] %s -> operator_key', 'operator_emp_id')
    df = df.withColumn('operator_key', F.col('operator_emp_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: start_ts -> assembly_start_ts
    logger.debug('  [rename   ] %s -> assembly_start_ts', 'start_ts')
    df = df.withColumn('assembly_start_ts', F.col('start_ts'))

    # RENAME: end_ts -> assembly_end_ts
    logger.debug('  [rename   ] %s -> assembly_end_ts', 'end_ts')
    df = df.withColumn('assembly_end_ts', F.col('end_ts'))

    # DIRECT: torque_nm -> torque_nm
    logger.debug('  [direct   ] %s -> torque_nm', 'torque_nm')
    df = df.withColumn('torque_nm', F.col('torque_nm'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: compression_ratio -> compression_ratio
    logger.debug('  [direct   ] %s -> compression_ratio', 'compression_ratio')
    df = df.withColumn('compression_ratio', F.col('compression_ratio'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: idle_rpm -> idle_rpm
    logger.debug('  [direct   ] %s -> idle_rpm', 'idle_rpm')
    df = df.withColumn('idle_rpm', F.col('idle_rpm'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: max_rpm -> max_rpm
    logger.debug('  [direct   ] %s -> max_rpm', 'max_rpm')
    df = df.withColumn('max_rpm', F.col('max_rpm'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: oil_pressure_bar -> oil_pressure_bar
    logger.debug('  [direct   ] %s -> oil_pressure_bar', 'oil_pressure_bar')
    df = df.withColumn('oil_pressure_bar', F.col('oil_pressure_bar'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: coolant_temp_c -> coolant_temp_c
    logger.debug('  [direct   ] %s -> coolant_temp_c', 'coolant_temp_c')
    df = df.withColumn('coolant_temp_c', F.col('coolant_temp_c'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: test_result_cd -> test_result  # PASS/FAIL/RETEST
    logger.debug('  [rename   ] %s -> test_result', 'test_result_cd')
    df = df.withColumn('test_result', F.col('test_result_cd'))

    # DERIVED: defect_flag -> has_defect  # Y/N → 1/0
    logger.debug('  [derived  ] %s -> has_defect', 'defect_flag')
    df = df.withColumn('has_defect', F.when(F.col('defect_flag') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'has_defect': 0})

    # RENAME: defect_desc -> defect_description
    logger.debug('  [rename   ] %s -> defect_description', 'defect_desc')
    df = df.withColumn('defect_description', F.col('defect_desc'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: rework_hrs -> rework_hours
    logger.debug('  [rename   ] %s -> rework_hours', 'rework_hrs')
    df = df.withColumn('rework_hours', F.col('rework_hrs'))
    df = df.fillna({'rework_hours': 0})

    # RENAME: assembly_cost_amt -> assembly_cost
    logger.debug('  [rename   ] %s -> assembly_cost', 'assembly_cost_amt')
    df = df.withColumn('assembly_cost', F.col('assembly_cost_amt'))

    # DIRECT: created_at -> created_at
    logger.debug('  [direct   ] %s -> created_at', 'created_at')
    df = df.withColumn('created_at', F.col('created_at'))

    # DIRECT: updated_at -> updated_at
    logger.debug('  [direct   ] %s -> updated_at', 'updated_at')
    df = df.withColumn('updated_at', F.col('updated_at'))

    # DROP: batch_id — excluded from target  # Internal – excluded
    logger.debug('  [drop]      batch_id')

    # CONSTANT: (no src) -> load_ts
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # DERIVED: (no src) -> order_date  # Order date from production_orders
    logger.debug('  [derived  ] %s -> order_date', '')
    df = df.withColumn('order_date', F.col('ord_order_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> planned_completion  # Planned end date
    logger.debug('  [derived  ] %s -> planned_completion', '')
    df = df.withColumn('planned_completion', F.col('ord_planned_end_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> order_status  # Order status
    logger.debug('  [derived  ] %s -> order_status', '')
    df = df.withColumn('order_status', F.col('ord_order_status_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # Reorder to target schema
    _exp  = ['assembly_log_key', 'production_order_key', 'engine_serial', 'vehicle_key', 'engine_type', 'plant_code', 'assembly_line', 'shift_name', 'operator_key', 'assembly_start_ts', 'assembly_end_ts', 'torque_nm', 'compression_ratio', 'idle_rpm', 'max_rpm', 'oil_pressure_bar', 'coolant_temp_c', 'test_result', 'has_defect', 'defect_description', 'rework_hours', 'assembly_cost', 'created_at', 'updated_at', 'load_ts', 'order_date', 'planned_completion', 'order_status']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['assembly_log_key', 'production_order_key', 'engine_serial', 'vehicle_key', 'engine_type', 'plant_code', 'assembly_line', 'shift_name', 'assembly_start_ts', 'assembly_end_ts', 'test_result', 'assembly_cost', 'created_at', 'updated_at', 'load_ts']
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
        df = df.withColumn('assembly_start_ts', F.when(F.col('assembly_start_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('assembly_start_ts').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] assembly_start_ts: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('assembly_end_ts', F.when(F.col('assembly_end_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('assembly_end_ts').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] assembly_end_ts: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('created_at').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('updated_at', F.when(F.col('updated_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('updated_at').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] updated_at: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('load_ts', F.when(F.col('load_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('load_ts').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] load_ts: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('order_date', F.when(F.col('order_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('order_date')))
        logger.debug('  [sf-coerce] order_date: date -> null zero-dates')
        df = df.withColumn('planned_completion', F.when(F.col('planned_completion').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('planned_completion')))
        logger.debug('  [sf-coerce] planned_completion: date -> null zero-dates')
    logger.info('  Output cols : %s', df.columns)
    logger.info('END TRANSFORM | dialect=%s', dialect)
    logger.info('=' * 70)
    return df
