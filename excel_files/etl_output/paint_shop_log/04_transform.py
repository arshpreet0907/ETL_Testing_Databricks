"""
04_transform.py  —  paint_shop_log  ->  fact_paint
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
        Transformed frame shaped to fact_paint target schema,
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
    Apply all column-level transforms for paint_shop_log.

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
    logger.info('START TRANSFORM | paint_shop_log -> fact_paint | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: paint_log_id -> paint_log_key
    logger.debug('  [direct   ] %s -> paint_log_key', 'paint_log_id')
    df = df.withColumn('paint_log_key', F.col('paint_log_id'))

    # RENAME: prod_order_id -> production_order_key  # FK to fact_production
    logger.debug('  [rename   ] %s -> production_order_key', 'prod_order_id')
    df = df.withColumn('production_order_key', F.col('prod_order_id'))

    # RENAME: vehicle_id -> vehicle_key
    logger.debug('  [rename   ] %s -> vehicle_key', 'vehicle_id')
    df = df.withColumn('vehicle_key', F.col('vehicle_id'))

    # RENAME: plant_cd -> plant_code
    logger.debug('  [rename   ] %s -> plant_code', 'plant_cd')
    df = df.withColumn('plant_code', F.col('plant_cd'))

    # RENAME: paint_line_no -> paint_line
    logger.debug('  [rename   ] %s -> paint_line', 'paint_line_no')
    df = df.withColumn('paint_line', F.col('paint_line_no'))

    # RENAME: shift_cd -> shift_name
    logger.debug('  [rename   ] %s -> shift_name', 'shift_cd')
    df = df.withColumn('shift_name', F.col('shift_cd'))

    # RENAME: color_cd -> color_code
    logger.debug('  [rename   ] %s -> color_code', 'color_cd')
    df = df.withColumn('color_code', F.col('color_cd'))

    # RENAME: color_desc -> color_name
    logger.debug('  [rename   ] %s -> color_name', 'color_desc')
    df = df.withColumn('color_name', F.col('color_desc'))

    # RENAME: oven_temp_celsius -> oven_temperature_c
    logger.debug('  [rename   ] %s -> oven_temperature_c', 'oven_temp_celsius')
    df = df.withColumn('oven_temperature_c', F.col('oven_temp_celsius'))

    # DIRECT: bake_duration_mins -> bake_duration_mins
    logger.debug('  [direct   ] %s -> bake_duration_mins', 'bake_duration_mins')
    df = df.withColumn('bake_duration_mins', F.col('bake_duration_mins'))

    # RENAME: paint_thickness_um -> thickness_um  # Microns
    logger.debug('  [rename   ] %s -> thickness_um', 'paint_thickness_um')
    df = df.withColumn('thickness_um', F.col('paint_thickness_um'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: gloss_level -> gloss_level
    logger.debug('  [direct   ] %s -> gloss_level', 'gloss_level')
    df = df.withColumn('gloss_level', F.col('gloss_level'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: defect_flag -> has_defect  # Y/N → 1/0
    logger.debug('  [derived  ] %s -> has_defect', 'defect_flag')
    df = df.withColumn('has_defect', F.when(F.col('defect_flag') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'has_defect': 0})

    # RENAME: defect_type_cd -> defect_type
    logger.debug('  [rename   ] %s -> defect_type', 'defect_type_cd')
    df = df.withColumn('defect_type', F.col('defect_type_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: rework_flag -> requires_rework  # Y/N → 1/0
    logger.debug('  [derived  ] %s -> requires_rework', 'rework_flag')
    df = df.withColumn('requires_rework', F.when(F.col('rework_flag') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'requires_rework': 0})

    # RENAME: paint_cost_amt -> paint_cost
    logger.debug('  [rename   ] %s -> paint_cost', 'paint_cost_amt')
    df = df.withColumn('paint_cost', F.col('paint_cost_amt'))

    # RENAME: operator_emp_id -> operator_key  # FK to dim_employee
    logger.debug('  [rename   ] %s -> operator_key', 'operator_emp_id')
    df = df.withColumn('operator_key', F.col('operator_emp_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: start_ts -> process_start_ts
    logger.debug('  [rename   ] %s -> process_start_ts', 'start_ts')
    df = df.withColumn('process_start_ts', F.col('start_ts'))

    # RENAME: end_ts -> process_end_ts
    logger.debug('  [rename   ] %s -> process_end_ts', 'end_ts')
    df = df.withColumn('process_end_ts', F.col('end_ts'))

    # RENAME: humidity_pct -> humidity_percent
    logger.debug('  [rename   ] %s -> humidity_percent', 'humidity_pct')
    df = df.withColumn('humidity_percent', F.col('humidity_pct'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: created_at -> created_at
    logger.debug('  [direct   ] %s -> created_at', 'created_at')
    df = df.withColumn('created_at', F.col('created_at'))

    # DROP: primer_batch_no — excluded from target  # Batch traceability – excluded
    logger.debug('  [drop]      primer_batch_no')

    # DROP: topcoat_batch_no — excluded from target  # Batch traceability – excluded
    logger.debug('  [drop]      topcoat_batch_no')

    # DROP: clear_coat_batch_no — excluded from target  # Batch traceability – excluded
    logger.debug('  [drop]      clear_coat_batch_no')

    # DROP: batch_id — excluded from target  # Internal – excluded
    logger.debug('  [drop]      batch_id')

    # CONSTANT: (no src) -> load_ts
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # DERIVED: (no src) -> order_date  # Order date from production_orders
    logger.debug('  [derived  ] %s -> order_date', '')
    df = df.withColumn('order_date', F.col('ord_order_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> planned_completion  # Planned end date from production_orders
    logger.debug('  [derived  ] %s -> planned_completion', '')
    df = df.withColumn('planned_completion', F.col('ord_planned_end_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> order_status  # Order status from production_orders
    logger.debug('  [derived  ] %s -> order_status', '')
    df = df.withColumn('order_status', F.col('ord_order_status_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # Reorder to target schema
    _exp  = ['paint_log_key', 'production_order_key', 'vehicle_key', 'plant_code', 'paint_line', 'shift_name', 'color_code', 'color_name', 'oven_temperature_c', 'bake_duration_mins', 'thickness_um', 'gloss_level', 'has_defect', 'defect_type', 'requires_rework', 'paint_cost', 'operator_key', 'process_start_ts', 'process_end_ts', 'humidity_percent', 'created_at', 'load_ts', 'order_date', 'planned_completion', 'order_status']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['paint_log_key', 'production_order_key', 'vehicle_key', 'plant_code', 'paint_line', 'shift_name', 'color_code', 'color_name', 'oven_temperature_c', 'bake_duration_mins', 'paint_cost', 'process_start_ts', 'process_end_ts', 'created_at', 'load_ts']
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
        df = df.withColumn('process_start_ts', F.when(F.col('process_start_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('process_start_ts').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] process_start_ts: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('process_end_ts', F.when(F.col('process_end_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('process_end_ts').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] process_end_ts: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('created_at').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
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
