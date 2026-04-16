"""
04_transform.py  —  quality_inspections  ->  fact_quality
Generated : 2026-04-16 02:58

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
        Transformed frame shaped to fact_quality target schema,
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
    Apply all column-level transforms for quality_inspections.

    The input DataFrame must contain every source field referenced in
    the mapping spec. Extra columns in the input are silently ignored.
    Input values are always assumed to be MySQL-dialect.
    Pass dialect='snowflake' to coerce values for Snowflake ingestion.

    Join columns expected in input DataFrame:
        [emp] employee_master: first_nm, last_nm
    """
    if dialect not in ('mysql', 'snowflake'):
        raise ValueError(f"dialect must be 'mysql' or 'snowflake', got {dialect!r}")
    logger.info("=" * 70)
    logger.info('START TRANSFORM | quality_inspections -> fact_quality | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: inspection_id -> inspection_key
    logger.debug('  [direct   ] %s -> inspection_key', 'inspection_id')
    df = df.withColumn('inspection_key', F.col('inspection_id'))

    # RENAME: prod_order_id -> production_order_key  # FK to fact_production
    logger.debug('  [rename   ] %s -> production_order_key', 'prod_order_id')
    df = df.withColumn('production_order_key', F.col('prod_order_id'))

    # RENAME: vehicle_id -> vehicle_key
    logger.debug('  [rename   ] %s -> vehicle_key', 'vehicle_id')
    df = df.withColumn('vehicle_key', F.col('vehicle_id'))

    # RENAME: inspector_emp_id -> inspector_key  # FK to dim_employee
    logger.debug('  [rename   ] %s -> inspector_key', 'inspector_emp_id')
    df = df.withColumn('inspector_key', F.col('inspector_emp_id'))

    # RENAME: inspection_dt -> inspection_date
    logger.debug('  [rename   ] %s -> inspection_date', 'inspection_dt')
    df = df.withColumn('inspection_date', F.col('inspection_dt'))

    # RENAME: inspection_type_cd -> inspection_type
    logger.debug('  [rename   ] %s -> inspection_type', 'inspection_type_cd')
    df = df.withColumn('inspection_type', F.col('inspection_type_cd'))

    # RENAME: defect_type_cd -> defect_type
    logger.debug('  [rename   ] %s -> defect_type', 'defect_type_cd')
    df = df.withColumn('defect_type', F.col('defect_type_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: defect_desc -> defect_description
    logger.debug('  [rename   ] %s -> defect_description', 'defect_desc')
    df = df.withColumn('defect_description', F.col('defect_desc'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: severity_cd -> severity_level
    logger.debug('  [rename   ] %s -> severity_level', 'severity_cd')
    df = df.withColumn('severity_level', F.col('severity_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: result_cd -> inspection_result  # PASS/FAIL/REWORK/HOLD
    logger.debug('  [rename   ] %s -> inspection_result', 'result_cd')
    df = df.withColumn('inspection_result', F.col('result_cd'))

    # RENAME: grade_cd -> quality_grade
    logger.debug('  [rename   ] %s -> quality_grade', 'grade_cd')
    df = df.withColumn('quality_grade', F.col('grade_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: inspection_score -> score
    logger.debug('  [rename   ] %s -> score', 'inspection_score')
    df = df.withColumn('score', F.col('inspection_score'))
    df = df.fillna({'score': 0})

    # DERIVED: rework_required_flag -> rework_required  # Y/N → 1/0
    logger.debug('  [derived  ] %s -> rework_required', 'rework_required_flag')
    df = df.withColumn('rework_required', F.when(F.col('rework_required_flag') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'rework_required': 0})

    # RENAME: rework_hrs -> rework_hours
    logger.debug('  [rename   ] %s -> rework_hours', 'rework_hrs')
    df = df.withColumn('rework_hours', F.col('rework_hrs'))
    df = df.fillna({'rework_hours': 0})

    # RENAME: rework_cost_amt -> rework_cost
    logger.debug('  [rename   ] %s -> rework_cost', 'rework_cost_amt')
    df = df.withColumn('rework_cost', F.col('rework_cost_amt'))
    df = df.fillna({'rework_cost': 0})

    # RENAME: plant_cd -> plant_code
    logger.debug('  [rename   ] %s -> plant_code', 'plant_cd')
    df = df.withColumn('plant_code', F.col('plant_cd'))

    # RENAME: shift_cd -> shift_name
    logger.debug('  [rename   ] %s -> shift_name', 'shift_cd')
    df = df.withColumn('shift_name', F.col('shift_cd'))

    # RENAME: line_no -> production_line
    logger.debug('  [rename   ] %s -> production_line', 'line_no')
    df = df.withColumn('production_line', F.col('line_no'))

    # RENAME: checkpoint_no -> checkpoint_number
    logger.debug('  [rename   ] %s -> checkpoint_number', 'checkpoint_no')
    df = df.withColumn('checkpoint_number', F.col('checkpoint_no'))

    # DERIVED: defect_type_cd -> is_defect_flag  # 1 if any defect recorded
    logger.debug('  [derived  ] %s -> is_defect_flag', 'defect_type_cd')
    df = df.withColumn('is_defect_flag', F.when(F.col('defect_type_cd').isNull(), F.lit(0)).otherwise(F.lit(1)))
    df = df.fillna({'is_defect_flag': 0})

    # DIRECT: created_at -> created_at
    logger.debug('  [direct   ] %s -> created_at', 'created_at')
    df = df.withColumn('created_at', F.col('created_at'))

    # DIRECT: updated_at -> updated_at
    logger.debug('  [direct   ] %s -> updated_at', 'updated_at')
    df = df.withColumn('updated_at', F.col('updated_at'))

    # DROP: tool_id — excluded from target  # Operational – excluded
    logger.debug('  [drop]      tool_id')

    # DROP: photo_ref_id — excluded from target  # Blob ref – excluded
    logger.debug('  [drop]      photo_ref_id')

    # DROP: batch_id — excluded from target  # Internal batch – excluded
    logger.debug('  [drop]      batch_id')

    # CONSTANT: (no src) -> load_ts
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # DERIVED: (no src) -> inspector_name  # Inspector full name from employee_master
    logger.debug('  [derived  ] %s -> inspector_name', '')
    df = df.withColumn('inspector_name', F.expr("emp_first_nm || ' ' || emp_last_nm"))
    # NULL values remain as NULL (not replaced with empty string)

    # Reorder to target schema
    _exp  = ['inspection_key', 'production_order_key', 'vehicle_key', 'inspector_key', 'inspection_date', 'inspection_type', 'defect_type', 'defect_description', 'severity_level', 'inspection_result', 'quality_grade', 'score', 'rework_required', 'rework_hours', 'rework_cost', 'plant_code', 'shift_name', 'production_line', 'checkpoint_number', 'is_defect_flag', 'created_at', 'updated_at', 'load_ts', 'inspector_name']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['inspection_key', 'production_order_key', 'vehicle_key', 'inspector_key', 'inspection_date', 'inspection_type', 'inspection_result', 'plant_code', 'shift_name', 'production_line', 'checkpoint_number', 'created_at', 'updated_at', 'load_ts']
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
        df = df.withColumn('inspection_date', F.when(F.col('inspection_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('inspection_date')))
        logger.debug('  [sf-coerce] inspection_date: date -> null zero-dates')
        df = df.withColumn('quality_grade', F.rtrim(F.col('quality_grade')))
        logger.debug('  [sf-coerce] quality_grade: char -> rtrim whitespace')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('created_at').cast(TimestampType())))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType, null zero-datetimes')
        df = df.withColumn('updated_at', F.when(F.col('updated_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('updated_at').cast(TimestampType())))
        logger.debug('  [sf-coerce] updated_at: datetime/timestamp -> TimestampType, null zero-datetimes')
        df = df.withColumn('load_ts', F.when(F.col('load_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('load_ts').cast(TimestampType())))
        logger.debug('  [sf-coerce] load_ts: datetime/timestamp -> TimestampType, null zero-datetimes')
    logger.info('  Output cols : %s', df.columns)
    logger.info('END TRANSFORM | dialect=%s', dialect)
    logger.info('=' * 70)
    return df
