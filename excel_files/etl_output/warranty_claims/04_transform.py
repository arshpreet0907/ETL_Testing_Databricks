"""
04_transform.py  —  warranty_claims  ->  fact_warranty
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
        Transformed frame shaped to fact_warranty target schema,
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
    Apply all column-level transforms for warranty_claims.

    The input DataFrame must contain every source field referenced in
    the mapping spec. Extra columns in the input are silently ignored.
    Input values are always assumed to be MySQL-dialect.
    Pass dialect='snowflake' to coerce values for Snowflake ingestion.

    Join columns expected in input DataFrame:
        [veh] vehicle_master: vin_number, model_nm, engine_type_cd
    """
    if dialect not in ('mysql', 'snowflake'):
        raise ValueError(f"dialect must be 'mysql' or 'snowflake', got {dialect!r}")
    logger.info("=" * 70)
    logger.info('START TRANSFORM | warranty_claims -> fact_warranty | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: claim_id -> claim_key
    logger.debug('  [direct   ] %s -> claim_key', 'claim_id')
    df = df.withColumn('claim_key', F.col('claim_id'))

    # RENAME: vin_number -> vin
    logger.debug('  [rename   ] %s -> vin', 'vin_number')
    df = df.withColumn('vin', F.col('vin_number'))

    # RENAME: vehicle_id -> vehicle_key
    logger.debug('  [rename   ] %s -> vehicle_key', 'vehicle_id')
    df = df.withColumn('vehicle_key', F.col('vehicle_id'))

    # RENAME: customer_id -> customer_key
    logger.debug('  [rename   ] %s -> customer_key', 'customer_id')
    df = df.withColumn('customer_key', F.col('customer_id'))

    # RENAME: claim_dt -> claim_date
    logger.debug('  [rename   ] %s -> claim_date', 'claim_dt')
    df = df.withColumn('claim_date', F.col('claim_dt'))

    # RENAME: reported_dt -> reported_date
    logger.debug('  [rename   ] %s -> reported_date', 'reported_dt')
    df = df.withColumn('reported_date', F.col('reported_dt'))

    # RENAME: defect_type_cd -> defect_type
    logger.debug('  [rename   ] %s -> defect_type', 'defect_type_cd')
    df = df.withColumn('defect_type', F.col('defect_type_cd'))

    # RENAME: defect_desc -> defect_description
    logger.debug('  [rename   ] %s -> defect_description', 'defect_desc')
    df = df.withColumn('defect_description', F.col('defect_desc'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: part_id -> part_key  # FK to dim_parts
    logger.debug('  [rename   ] %s -> part_key', 'part_id')
    df = df.withColumn('part_key', F.col('part_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: repair_cost_amt -> repair_cost
    logger.debug('  [rename   ] %s -> repair_cost', 'repair_cost_amt')
    df = df.withColumn('repair_cost', F.col('repair_cost_amt'))

    # RENAME: labour_cost_amt -> labour_cost
    logger.debug('  [rename   ] %s -> labour_cost', 'labour_cost_amt')
    df = df.withColumn('labour_cost', F.col('labour_cost_amt'))

    # RENAME: parts_cost_amt -> parts_cost
    logger.debug('  [rename   ] %s -> parts_cost', 'parts_cost_amt')
    df = df.withColumn('parts_cost', F.col('parts_cost_amt'))

    # DERIVED: repair_cost_amt -> total_claim_amt  # Sum of all cost components
    logger.debug('  [derived  ] %s -> total_claim_amt', 'repair_cost_amt')
    df = df.withColumn('total_claim_amt', F.expr("repair_cost_amt + labour_cost_amt + parts_cost_amt"))

    # RENAME: dealer_id -> dealer_key
    logger.debug('  [rename   ] %s -> dealer_key', 'dealer_id')
    df = df.withColumn('dealer_key', F.col('dealer_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: service_center_id -> service_center_key
    logger.debug('  [rename   ] %s -> service_center_key', 'service_center_id')
    df = df.withColumn('service_center_key', F.col('service_center_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: technician_emp_id -> technician_key  # FK to dim_employee
    logger.debug('  [rename   ] %s -> technician_key', 'technician_emp_id')
    df = df.withColumn('technician_key', F.col('technician_emp_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: repair_start_dt -> repair_start_date
    logger.debug('  [rename   ] %s -> repair_start_date', 'repair_start_dt')
    df = df.withColumn('repair_start_date', F.col('repair_start_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: repair_end_dt -> repair_end_date
    logger.debug('  [rename   ] %s -> repair_end_date', 'repair_end_dt')
    df = df.withColumn('repair_end_date', F.col('repair_end_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: claim_status_cd -> claim_status
    logger.debug('  [rename   ] %s -> claim_status', 'claim_status_cd')
    df = df.withColumn('claim_status', F.col('claim_status_cd'))

    # DERIVED: supplier_liability_flag -> supplier_liable  # Y/N → 1/0
    logger.debug('  [derived  ] %s -> supplier_liable', 'supplier_liability_flag')
    df = df.withColumn('supplier_liable', F.when(F.col('supplier_liability_flag') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'supplier_liable': 0})

    # DIRECT: mileage_km -> mileage_km
    logger.debug('  [direct   ] %s -> mileage_km', 'mileage_km')
    df = df.withColumn('mileage_km', F.col('mileage_km'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: odometer_reading -> odometer_km
    logger.debug('  [rename   ] %s -> odometer_km', 'odometer_reading')
    df = df.withColumn('odometer_km', F.col('odometer_reading'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: created_at -> created_at
    logger.debug('  [direct   ] %s -> created_at', 'created_at')
    df = df.withColumn('created_at', F.col('created_at'))

    # DIRECT: approved_by -> approved_by
    logger.debug('  [direct   ] %s -> approved_by', 'approved_by')
    df = df.withColumn('approved_by', F.col('approved_by'))
    # NULL values remain as NULL (not replaced with empty string)

    # DROP: internal_notes — excluded from target  # Sensitive – excluded
    logger.debug('  [drop]      internal_notes')

    # CONSTANT: (no src) -> load_ts
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # DERIVED: (no src) -> vehicle_model  # Vehicle model on claim
    logger.debug('  [derived  ] %s -> vehicle_model', '')
    df = df.withColumn('vehicle_model', F.col('veh_model_nm'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> engine_type  # Engine type on claim
    logger.debug('  [derived  ] %s -> engine_type', '')
    df = df.withColumn('engine_type', F.col('veh_engine_type_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # Reorder to target schema
    _exp  = ['claim_key', 'vin', 'vehicle_key', 'customer_key', 'claim_date', 'reported_date', 'defect_type', 'defect_description', 'part_key', 'repair_cost', 'labour_cost', 'parts_cost', 'total_claim_amt', 'dealer_key', 'service_center_key', 'technician_key', 'repair_start_date', 'repair_end_date', 'claim_status', 'supplier_liable', 'mileage_km', 'odometer_km', 'created_at', 'approved_by', 'load_ts', 'vehicle_model', 'engine_type']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['claim_key', 'vin', 'vehicle_key', 'customer_key', 'claim_date', 'reported_date', 'defect_type', 'repair_cost', 'labour_cost', 'parts_cost', 'total_claim_amt', 'claim_status', 'created_at', 'load_ts']
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
        df = df.withColumn('claim_date', F.when(F.col('claim_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('claim_date')))
        logger.debug('  [sf-coerce] claim_date: date -> null zero-dates')
        df = df.withColumn('reported_date', F.when(F.col('reported_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('reported_date')))
        logger.debug('  [sf-coerce] reported_date: date -> null zero-dates')
        df = df.withColumn('repair_start_date', F.when(F.col('repair_start_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('repair_start_date')))
        logger.debug('  [sf-coerce] repair_start_date: date -> null zero-dates')
        df = df.withColumn('repair_end_date', F.when(F.col('repair_end_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('repair_end_date')))
        logger.debug('  [sf-coerce] repair_end_date: date -> null zero-dates')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('created_at').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('load_ts', F.when(F.col('load_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('load_ts').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] load_ts: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
    logger.info('  Output cols : %s', df.columns)
    logger.info('END TRANSFORM | dialect=%s', dialect)
    logger.info('=' * 70)
    return df
