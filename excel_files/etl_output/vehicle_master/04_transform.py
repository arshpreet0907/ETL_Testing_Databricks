"""
04_transform.py  —  vehicle_master  ->  dim_vehicle
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

    dialect : Literal['mysql', 'snowflake']
        Target dialect for the returned DataFrame.
        - 'mysql'     : values left in MySQL-native form (default).
        - 'snowflake' : applies dialect coercions so the frame is
                        ready for write_pandas / Snowflake ingestion.

Returns
-------
    pyspark.sql.DataFrame
        Transformed frame shaped to dim_vehicle target schema,
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
    Apply all column-level transforms for vehicle_master.

    The input DataFrame must contain every source field referenced in
    the mapping spec. Extra columns in the input are silently ignored.
    Input values are always assumed to be MySQL-dialect.
    Pass dialect='snowflake' to coerce values for Snowflake ingestion.
    """
    if dialect not in ('mysql', 'snowflake'):
        raise ValueError(f"dialect must be 'mysql' or 'snowflake', got {dialect!r}")
    logger.info("=" * 70)
    logger.info('START TRANSFORM | vehicle_master -> dim_vehicle | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: vehicle_id -> vehicle_key  # Surrogate key
    logger.debug('  [direct   ] %s -> vehicle_key', 'vehicle_id')
    df = df.withColumn('vehicle_key', F.col('vehicle_id'))

    # RENAME: vin_number -> vin  # Vehicle Identification Number
    logger.debug('  [rename   ] %s -> vin', 'vin_number')
    df = df.withColumn('vin', F.col('vin_number'))

    # RENAME: model_nm -> model_name  # Full model name
    logger.debug('  [rename   ] %s -> model_name', 'model_nm')
    df = df.withColumn('model_name', F.col('model_nm'))

    # RENAME: variant_cd -> variant_name  # Variant / trim level
    logger.debug('  [rename   ] %s -> variant_name', 'variant_cd')
    df = df.withColumn('variant_name', F.col('variant_cd'))

    # RENAME: model_yr -> model_year  # Manufacturing year
    logger.debug('  [rename   ] %s -> model_year', 'model_yr')
    df = df.withColumn('model_year', F.col('model_yr'))

    # RENAME: color_desc -> color_name
    logger.debug('  [rename   ] %s -> color_name', 'color_desc')
    df = df.withColumn('color_name', F.col('color_desc'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: engine_type_cd -> engine_type
    logger.debug('  [rename   ] %s -> engine_type', 'engine_type_cd')
    df = df.withColumn('engine_type', F.col('engine_type_cd'))

    # RENAME: transmission_cd -> transmission_type
    logger.debug('  [rename   ] %s -> transmission_type', 'transmission_cd')
    df = df.withColumn('transmission_type', F.col('transmission_cd'))

    # RENAME: plant_cd -> manufacturing_plant
    logger.debug('  [rename   ] %s -> manufacturing_plant', 'plant_cd')
    df = df.withColumn('manufacturing_plant', F.col('plant_cd'))

    # CAST: base_price_amt -> base_price_inr  # Price rounded to 2dp
    logger.debug('  [cast     ] %s -> base_price_inr', 'base_price_amt')
    df = df.withColumn('base_price_inr', F.expr("ROUND(base_price_amt, 2)"))

    # RENAME: launch_dt -> launch_date
    logger.debug('  [rename   ] %s -> launch_date', 'launch_dt')
    df = df.withColumn('launch_date', F.col('launch_dt'))

    # RENAME: discontinue_dt -> discontinue_date
    logger.debug('  [rename   ] %s -> discontinue_date', 'discontinue_dt')
    df = df.withColumn('discontinue_date', F.col('discontinue_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: status_cd -> vehicle_status
    logger.debug('  [rename   ] %s -> vehicle_status', 'status_cd')
    df = df.withColumn('vehicle_status', F.col('status_cd'))

    # RENAME: fuel_economy_kmpl -> fuel_economy
    logger.debug('  [rename   ] %s -> fuel_economy', 'fuel_economy_kmpl')
    df = df.withColumn('fuel_economy', F.col('fuel_economy_kmpl'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: gross_wt_kg -> gross_weight_kg
    logger.debug('  [rename   ] %s -> gross_weight_kg', 'gross_wt_kg')
    df = df.withColumn('gross_weight_kg', F.col('gross_wt_kg'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: seating_capacity -> seating_capacity
    logger.debug('  [direct   ] %s -> seating_capacity', 'seating_capacity')
    df = df.withColumn('seating_capacity', F.col('seating_capacity'))

    # RENAME: country_of_origin -> origin_country
    logger.debug('  [rename   ] %s -> origin_country', 'country_of_origin')
    df = df.withColumn('origin_country', F.col('country_of_origin'))

    # DIRECT: safety_rating -> safety_rating
    logger.debug('  [direct   ] %s -> safety_rating', 'safety_rating')
    df = df.withColumn('safety_rating', F.col('safety_rating'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: warranty_yrs -> warranty_years
    logger.debug('  [rename   ] %s -> warranty_years', 'warranty_yrs')
    df = df.withColumn('warranty_years', F.col('warranty_yrs'))

    # DERIVED: is_electric_flag -> is_electric  # Y/N → 1/0 boolean
    logger.debug('  [derived  ] %s -> is_electric', 'is_electric_flag')
    df = df.withColumn('is_electric', F.when(F.col('is_electric_flag') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'is_electric': 0})

    # DIRECT: created_at -> created_at
    logger.debug('  [direct   ] %s -> created_at', 'created_at')
    df = df.withColumn('created_at', F.col('created_at'))

    # DIRECT: updated_at -> updated_at
    logger.debug('  [direct   ] %s -> updated_at', 'updated_at')
    df = df.withColumn('updated_at', F.col('updated_at'))

    # DIRECT: created_by -> created_by
    logger.debug('  [direct   ] %s -> created_by', 'created_by')
    df = df.withColumn('created_by', F.col('created_by'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: payload_kg -> payload_kg
    logger.debug('  [direct   ] %s -> payload_kg', 'payload_kg')
    df = df.withColumn('payload_kg', F.col('payload_kg'))
    # NULL values remain as NULL (not replaced with empty string)

    # DROP: internal_notes — excluded from target  # Sensitive – excluded from target
    logger.debug('  [drop]      internal_notes')

    # CONSTANT: (no src) -> load_ts  # ETL pipeline load timestamp
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # CONSTANT: (no src) -> batch_id  # ETL batch identifier
    logger.debug('  [constant ] %s -> batch_id', '')
    df = df.withColumn('batch_id', F.lit("ETL_VALIDATION")  # placeholder — real batch ID set by ETL engine)

    # Reorder to target schema
    _exp  = ['vehicle_key', 'vin', 'model_name', 'variant_name', 'model_year', 'color_name', 'engine_type', 'transmission_type', 'manufacturing_plant', 'base_price_inr', 'launch_date', 'discontinue_date', 'vehicle_status', 'fuel_economy', 'gross_weight_kg', 'seating_capacity', 'origin_country', 'safety_rating', 'warranty_years', 'is_electric', 'created_at', 'updated_at', 'created_by', 'payload_kg', 'load_ts', 'batch_id']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['vehicle_key', 'vin', 'model_name', 'variant_name', 'model_year', 'engine_type', 'transmission_type', 'manufacturing_plant', 'base_price_inr', 'launch_date', 'vehicle_status', 'seating_capacity', 'origin_country', 'warranty_years', 'created_at', 'updated_at', 'load_ts', 'batch_id']
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
        df = df.withColumn('launch_date', F.when(F.col('launch_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('launch_date')))
        logger.debug('  [sf-coerce] launch_date: date -> null zero-dates')
        df = df.withColumn('discontinue_date', F.when(F.col('discontinue_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('discontinue_date')))
        logger.debug('  [sf-coerce] discontinue_date: date -> null zero-dates')
        df = df.withColumn('fuel_economy', F.col('fuel_economy').cast(DoubleType()))
        logger.debug('  [sf-coerce] fuel_economy: decimal/numeric -> DoubleType')
        df = df.withColumn('gross_weight_kg', F.col('gross_weight_kg').cast(DoubleType()))
        logger.debug('  [sf-coerce] gross_weight_kg: decimal/numeric -> DoubleType')
        df = df.withColumn('safety_rating', F.col('safety_rating').cast(DoubleType()))
        logger.debug('  [sf-coerce] safety_rating: decimal/numeric -> DoubleType')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('created_at').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('updated_at', F.when(F.col('updated_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('updated_at').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] updated_at: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('payload_kg', F.col('payload_kg').cast(DoubleType()))
        logger.debug('  [sf-coerce] payload_kg: decimal/numeric -> DoubleType')
        df = df.withColumn('load_ts', F.when(F.col('load_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('load_ts').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] load_ts: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
    logger.info('  Output cols : %s', df.columns)
    logger.info('END TRANSFORM | dialect=%s', dialect)
    logger.info('=' * 70)
    return df
