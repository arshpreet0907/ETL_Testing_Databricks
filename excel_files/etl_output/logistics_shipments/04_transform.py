"""
04_transform.py  —  logistics_shipments  ->  fact_shipment
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
        Transformed frame shaped to fact_shipment target schema,
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
    Apply all column-level transforms for logistics_shipments.

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
    logger.info('START TRANSFORM | logistics_shipments -> fact_shipment | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: shipment_id -> shipment_key
    logger.debug('  [direct   ] %s -> shipment_key', 'shipment_id')
    df = df.withColumn('shipment_key', F.col('shipment_id'))

    # RENAME: prod_order_id -> production_order_key  # FK to fact_production
    logger.debug('  [rename   ] %s -> production_order_key', 'prod_order_id')
    df = df.withColumn('production_order_key', F.col('prod_order_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: sales_order_id -> sales_order_key  # FK to fact_sales
    logger.debug('  [rename   ] %s -> sales_order_key', 'sales_order_id')
    df = df.withColumn('sales_order_key', F.col('sales_order_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: vehicle_id -> vehicle_key
    logger.debug('  [rename   ] %s -> vehicle_key', 'vehicle_id')
    df = df.withColumn('vehicle_key', F.col('vehicle_id'))

    # RENAME: origin_plant_cd -> origin_plant
    logger.debug('  [rename   ] %s -> origin_plant', 'origin_plant_cd')
    df = df.withColumn('origin_plant', F.col('origin_plant_cd'))

    # RENAME: dest_dealer_id -> destination_dealer_key  # FK to dim_dealer
    logger.debug('  [rename   ] %s -> destination_dealer_key', 'dest_dealer_id')
    df = df.withColumn('destination_dealer_key', F.col('dest_dealer_id'))

    # RENAME: carrier_nm -> carrier_name
    logger.debug('  [rename   ] %s -> carrier_name', 'carrier_nm')
    df = df.withColumn('carrier_name', F.col('carrier_nm'))

    # RENAME: shipment_dt -> shipment_date
    logger.debug('  [rename   ] %s -> shipment_date', 'shipment_dt')
    df = df.withColumn('shipment_date', F.col('shipment_dt'))

    # RENAME: estimated_arrival_dt -> estimated_arrival
    logger.debug('  [rename   ] %s -> estimated_arrival', 'estimated_arrival_dt')
    df = df.withColumn('estimated_arrival', F.col('estimated_arrival_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: actual_arrival_dt -> actual_arrival
    logger.debug('  [rename   ] %s -> actual_arrival', 'actual_arrival_dt')
    df = df.withColumn('actual_arrival', F.col('actual_arrival_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: transport_mode_cd -> transport_mode
    logger.debug('  [rename   ] %s -> transport_mode', 'transport_mode_cd')
    df = df.withColumn('transport_mode', F.col('transport_mode_cd'))

    # RENAME: tracking_no -> tracking_number
    logger.debug('  [rename   ] %s -> tracking_number', 'tracking_no')
    df = df.withColumn('tracking_number', F.col('tracking_no'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: vehicle_count -> vehicle_count
    logger.debug('  [direct   ] %s -> vehicle_count', 'vehicle_count')
    df = df.withColumn('vehicle_count', F.col('vehicle_count'))

    # RENAME: freight_cost_amt -> freight_cost
    logger.debug('  [rename   ] %s -> freight_cost', 'freight_cost_amt')
    df = df.withColumn('freight_cost', F.col('freight_cost_amt'))

    # RENAME: insurance_cost_amt -> insurance_cost
    logger.debug('  [rename   ] %s -> insurance_cost', 'insurance_cost_amt')
    df = df.withColumn('insurance_cost', F.col('insurance_cost_amt'))

    # RENAME: total_cost_amt -> total_cost
    logger.debug('  [rename   ] %s -> total_cost', 'total_cost_amt')
    df = df.withColumn('total_cost', F.col('total_cost_amt'))

    # RENAME: status_cd -> shipment_status
    logger.debug('  [rename   ] %s -> shipment_status', 'status_cd')
    df = df.withColumn('shipment_status', F.col('status_cd'))

    # RENAME: delay_reason_cd -> delay_reason
    logger.debug('  [rename   ] %s -> delay_reason', 'delay_reason_cd')
    df = df.withColumn('delay_reason', F.col('delay_reason_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: distance_km -> distance_km
    logger.debug('  [direct   ] %s -> distance_km', 'distance_km')
    df = df.withColumn('distance_km', F.col('distance_km'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: estimated_arrival_dt -> is_delayed_flag  # 1 if arrived after ETA
    logger.debug('  [derived  ] %s -> is_delayed_flag', 'estimated_arrival_dt')
    df = df.withColumn('is_delayed_flag', F.expr("1 if actual_arrival_dt and actual_arrival_dt > estimated_arrival_dt else 0"))
    df = df.fillna({'is_delayed_flag': 0})

    # DERIVED: total_cost_amt -> cost_per_vehicle  # Total cost ÷ vehicle count
    logger.debug('  [derived  ] %s -> cost_per_vehicle', 'total_cost_amt')
    df = df.withColumn('cost_per_vehicle', F.when(F.col('vehicle_count') > F.lit(0), F.col('ROUND(total_cost_amt / vehicle_count, 2)')).otherwise(F.lit(0)))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: created_at -> created_at
    logger.debug('  [direct   ] %s -> created_at', 'created_at')
    df = df.withColumn('created_at', F.col('created_at'))

    # DROP: driver_nm — excluded from target  # PII – excluded
    logger.debug('  [drop]      driver_nm')

    # DROP: driver_phone — excluded from target  # PII – excluded
    logger.debug('  [drop]      driver_phone')

    # DROP: internal_ref_no — excluded from target  # Internal – excluded
    logger.debug('  [drop]      internal_ref_no')

    # CONSTANT: (no src) -> load_ts
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # DERIVED: (no src) -> vehicle_vin  # VIN on shipment
    logger.debug('  [derived  ] %s -> vehicle_vin', '')
    df = df.withColumn('vehicle_vin', F.col('veh_vin_number'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> vehicle_model  # Vehicle model on shipment
    logger.debug('  [derived  ] %s -> vehicle_model', '')
    df = df.withColumn('vehicle_model', F.col('veh_model_nm'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> engine_type  # Engine type
    logger.debug('  [derived  ] %s -> engine_type', '')
    df = df.withColumn('engine_type', F.col('veh_engine_type_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # Reorder to target schema
    _exp  = ['shipment_key', 'production_order_key', 'sales_order_key', 'vehicle_key', 'origin_plant', 'destination_dealer_key', 'carrier_name', 'shipment_date', 'estimated_arrival', 'actual_arrival', 'transport_mode', 'tracking_number', 'vehicle_count', 'freight_cost', 'insurance_cost', 'total_cost', 'shipment_status', 'delay_reason', 'distance_km', 'is_delayed_flag', 'cost_per_vehicle', 'created_at', 'load_ts', 'vehicle_vin', 'vehicle_model', 'engine_type']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['shipment_key', 'vehicle_key', 'origin_plant', 'destination_dealer_key', 'carrier_name', 'shipment_date', 'transport_mode', 'vehicle_count', 'freight_cost', 'insurance_cost', 'total_cost', 'shipment_status', 'created_at', 'load_ts']
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
        df = df.withColumn('shipment_date', F.when(F.col('shipment_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('shipment_date')))
        logger.debug('  [sf-coerce] shipment_date: date -> null zero-dates')
        df = df.withColumn('estimated_arrival', F.when(F.col('estimated_arrival').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('estimated_arrival')))
        logger.debug('  [sf-coerce] estimated_arrival: date -> null zero-dates')
        df = df.withColumn('actual_arrival', F.when(F.col('actual_arrival').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('actual_arrival')))
        logger.debug('  [sf-coerce] actual_arrival: date -> null zero-dates')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('created_at').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('load_ts', F.when(F.col('load_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('load_ts').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] load_ts: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
    logger.info('  Output cols : %s', df.columns)
    logger.info('END TRANSFORM | dialect=%s', dialect)
    logger.info('=' * 70)
    return df
