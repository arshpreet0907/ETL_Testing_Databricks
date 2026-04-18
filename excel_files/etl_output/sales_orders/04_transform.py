"""
04_transform.py  —  sales_orders  ->  fact_sales
Generated : 2026-04-19 00:02

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
        Transformed frame shaped to fact_sales target schema,
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
    Apply all column-level transforms for sales_orders.

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
    logger.info('START TRANSFORM | sales_orders -> fact_sales | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: sales_order_id -> sales_order_key
    logger.debug('  [direct   ] %s -> sales_order_key', 'sales_order_id')
    df = df.withColumn('sales_order_key', F.col('sales_order_id'))

    # RENAME: vehicle_id -> vehicle_key  # FK to dim_vehicle
    logger.debug('  [rename   ] %s -> vehicle_key', 'vehicle_id')
    df = df.withColumn('vehicle_key', F.col('vehicle_id'))

    # RENAME: dealer_id -> dealer_key
    logger.debug('  [rename   ] %s -> dealer_key', 'dealer_id')
    df = df.withColumn('dealer_key', F.col('dealer_id'))

    # RENAME: customer_id -> customer_key
    logger.debug('  [rename   ] %s -> customer_key', 'customer_id')
    df = df.withColumn('customer_key', F.col('customer_id'))

    # RENAME: order_dt -> order_date
    logger.debug('  [rename   ] %s -> order_date', 'order_dt')
    df = df.withColumn('order_date', F.col('order_dt'))

    # RENAME: delivery_dt -> delivery_date
    logger.debug('  [rename   ] %s -> delivery_date', 'delivery_dt')
    df = df.withColumn('delivery_date', F.col('delivery_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: invoice_no -> invoice_number
    logger.debug('  [rename   ] %s -> invoice_number', 'invoice_no')
    df = df.withColumn('invoice_number', F.col('invoice_no'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: invoice_dt -> invoice_date
    logger.debug('  [rename   ] %s -> invoice_date', 'invoice_dt')
    df = df.withColumn('invoice_date', F.col('invoice_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: sale_price_amt -> sale_price
    logger.debug('  [rename   ] %s -> sale_price', 'sale_price_amt')
    df = df.withColumn('sale_price', F.col('sale_price_amt'))

    # RENAME: discount_pct -> discount_percent
    logger.debug('  [rename   ] %s -> discount_percent', 'discount_pct')
    df = df.withColumn('discount_percent', F.col('discount_pct'))
    df = df.fillna({'discount_percent': 0})

    # DERIVED: sale_price_amt -> discount_amt  # Absolute discount value
    logger.debug('  [derived  ] %s -> discount_amt', 'sale_price_amt')
    df = df.withColumn('discount_amt', F.expr("ROUND(sale_price_amt * discount_pct / 100, 2)"))
    df = df.fillna({'discount_amt': 0})

    # RENAME: tax_amt -> tax_amount
    logger.debug('  [rename   ] %s -> tax_amount', 'tax_amt')
    df = df.withColumn('tax_amount', F.col('tax_amt'))

    # RENAME: insurance_amt -> insurance_amount
    logger.debug('  [rename   ] %s -> insurance_amount', 'insurance_amt')
    df = df.withColumn('insurance_amount', F.col('insurance_amt'))
    df = df.fillna({'insurance_amount': 0})

    # RENAME: accessories_amt -> accessories_amount
    logger.debug('  [rename   ] %s -> accessories_amount', 'accessories_amt')
    df = df.withColumn('accessories_amount', F.col('accessories_amt'))
    df = df.fillna({'accessories_amount': 0})

    # RENAME: total_invoice_amt -> total_invoice
    logger.debug('  [rename   ] %s -> total_invoice', 'total_invoice_amt')
    df = df.withColumn('total_invoice', F.col('total_invoice_amt'))

    # RENAME: payment_mode_cd -> payment_mode
    logger.debug('  [rename   ] %s -> payment_mode', 'payment_mode_cd')
    df = df.withColumn('payment_mode', F.col('payment_mode_cd'))

    # RENAME: finance_bank_nm -> finance_bank
    logger.debug('  [rename   ] %s -> finance_bank', 'finance_bank_nm')
    df = df.withColumn('finance_bank', F.col('finance_bank_nm'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: vin_allocated -> vin_number
    logger.debug('  [rename   ] %s -> vin_number', 'vin_allocated')
    df = df.withColumn('vin_number', F.col('vin_allocated'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: region_cd -> region
    logger.debug('  [rename   ] %s -> region', 'region_cd')
    df = df.withColumn('region', F.col('region_cd'))

    # RENAME: sales_rep_emp_id -> sales_rep_key  # FK to dim_employee
    logger.debug('  [rename   ] %s -> sales_rep_key', 'sales_rep_emp_id')
    df = df.withColumn('sales_rep_key', F.col('sales_rep_emp_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: order_status_cd -> order_status
    logger.debug('  [rename   ] %s -> order_status', 'order_status_cd')
    df = df.withColumn('order_status', F.col('order_status_cd'))

    # RENAME: cancel_reason_cd -> cancellation_reason
    logger.debug('  [rename   ] %s -> cancellation_reason', 'cancel_reason_cd')
    df = df.withColumn('cancellation_reason', F.col('cancel_reason_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: source_channel_cd -> sales_channel
    logger.debug('  [rename   ] %s -> sales_channel', 'source_channel_cd')
    df = df.withColumn('sales_channel', F.col('source_channel_cd'))

    # DIRECT: created_at -> created_at
    logger.debug('  [direct   ] %s -> created_at', 'created_at')
    df = df.withColumn('created_at', F.col('created_at'))

    # DROP: internal_ref_no — excluded from target  # Internal – excluded
    logger.debug('  [drop]      internal_ref_no')

    # CONSTANT: (no src) -> load_ts
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # DERIVED: (no src) -> vehicle_vin  # VIN on this sale
    logger.debug('  [derived  ] %s -> vehicle_vin', '')
    df = df.withColumn('vehicle_vin', F.col('veh_vin_number'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> vehicle_model  # Vehicle model name
    logger.debug('  [derived  ] %s -> vehicle_model', '')
    df = df.withColumn('vehicle_model', F.col('veh_model_nm'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> vehicle_variant  # Vehicle variant
    logger.debug('  [derived  ] %s -> vehicle_variant', '')
    df = df.withColumn('vehicle_variant', F.col('veh_variant_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # Reorder to target schema
    _exp  = ['sales_order_key', 'vehicle_key', 'dealer_key', 'customer_key', 'order_date', 'delivery_date', 'invoice_number', 'invoice_date', 'sale_price', 'discount_percent', 'discount_amt', 'tax_amount', 'insurance_amount', 'accessories_amount', 'total_invoice', 'payment_mode', 'finance_bank', 'vin_number', 'region', 'sales_rep_key', 'order_status', 'cancellation_reason', 'sales_channel', 'created_at', 'load_ts', 'vehicle_vin', 'vehicle_model', 'vehicle_variant']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['sales_order_key', 'vehicle_key', 'dealer_key', 'customer_key', 'order_date', 'sale_price', 'tax_amount', 'total_invoice', 'payment_mode', 'region', 'order_status', 'sales_channel', 'created_at', 'load_ts']
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
        df = df.withColumn('delivery_date', F.when(F.col('delivery_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('delivery_date')))
        logger.debug('  [sf-coerce] delivery_date: date -> null zero-dates')
        df = df.withColumn('invoice_date', F.when(F.col('invoice_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('invoice_date')))
        logger.debug('  [sf-coerce] invoice_date: date -> null zero-dates')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('created_at').cast(TimestampType())))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType (no tz shift, MySQL stores local time)')
        df = df.withColumn('load_ts', F.when(F.col('load_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('load_ts').cast(TimestampType())))
        logger.debug('  [sf-coerce] load_ts: datetime/timestamp -> TimestampType (no tz shift, MySQL stores local time)')
    logger.info('  Output cols : %s', df.columns)
    logger.info('END TRANSFORM | dialect=%s', dialect)
    logger.info('=' * 70)
    return df
