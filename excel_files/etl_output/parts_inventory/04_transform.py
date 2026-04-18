"""
04_transform.py  —  parts_inventory  ->  dim_parts
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
        Transformed frame shaped to dim_parts target schema,
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
    Apply all column-level transforms for parts_inventory.

    The input DataFrame must contain every source field referenced in
    the mapping spec. Extra columns in the input are silently ignored.
    Input values are always assumed to be MySQL-dialect.
    Pass dialect='snowflake' to coerce values for Snowflake ingestion.

    Join columns expected in input DataFrame:
        [sup] supplier_master: supplier_nm, country_cd, contact_person
    """
    if dialect not in ('mysql', 'snowflake'):
        raise ValueError(f"dialect must be 'mysql' or 'snowflake', got {dialect!r}")
    logger.info("=" * 70)
    logger.info('START TRANSFORM | parts_inventory -> dim_parts | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: part_id -> part_key
    logger.debug('  [direct   ] %s -> part_key', 'part_id')
    df = df.withColumn('part_key', F.col('part_id'))

    # RENAME: part_no -> part_number
    logger.debug('  [rename   ] %s -> part_number', 'part_no')
    df = df.withColumn('part_number', F.col('part_no'))

    # RENAME: part_nm -> part_name
    logger.debug('  [rename   ] %s -> part_name', 'part_nm')
    df = df.withColumn('part_name', F.col('part_nm'))

    # RENAME: part_category -> category_name
    logger.debug('  [rename   ] %s -> category_name', 'part_category')
    df = df.withColumn('category_name', F.col('part_category'))

    # RENAME: supplier_id -> supplier_key  # FK to dim_supplier
    logger.debug('  [rename   ] %s -> supplier_key', 'supplier_id')
    df = df.withColumn('supplier_key', F.col('supplier_id'))

    # RENAME: unit_cost_amt -> unit_cost
    logger.debug('  [rename   ] %s -> unit_cost', 'unit_cost_amt')
    df = df.withColumn('unit_cost', F.col('unit_cost_amt'))

    # RENAME: currency_cd -> currency_code
    logger.debug('  [rename   ] %s -> currency_code', 'currency_cd')
    df = df.withColumn('currency_code', F.col('currency_cd'))

    # RENAME: qty_on_hand -> stock_quantity
    logger.debug('  [rename   ] %s -> stock_quantity', 'qty_on_hand')
    df = df.withColumn('stock_quantity', F.col('qty_on_hand'))
    df = df.fillna({'stock_quantity': 0})

    # RENAME: reorder_point -> reorder_threshold
    logger.debug('  [rename   ] %s -> reorder_threshold', 'reorder_point')
    df = df.withColumn('reorder_threshold', F.col('reorder_point'))

    # RENAME: reorder_qty -> reorder_quantity
    logger.debug('  [rename   ] %s -> reorder_quantity', 'reorder_qty')
    df = df.withColumn('reorder_quantity', F.col('reorder_qty'))

    # DIRECT: lead_time_days -> lead_time_days
    logger.debug('  [direct   ] %s -> lead_time_days', 'lead_time_days')
    df = df.withColumn('lead_time_days', F.col('lead_time_days'))

    # RENAME: storage_loc_cd -> storage_location
    logger.debug('  [rename   ] %s -> storage_location', 'storage_loc_cd')
    df = df.withColumn('storage_location', F.col('storage_loc_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: weight_gm -> weight_grams
    logger.debug('  [rename   ] %s -> weight_grams', 'weight_gm')
    df = df.withColumn('weight_grams', F.col('weight_gm'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: is_critical_flag -> is_critical  # Y/N → 1/0
    logger.debug('  [derived  ] %s -> is_critical', 'is_critical_flag')
    df = df.withColumn('is_critical', F.when(F.col('is_critical_flag') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'is_critical': 0})

    # RENAME: last_receipt_dt -> last_receipt_date
    logger.debug('  [rename   ] %s -> last_receipt_date', 'last_receipt_dt')
    df = df.withColumn('last_receipt_date', F.col('last_receipt_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: expiry_dt -> expiry_date
    logger.debug('  [rename   ] %s -> expiry_date', 'expiry_dt')
    df = df.withColumn('expiry_date', F.col('expiry_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: status_cd -> part_status
    logger.debug('  [rename   ] %s -> part_status', 'status_cd')
    df = df.withColumn('part_status', F.col('status_cd'))

    # RENAME: country_of_origin -> origin_country
    logger.debug('  [rename   ] %s -> origin_country', 'country_of_origin')
    df = df.withColumn('origin_country', F.col('country_of_origin'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: tariff_code -> tariff_code
    logger.debug('  [direct   ] %s -> tariff_code', 'tariff_code')
    df = df.withColumn('tariff_code', F.col('tariff_code'))
    # NULL values remain as NULL (not replaced with empty string)

    # DIRECT: hsn_code -> hsn_code
    logger.debug('  [direct   ] %s -> hsn_code', 'hsn_code')
    df = df.withColumn('hsn_code', F.col('hsn_code'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: uom_cd -> unit_of_measure
    logger.debug('  [rename   ] %s -> unit_of_measure', 'uom_cd')
    df = df.withColumn('unit_of_measure', F.col('uom_cd'))

    # DERIVED: qty_on_hand -> stock_value_amt  # Stock qty × unit cost
    logger.debug('  [derived  ] %s -> stock_value_amt', 'qty_on_hand')
    df = df.withColumn('stock_value_amt', F.expr("qty_on_hand * unit_cost_amt"))
    df = df.fillna({'stock_value_amt': 0})

    # DERIVED: qty_on_hand -> below_reorder_flag  # 1 if stock below threshold
    logger.debug('  [derived  ] %s -> below_reorder_flag', 'qty_on_hand')
    df = df.withColumn('below_reorder_flag', F.when(F.col('qty_on_hand') < F.col('reorder_point'), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'below_reorder_flag': 0})

    # DIRECT: created_at -> created_at
    logger.debug('  [direct   ] %s -> created_at', 'created_at')
    df = df.withColumn('created_at', F.col('created_at'))

    # DROP: internal_ref_cd — excluded from target  # Internal reference – excluded
    logger.debug('  [drop]      internal_ref_cd')

    # DROP: remarks — excluded from target  # Free text – excluded
    logger.debug('  [drop]      remarks')

    # CONSTANT: (no src) -> load_ts
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # DERIVED: (no src) -> supplier_name  # Supplier name from supplier_master
    logger.debug('  [derived  ] %s -> supplier_name', '')
    df = df.withColumn('supplier_name', F.col('sup_supplier_nm'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> supplier_country  # Supplier country uppercased
    logger.debug('  [derived  ] %s -> supplier_country', '')
    df = df.withColumn('supplier_country', F.upper(F.col('sup_country_cd')))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> supplier_contact  # Supplier contact person
    logger.debug('  [derived  ] %s -> supplier_contact', '')
    df = df.withColumn('supplier_contact', F.col('sup_contact_person'))
    # NULL values remain as NULL (not replaced with empty string)

    # Reorder to target schema
    _exp  = ['part_key', 'part_number', 'part_name', 'category_name', 'supplier_key', 'unit_cost', 'currency_code', 'stock_quantity', 'reorder_threshold', 'reorder_quantity', 'lead_time_days', 'storage_location', 'weight_grams', 'is_critical', 'last_receipt_date', 'expiry_date', 'part_status', 'origin_country', 'tariff_code', 'hsn_code', 'unit_of_measure', 'stock_value_amt', 'below_reorder_flag', 'created_at', 'load_ts', 'supplier_name', 'supplier_country', 'supplier_contact']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['part_key', 'part_number', 'part_name', 'category_name', 'supplier_key', 'unit_cost', 'currency_code', 'reorder_threshold', 'reorder_quantity', 'lead_time_days', 'part_status', 'unit_of_measure', 'created_at', 'load_ts']
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
        df = df.withColumn('weight_grams', F.col('weight_grams').cast(DoubleType()))
        logger.debug('  [sf-coerce] weight_grams: decimal/numeric -> DoubleType')
        df = df.withColumn('last_receipt_date', F.when(F.col('last_receipt_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('last_receipt_date')))
        logger.debug('  [sf-coerce] last_receipt_date: date -> null zero-dates')
        df = df.withColumn('expiry_date', F.when(F.col('expiry_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('expiry_date')))
        logger.debug('  [sf-coerce] expiry_date: date -> null zero-dates')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('created_at').cast(TimestampType())))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType (no tz shift, MySQL stores local time)')
        df = df.withColumn('load_ts', F.when(F.col('load_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('load_ts').cast(TimestampType())))
        logger.debug('  [sf-coerce] load_ts: datetime/timestamp -> TimestampType (no tz shift, MySQL stores local time)')
    logger.info('  Output cols : %s', df.columns)
    logger.info('END TRANSFORM | dialect=%s', dialect)
    logger.info('=' * 70)
    return df
