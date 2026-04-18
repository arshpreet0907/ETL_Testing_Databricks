"""
04_transform.py  —  cost_ledger  ->  fact_cost
Generated : 2026-04-19 03:34

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
        Transformed frame shaped to fact_cost target schema,
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
    Apply all column-level transforms for cost_ledger.

    The input DataFrame must contain every source field referenced in
    the mapping spec. Extra columns in the input are silently ignored.
    Input values are always assumed to be MySQL-dialect.
    Pass dialect='snowflake' to coerce values for Snowflake ingestion.

    Join columns expected in input DataFrame:
        [sup] supplier_master: supplier_nm, country_cd
    """
    if dialect not in ('mysql', 'snowflake'):
        raise ValueError(f"dialect must be 'mysql' or 'snowflake', got {dialect!r}")
    logger.info("=" * 70)
    logger.info('START TRANSFORM | cost_ledger -> fact_cost | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: ledger_id -> ledger_key
    logger.debug('  [direct   ] %s -> ledger_key', 'ledger_id')
    df = df.withColumn('ledger_key', F.col('ledger_id'))

    # RENAME: prod_order_id -> production_order_key  # FK to fact_production
    logger.debug('  [rename   ] %s -> production_order_key', 'prod_order_id')
    df = df.withColumn('production_order_key', F.col('prod_order_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: vehicle_id -> vehicle_key  # FK to dim_vehicle
    logger.debug('  [rename   ] %s -> vehicle_key', 'vehicle_id')
    df = df.withColumn('vehicle_key', F.col('vehicle_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: plant_cd -> plant_code
    logger.debug('  [rename   ] %s -> plant_code', 'plant_cd')
    df = df.withColumn('plant_code', F.col('plant_cd'))

    # RENAME: cost_type_cd -> cost_type  # Material/Labour/Overhead etc.
    logger.debug('  [rename   ] %s -> cost_type', 'cost_type_cd')
    df = df.withColumn('cost_type', F.col('cost_type_cd'))

    # RENAME: cost_category_cd -> cost_category  # Direct/Indirect/Fixed/Variable
    logger.debug('  [rename   ] %s -> cost_category', 'cost_category_cd')
    df = df.withColumn('cost_category', F.col('cost_category_cd'))

    # RENAME: gl_account_no -> gl_account
    logger.debug('  [rename   ] %s -> gl_account', 'gl_account_no')
    df = df.withColumn('gl_account', F.col('gl_account_no'))

    # RENAME: cost_center_cd -> cost_center
    logger.debug('  [rename   ] %s -> cost_center', 'cost_center_cd')
    df = df.withColumn('cost_center', F.col('cost_center_cd'))

    # RENAME: posting_dt -> posting_date
    logger.debug('  [rename   ] %s -> posting_date', 'posting_dt')
    df = df.withColumn('posting_date', F.col('posting_dt'))

    # RENAME: fiscal_yr -> fiscal_year
    logger.debug('  [rename   ] %s -> fiscal_year', 'fiscal_yr')
    df = df.withColumn('fiscal_year', F.col('fiscal_yr'))

    # DIRECT: fiscal_period -> fiscal_period  # 1–12
    logger.debug('  [direct   ] %s -> fiscal_period', 'fiscal_period')
    df = df.withColumn('fiscal_period', F.col('fiscal_period'))

    # RENAME: currency_cd -> currency_code
    logger.debug('  [rename   ] %s -> currency_code', 'currency_cd')
    df = df.withColumn('currency_code', F.col('currency_cd'))

    # RENAME: amount_lc -> amount_local
    logger.debug('  [rename   ] %s -> amount_local', 'amount_lc')
    df = df.withColumn('amount_local', F.col('amount_lc'))

    # DIRECT: amount_usd -> amount_usd
    logger.debug('  [direct   ] %s -> amount_usd', 'amount_usd')
    df = df.withColumn('amount_usd', F.col('amount_usd'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: exchange_rate -> fx_rate  # Default 1.0 if null
    logger.debug('  [rename   ] %s -> fx_rate', 'exchange_rate')
    df = df.withColumn('fx_rate', F.col('exchange_rate'))

    # RENAME: qty_consumed -> quantity
    logger.debug('  [rename   ] %s -> quantity', 'qty_consumed')
    df = df.withColumn('quantity', F.col('qty_consumed'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: uom_cd -> unit_of_measure
    logger.debug('  [rename   ] %s -> unit_of_measure', 'uom_cd')
    df = df.withColumn('unit_of_measure', F.col('uom_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: part_id -> part_key  # FK to dim_parts
    logger.debug('  [rename   ] %s -> part_key', 'part_id')
    df = df.withColumn('part_key', F.col('part_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: supplier_id -> supplier_key  # FK to dim_supplier
    logger.debug('  [rename   ] %s -> supplier_key', 'supplier_id')
    df = df.withColumn('supplier_key', F.col('supplier_id'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: approved_flag -> is_approved  # Y/N → 1/0
    logger.debug('  [derived  ] %s -> is_approved', 'approved_flag')
    df = df.withColumn('is_approved', F.when(F.col('approved_flag') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'is_approved': 0})

    # DIRECT: approved_by -> approved_by
    logger.debug('  [direct   ] %s -> approved_by', 'approved_by')
    df = df.withColumn('approved_by', F.col('approved_by'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: fiscal_yr -> fiscal_yr_month  # e.g. 2024-03
    logger.debug('  [derived  ] %s -> fiscal_yr_month', 'fiscal_yr')
    df = df.withColumn('fiscal_yr_month', F.concat(F.col('fiscal_yr').cast('string'), F.lit("-"), F.lpad(F.col('fiscal_period').cast('string'), 2, '0')))

    # DIRECT: created_at -> created_at
    logger.debug('  [direct   ] %s -> created_at', 'created_at')
    df = df.withColumn('created_at', F.col('created_at'))

    # DIRECT: updated_at -> updated_at
    logger.debug('  [direct   ] %s -> updated_at', 'updated_at')
    df = df.withColumn('updated_at', F.col('updated_at'))

    # RENAME: journal_ref_no -> journal_reference
    logger.debug('  [rename   ] %s -> journal_reference', 'journal_ref_no')
    df = df.withColumn('journal_reference', F.col('journal_ref_no'))
    # NULL values remain as NULL (not replaced with empty string)

    # DROP: internal_notes — excluded from target  # Finance notes – excluded
    logger.debug('  [drop]      internal_notes')

    # CONSTANT: (no src) -> load_ts
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # DERIVED: (no src) -> supplier_name  # Supplier name on cost entry
    logger.debug('  [derived  ] %s -> supplier_name', '')
    df = df.withColumn('supplier_name', F.col('sup_supplier_nm'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> supplier_country  # Supplier country
    logger.debug('  [derived  ] %s -> supplier_country', '')
    df = df.withColumn('supplier_country', F.upper(F.col('sup_country_cd')))
    # NULL values remain as NULL (not replaced with empty string)

    # Reorder to target schema
    _exp  = ['ledger_key', 'production_order_key', 'vehicle_key', 'plant_code', 'cost_type', 'cost_category', 'gl_account', 'cost_center', 'posting_date', 'fiscal_year', 'fiscal_period', 'currency_code', 'amount_local', 'amount_usd', 'fx_rate', 'quantity', 'unit_of_measure', 'part_key', 'supplier_key', 'is_approved', 'approved_by', 'fiscal_yr_month', 'created_at', 'updated_at', 'journal_reference', 'load_ts', 'supplier_name', 'supplier_country']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['ledger_key', 'plant_code', 'cost_type', 'cost_category', 'gl_account', 'cost_center', 'posting_date', 'fiscal_year', 'fiscal_period', 'currency_code', 'amount_local', 'fiscal_yr_month', 'created_at', 'updated_at', 'load_ts']
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
        df = df.withColumn('posting_date', F.when(F.col('posting_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('posting_date')))
        logger.debug('  [sf-coerce] posting_date: date -> null zero-dates')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('created_at').cast(TimestampType())))
        # Source timestamps are IST (from MySQL); Snowflake stores them converted to IST representation
        df = df.withColumn('created_at', F.from_utc_timestamp(F.col('created_at'), 'Asia/Kolkata'))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType (IST adjusted)')
        df = df.withColumn('updated_at', F.when(F.col('updated_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('updated_at').cast(TimestampType())))
        # Source timestamps are IST (from MySQL); Snowflake stores them converted to IST representation
        df = df.withColumn('updated_at', F.from_utc_timestamp(F.col('updated_at'), 'Asia/Kolkata'))
        logger.debug('  [sf-coerce] updated_at: datetime/timestamp -> TimestampType (IST adjusted)')
        df = df.withColumn('load_ts', F.when(F.col('load_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('load_ts').cast(TimestampType())))
        # Source timestamps are IST (from MySQL); Snowflake stores them converted to IST representation
        df = df.withColumn('load_ts', F.from_utc_timestamp(F.col('load_ts'), 'Asia/Kolkata'))
        logger.debug('  [sf-coerce] load_ts: datetime/timestamp -> TimestampType (IST adjusted)')
    logger.info('  Output cols : %s', df.columns)
    logger.info('END TRANSFORM | dialect=%s', dialect)
    logger.info('=' * 70)
    return df
