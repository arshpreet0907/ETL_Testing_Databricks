"""
04_transform.py  —  supplier_master  ->  dim_supplier
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
        Transformed frame shaped to dim_supplier target schema,
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
    Apply all column-level transforms for supplier_master.

    The input DataFrame must contain every source field referenced in
    the mapping spec. Extra columns in the input are silently ignored.
    Input values are always assumed to be MySQL-dialect.
    Pass dialect='snowflake' to coerce values for Snowflake ingestion.

    Join columns expected in input DataFrame:
        [mgr] employee_master: first_nm, last_nm, emp_code
    """
    if dialect not in ('mysql', 'snowflake'):
        raise ValueError(f"dialect must be 'mysql' or 'snowflake', got {dialect!r}")
    logger.info("=" * 70)
    logger.info('START TRANSFORM | supplier_master -> dim_supplier | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: supplier_id -> supplier_key
    logger.debug('  [direct   ] %s -> supplier_key', 'supplier_id')
    df = df.withColumn('supplier_key', F.col('supplier_id'))

    # RENAME: supplier_nm -> supplier_name
    logger.debug('  [rename   ] %s -> supplier_name', 'supplier_nm')
    df = df.withColumn('supplier_name', F.col('supplier_nm'))

    # DIRECT: supplier_code -> supplier_code
    logger.debug('  [direct   ] %s -> supplier_code', 'supplier_code')
    df = df.withColumn('supplier_code', F.col('supplier_code'))

    # RENAME: contact_person -> contact_name
    logger.debug('  [rename   ] %s -> contact_name', 'contact_person')
    df = df.withColumn('contact_name', F.col('contact_person'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: email_addr -> email
    logger.debug('  [rename   ] %s -> email', 'email_addr')
    df = df.withColumn('email', F.col('email_addr'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: phone_no -> phone
    logger.debug('  [rename   ] %s -> phone', 'phone_no')
    df = df.withColumn('phone', F.col('phone_no'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: country_cd -> country
    logger.debug('  [rename   ] %s -> country', 'country_cd')
    df = df.withColumn('country', F.col('country_cd'))

    # RENAME: city_nm -> city
    logger.debug('  [rename   ] %s -> city', 'city_nm')
    df = df.withColumn('city', F.col('city_nm'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: address_txt -> address
    logger.debug('  [rename   ] %s -> address', 'address_txt')
    df = df.withColumn('address', F.col('address_txt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: payment_terms_cd -> payment_terms
    logger.debug('  [rename   ] %s -> payment_terms', 'payment_terms_cd')
    df = df.withColumn('payment_terms', F.col('payment_terms_cd'))

    # RENAME: credit_limit_amt -> credit_limit
    logger.debug('  [rename   ] %s -> credit_limit', 'credit_limit_amt')
    df = df.withColumn('credit_limit', F.col('credit_limit_amt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: currency_cd -> currency_code
    logger.debug('  [rename   ] %s -> currency_code', 'currency_cd')
    df = df.withColumn('currency_code', F.col('currency_cd'))

    # RENAME: rating_score -> supplier_rating
    logger.debug('  [rename   ] %s -> supplier_rating', 'rating_score')
    df = df.withColumn('supplier_rating', F.col('rating_score'))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: is_approved_flag -> is_approved  # Y/N → 1/0
    logger.debug('  [derived  ] %s -> is_approved', 'is_approved_flag')
    df = df.withColumn('is_approved', F.when(F.col('is_approved_flag') == F.lit("Y"), F.lit(1)).otherwise(F.lit(0)))
    df = df.fillna({'is_approved': 0})

    # RENAME: approval_dt -> approval_date
    logger.debug('  [rename   ] %s -> approval_date', 'approval_dt')
    df = df.withColumn('approval_date', F.col('approval_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: contract_start_dt -> contract_start
    logger.debug('  [rename   ] %s -> contract_start', 'contract_start_dt')
    df = df.withColumn('contract_start', F.col('contract_start_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: contract_end_dt -> contract_end
    logger.debug('  [rename   ] %s -> contract_end', 'contract_end_dt')
    df = df.withColumn('contract_end', F.col('contract_end_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: status_cd -> supplier_status
    logger.debug('  [rename   ] %s -> supplier_status', 'status_cd')
    df = df.withColumn('supplier_status', F.col('status_cd'))

    # DIRECT: tier_level -> tier_level  # 1=Tier1, 2=Tier2, 3=Tier3
    logger.debug('  [direct   ] %s -> tier_level', 'tier_level')
    df = df.withColumn('tier_level', F.col('tier_level'))

    # DROP: tax_id — excluded from target  # PII – excluded from target
    logger.debug('  [drop]      tax_id')

    # DROP: bank_account_no — excluded from target  # PII – excluded from target
    logger.debug('  [drop]      bank_account_no')

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

    # DROP: internal_notes — excluded from target  # Sensitive – excluded
    logger.debug('  [drop]      internal_notes')

    # DROP: account_mgr_emp_id — excluded from target  # Used for join only - not loaded to target
    logger.debug('  [drop]      account_mgr_emp_id')

    # CONSTANT: (no src) -> load_ts
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # DERIVED: (no src) -> account_manager_name  # Account manager full name
    logger.debug('  [derived  ] %s -> account_manager_name', '')
    df = df.withColumn('account_manager_name', F.expr("mgr_first_nm || ' ' || mgr_last_nm"))
    # NULL values remain as NULL (not replaced with empty string)

    # DERIVED: (no src) -> account_manager_code  # Account manager employee code
    logger.debug('  [derived  ] %s -> account_manager_code', '')
    df = df.withColumn('account_manager_code', F.col('mgr_emp_code'))
    # NULL values remain as NULL (not replaced with empty string)

    # Reorder to target schema
    _exp  = ['supplier_key', 'supplier_name', 'supplier_code', 'contact_name', 'email', 'phone', 'country', 'city', 'address', 'payment_terms', 'credit_limit', 'currency_code', 'supplier_rating', 'is_approved', 'approval_date', 'contract_start', 'contract_end', 'supplier_status', 'tier_level', 'created_at', 'updated_at', 'created_by', 'load_ts', 'account_manager_name', 'account_manager_code']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['supplier_key', 'supplier_name', 'supplier_code', 'country', 'payment_terms', 'currency_code', 'supplier_status', 'tier_level', 'created_at', 'updated_at', 'load_ts']
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
        df = df.withColumn('supplier_rating', F.col('supplier_rating').cast(DoubleType()))
        logger.debug('  [sf-coerce] supplier_rating: decimal/numeric -> DoubleType')
        df = df.withColumn('approval_date', F.when(F.col('approval_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('approval_date')))
        logger.debug('  [sf-coerce] approval_date: date -> null zero-dates')
        df = df.withColumn('contract_start', F.when(F.col('contract_start').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('contract_start')))
        logger.debug('  [sf-coerce] contract_start: date -> null zero-dates')
        df = df.withColumn('contract_end', F.when(F.col('contract_end').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('contract_end')))
        logger.debug('  [sf-coerce] contract_end: date -> null zero-dates')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('created_at').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('updated_at', F.when(F.col('updated_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('updated_at').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] updated_at: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
        df = df.withColumn('load_ts', F.when(F.col('load_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.to_utc_timestamp(F.col('load_ts').cast(TimestampType()), 'Asia/Kolkata')))
        logger.debug('  [sf-coerce] load_ts: datetime/timestamp -> TimestampType (IST->UTC), null zero-datetimes')
    logger.info('  Output cols : %s', df.columns)
    logger.info('END TRANSFORM | dialect=%s', dialect)
    logger.info('=' * 70)
    return df
