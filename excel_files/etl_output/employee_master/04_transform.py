"""
04_transform.py  —  employee_master  ->  dim_employee
Generated : 2026-04-19 00:43

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
        Transformed frame shaped to dim_employee target schema,
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
    Apply all column-level transforms for employee_master.

    The input DataFrame must contain every source field referenced in
    the mapping spec. Extra columns in the input are silently ignored.
    Input values are always assumed to be MySQL-dialect.
    Pass dialect='snowflake' to coerce values for Snowflake ingestion.

    Join columns expected in input DataFrame:
        [mgr] employee_master: first_nm, last_nm
    """
    if dialect not in ('mysql', 'snowflake'):
        raise ValueError(f"dialect must be 'mysql' or 'snowflake', got {dialect!r}")
    logger.info("=" * 70)
    logger.info('START TRANSFORM | employee_master -> dim_employee | dialect=%s', dialect)
    logger.info('  Input  cols : %s', df.columns)

    # DIRECT: emp_id -> employee_key
    logger.debug('  [direct   ] %s -> employee_key', 'emp_id')
    df = df.withColumn('employee_key', F.col('emp_id'))

    # RENAME: emp_code -> employee_code
    logger.debug('  [rename   ] %s -> employee_code', 'emp_code')
    df = df.withColumn('employee_code', F.col('emp_code'))

    # RENAME: first_nm -> first_name
    logger.debug('  [rename   ] %s -> first_name', 'first_nm')
    df = df.withColumn('first_name', F.col('first_nm'))

    # RENAME: last_nm -> last_name
    logger.debug('  [rename   ] %s -> last_name', 'last_nm')
    df = df.withColumn('last_name', F.col('last_nm'))

    # DERIVED: first_nm -> full_name  # Concatenate first + last
    logger.debug('  [derived  ] %s -> full_name', 'first_nm')
    df = df.withColumn('full_name', F.expr("first_nm || ' ' || last_nm"))

    # RENAME: dob_dt -> date_of_birth
    logger.debug('  [rename   ] %s -> date_of_birth', 'dob_dt')
    df = df.withColumn('date_of_birth', F.col('dob_dt'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: gender_cd -> gender
    logger.debug('  [rename   ] %s -> gender', 'gender_cd')
    df = df.withColumn('gender', F.col('gender_cd'))
    # NULL values remain as NULL (not replaced with empty string)

    # RENAME: join_dt -> joining_date
    logger.debug('  [rename   ] %s -> joining_date', 'join_dt')
    df = df.withColumn('joining_date', F.col('join_dt'))

    # RENAME: dept_nm -> department
    logger.debug('  [rename   ] %s -> department', 'dept_nm')
    df = df.withColumn('department', F.col('dept_nm'))

    # RENAME: role_nm -> job_role
    logger.debug('  [rename   ] %s -> job_role', 'role_nm')
    df = df.withColumn('job_role', F.col('role_nm'))

    # RENAME: grade_cd -> grade
    logger.debug('  [rename   ] %s -> grade', 'grade_cd')
    df = df.withColumn('grade', F.col('grade_cd'))

    # RENAME: plant_cd -> plant_code
    logger.debug('  [rename   ] %s -> plant_code', 'plant_cd')
    df = df.withColumn('plant_code', F.col('plant_cd'))

    # RENAME: shift_cd -> shift_name
    logger.debug('  [rename   ] %s -> shift_name', 'shift_cd')
    df = df.withColumn('shift_name', F.col('shift_cd'))

    # RENAME: basic_salary_amt -> basic_salary
    logger.debug('  [rename   ] %s -> basic_salary', 'basic_salary_amt')
    df = df.withColumn('basic_salary', F.col('basic_salary_amt'))

    # RENAME: hra_amt -> hra
    logger.debug('  [rename   ] %s -> hra', 'hra_amt')
    df = df.withColumn('hra', F.col('hra_amt'))

    # DERIVED: basic_salary_amt -> gross_salary  # Basic + HRA
    logger.debug('  [derived  ] %s -> gross_salary', 'basic_salary_amt')
    df = df.withColumn('gross_salary', F.expr("basic_salary_amt + hra_amt"))

    # RENAME: pf_pct -> pf_percent
    logger.debug('  [rename   ] %s -> pf_percent', 'pf_pct')
    df = df.withColumn('pf_percent', F.col('pf_pct'))

    # RENAME: status_cd -> employee_status
    logger.debug('  [rename   ] %s -> employee_status', 'status_cd')
    df = df.withColumn('employee_status', F.col('status_cd'))

    # RENAME: mgr_emp_id -> manager_key  # Self-ref FK
    logger.debug('  [rename   ] %s -> manager_key', 'mgr_emp_id')
    df = df.withColumn('manager_key', F.col('mgr_emp_id'))
    # NULL values remain as NULL (not replaced with empty string)

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

    # DROP: pan_no — excluded from target  # PII – excluded
    logger.debug('  [drop]      pan_no')

    # DROP: aadhaar_no — excluded from target  # PII – excluded
    logger.debug('  [drop]      aadhaar_no')

    # DROP: phone_no — excluded from target  # PII – excluded
    logger.debug('  [drop]      phone_no')

    # DROP: remarks — excluded from target  # HR notes – excluded
    logger.debug('  [drop]      remarks')

    # CONSTANT: (no src) -> load_ts
    logger.debug('  [constant ] %s -> load_ts', '')
    df = df.withColumn('load_ts', F.current_timestamp())

    # DERIVED: (no src) -> manager_name  # Manager full name (self-join)
    logger.debug('  [derived  ] %s -> manager_name', '')
    df = df.withColumn('manager_name', F.expr("mgr_first_nm || ' ' || mgr_last_nm"))
    # NULL values remain as NULL (not replaced with empty string)

    # Reorder to target schema
    _exp  = ['employee_key', 'employee_code', 'first_name', 'last_name', 'full_name', 'date_of_birth', 'gender', 'joining_date', 'department', 'job_role', 'grade', 'plant_code', 'shift_name', 'basic_salary', 'hra', 'gross_salary', 'pf_percent', 'employee_status', 'manager_key', 'created_at', 'updated_at', 'created_by', 'load_ts', 'manager_name']
    _pres = [c for c in _exp if c in df.columns]
    _miss = [c for c in _exp if c not in df.columns]
    if _miss:
        logger.warning('  Missing target cols: %s', _miss)
    df = df.select(*_pres)

    # ── Batch null validation (single Spark action) ──────────────────
    _nn_cols = ['employee_key', 'employee_code', 'first_name', 'last_name', 'full_name', 'joining_date', 'department', 'job_role', 'grade', 'plant_code', 'shift_name', 'basic_salary', 'hra', 'gross_salary', 'pf_percent', 'employee_status', 'created_at', 'updated_at', 'load_ts']
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
        df = df.withColumn('date_of_birth', F.when(F.col('date_of_birth').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('date_of_birth')))
        logger.debug('  [sf-coerce] date_of_birth: date -> null zero-dates')
        df = df.withColumn('gender', F.rtrim(F.col('gender')))
        logger.debug('  [sf-coerce] gender: char -> rtrim whitespace')
        df = df.withColumn('joining_date', F.when(F.col('joining_date').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('joining_date')))
        logger.debug('  [sf-coerce] joining_date: date -> null zero-dates')
        df = df.withColumn('grade', F.rtrim(F.col('grade')))
        logger.debug('  [sf-coerce] grade: char -> rtrim whitespace')
        df = df.withColumn('created_at', F.when(F.col('created_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('created_at').cast(TimestampType())))
        logger.debug('  [sf-coerce] created_at: datetime/timestamp -> TimestampType')
        df = df.withColumn('updated_at', F.when(F.col('updated_at').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('updated_at').cast(TimestampType())))
        logger.debug('  [sf-coerce] updated_at: datetime/timestamp -> TimestampType')
        df = df.withColumn('load_ts', F.when(F.col('load_ts').cast(StringType()).startswith('0000-00-00'), F.lit(None)).otherwise(F.col('load_ts').cast(TimestampType())))
        logger.debug('  [sf-coerce] load_ts: datetime/timestamp -> TimestampType')
    logger.info('  Output cols : %s', df.columns)
    logger.info('END TRANSFORM | dialect=%s', dialect)
    logger.info('=' * 70)
    return df
