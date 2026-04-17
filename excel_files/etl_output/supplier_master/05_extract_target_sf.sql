-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: dim_supplier
-- ============================================================
-- Dialect   : Snowflake
-- Generated : 2026-04-17 15:17
--
-- Simple SELECT of all target columns for comparison against
-- the transformed source data (04_transform.py output).
--
-- [[FILTER_PLACEHOLDER]]
-- The following WHERE clause is injected at runtime by custom_execution.py.
-- Do NOT edit this block manually — it is overwritten on each run.
--
-- PK filter modes (PK_FILTER_MODE in custom_execution.py):
--   full    : no PK WHERE clause
--   pk_range: WHERE supplier_key >= {LOWER} AND/OR supplier_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE supplier_key IN ({PK_SET})
--
-- Date watermark modes (DATE_WATERMARK_MODE in custom_execution.py):
--   full    : no date WHERE clause
--   range   : WHERE {{DATE_FROM_COL}} >= '{{DATE_FROM}}'
--             AND/OR {{DATE_TO_COL}} <= '{{DATE_TO}}'
--             (either bound may be None → single-sided limit)
--
-- [[/FILTER_PLACEHOLDER]]
-- Date cols available in this table: created_at, updated_at

SELECT
    supplier_key,
    supplier_name,
    supplier_code,
    contact_name,
    email,
    phone,
    country,
    city,
    address,
    payment_terms,
    credit_limit,
    currency_code,
    supplier_rating,
    is_approved,
    approval_date,
    contract_start,
    contract_end,
    supplier_status,
    tier_level,
    created_at,
    updated_at,
    created_by,
    load_ts,
    account_manager_name,
    account_manager_code
FROM dim_supplier;