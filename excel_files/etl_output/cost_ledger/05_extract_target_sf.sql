-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: fact_cost
-- ============================================================
-- Dialect   : Snowflake
-- Generated : 2026-04-19 01:14
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
--   pk_range: WHERE ledger_key >= {LOWER} AND/OR ledger_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE ledger_key IN ({PK_SET})
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
    ledger_key,
    production_order_key,
    vehicle_key,
    plant_code,
    cost_type,
    cost_category,
    gl_account,
    cost_center,
    posting_date,
    fiscal_year,
    fiscal_period,
    currency_code,
    amount_local,
    amount_usd,
    fx_rate,
    quantity,
    unit_of_measure,
    part_key,
    supplier_key,
    is_approved,
    approved_by,
    fiscal_yr_month,
    created_at,
    updated_at,
    journal_reference,
    load_ts,
    supplier_name,
    supplier_country
FROM fact_cost;