-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: dim_parts
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
--   pk_range: WHERE part_key >= {LOWER} AND/OR part_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE part_key IN ({PK_SET})
--
-- Date watermark modes (DATE_WATERMARK_MODE in custom_execution.py):
--   full    : no date WHERE clause
--   range   : WHERE {{DATE_FROM_COL}} >= '{{DATE_FROM}}'
--             AND/OR {{DATE_TO_COL}} <= '{{DATE_TO}}'
--             (either bound may be None → single-sided limit)
--
-- [[/FILTER_PLACEHOLDER]]
-- Date cols available in this table: created_at

SELECT
    part_key,
    part_number,
    part_name,
    category_name,
    supplier_key,
    unit_cost,
    currency_code,
    stock_quantity,
    reorder_threshold,
    reorder_quantity,
    lead_time_days,
    storage_location,
    weight_grams,
    is_critical,
    last_receipt_date,
    expiry_date,
    part_status,
    origin_country,
    tariff_code,
    hsn_code,
    unit_of_measure,
    stock_value_amt,
    below_reorder_flag,
    created_at,
    load_ts,
    supplier_name,
    supplier_country,
    supplier_contact
FROM dim_parts;