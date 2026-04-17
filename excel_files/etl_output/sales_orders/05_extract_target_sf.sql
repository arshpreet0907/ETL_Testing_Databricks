-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: fact_sales
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
--   pk_range: WHERE sales_order_key >= {LOWER} AND/OR sales_order_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE sales_order_key IN ({PK_SET})
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
    sales_order_key,
    vehicle_key,
    dealer_key,
    customer_key,
    order_date,
    delivery_date,
    invoice_number,
    invoice_date,
    sale_price,
    discount_percent,
    discount_amt,
    tax_amount,
    insurance_amount,
    accessories_amount,
    total_invoice,
    payment_mode,
    finance_bank,
    vin_number,
    region,
    sales_rep_key,
    order_status,
    cancellation_reason,
    sales_channel,
    created_at,
    load_ts,
    vehicle_vin,
    vehicle_model,
    vehicle_variant
FROM fact_sales;