-- ============================================================
-- STEP 3 : EXTRACT  |  source: sales_orders
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-19 00:43
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): internal_ref_no
--
-- Enrichment joins:
--   LEFT   JOIN vehicle_master  AS veh  ON m.vehicle_id = veh.vehicle_id
--          fetches: vin_number, model_nm, variant_cd
--
--
-- [[FILTER_PLACEHOLDER]]
-- The following WHERE clause is injected at runtime by custom_execution.py.
-- Do NOT edit this block manually — it is overwritten on each run.
--
-- PK filter modes (PK_FILTER_MODE in custom_execution.py):
--   full    : no PK WHERE clause
--   pk_range: WHERE m.sales_order_id >= {LOWER} AND/OR m.sales_order_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE m.sales_order_id IN ({PK_SET})
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
    m.sales_order_id,
    m.vehicle_id,
    m.dealer_id,
    m.customer_id,
    m.order_dt,
    m.delivery_dt,
    m.invoice_no,
    m.invoice_dt,
    m.sale_price_amt,
    m.discount_pct,
    m.tax_amt,
    m.insurance_amt,
    m.accessories_amt,
    m.total_invoice_amt,
    m.payment_mode_cd,
    m.finance_bank_nm,
    m.vin_allocated,
    m.region_cd,
    m.sales_rep_emp_id,
    m.order_status_cd,
    m.cancel_reason_cd,
    m.source_channel_cd,
    m.created_at,
    veh.vin_number AS veh_vin_number,
    veh.model_nm AS veh_model_nm,
    veh.variant_cd AS veh_variant_cd
FROM sales_orders m
LEFT JOIN vehicle_master veh
    ON m.vehicle_id = veh.vehicle_id;