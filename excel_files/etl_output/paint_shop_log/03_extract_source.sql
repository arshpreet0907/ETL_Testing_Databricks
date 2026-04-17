-- ============================================================
-- STEP 3 : EXTRACT  |  source: paint_shop_log
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-17 15:17
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): primer_batch_no, topcoat_batch_no, clear_coat_batch_no, batch_id
--
-- Enrichment joins:
--   LEFT   JOIN production_orders  AS ord  ON m.prod_order_id = ord.prod_order_id
--          fetches: order_dt, planned_end_dt, order_status_cd
--
--
-- [[FILTER_PLACEHOLDER]]
-- The following WHERE clause is injected at runtime by custom_execution.py.
-- Do NOT edit this block manually — it is overwritten on each run.
--
-- PK filter modes (PK_FILTER_MODE in custom_execution.py):
--   full    : no PK WHERE clause
--   pk_range: WHERE m.paint_log_id >= {LOWER} AND/OR m.paint_log_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE m.paint_log_id IN ({PK_SET})
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
    m.paint_log_id,
    m.prod_order_id,
    m.vehicle_id,
    m.plant_cd,
    m.paint_line_no,
    m.shift_cd,
    m.color_cd,
    m.color_desc,
    m.oven_temp_celsius,
    m.bake_duration_mins,
    m.paint_thickness_um,
    m.gloss_level,
    m.defect_flag,
    m.defect_type_cd,
    m.rework_flag,
    m.paint_cost_amt,
    m.operator_emp_id,
    m.start_ts,
    m.end_ts,
    m.humidity_pct,
    m.created_at,
    ord.order_dt AS ord_order_dt,
    ord.planned_end_dt AS ord_planned_end_dt,
    ord.order_status_cd AS ord_order_status_cd
FROM paint_shop_log m
LEFT JOIN production_orders ord
    ON m.prod_order_id = ord.prod_order_id;