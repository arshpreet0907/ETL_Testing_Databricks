-- ============================================================
-- STEP 3 : EXTRACT  |  source: engine_assembly_log
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-19 00:43
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): batch_id
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
--   pk_range: WHERE m.assembly_log_id >= {LOWER} AND/OR m.assembly_log_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE m.assembly_log_id IN ({PK_SET})
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
    m.assembly_log_id,
    m.prod_order_id,
    m.engine_serial_no,
    m.vehicle_id,
    m.engine_type_cd,
    m.plant_cd,
    m.assembly_line_no,
    m.shift_cd,
    m.operator_emp_id,
    m.start_ts,
    m.end_ts,
    m.torque_nm,
    m.compression_ratio,
    m.idle_rpm,
    m.max_rpm,
    m.oil_pressure_bar,
    m.coolant_temp_c,
    m.test_result_cd,
    m.defect_flag,
    m.defect_desc,
    m.rework_hrs,
    m.assembly_cost_amt,
    m.created_at,
    m.updated_at,
    ord.order_dt AS ord_order_dt,
    ord.planned_end_dt AS ord_planned_end_dt,
    ord.order_status_cd AS ord_order_status_cd
FROM engine_assembly_log m
LEFT JOIN production_orders ord
    ON m.prod_order_id = ord.prod_order_id;