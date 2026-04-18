-- ============================================================
-- STEP 3 : EXTRACT  |  source: production_orders
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-19 01:14
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): batch_flag
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
--   pk_range: WHERE m.prod_order_id >= {LOWER} AND/OR m.prod_order_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE m.prod_order_id IN ({PK_SET})
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
    m.prod_order_id,
    m.vehicle_id,
    m.plant_cd,
    m.order_dt,
    m.planned_start_dt,
    m.actual_start_dt,
    m.planned_end_dt,
    m.actual_end_dt,
    m.qty_planned,
    m.qty_produced,
    m.qty_rejected,
    m.shift_cd,
    m.line_no,
    m.supervisor_emp_id,
    m.order_status_cd,
    m.priority_lvl,
    m.downtime_mins,
    m.scrap_cost_amt,
    m.rework_hrs,
    m.efficiency_pct,
    m.target_takt_secs,
    m.actual_takt_secs,
    m.created_at,
    m.updated_at,
    veh.vin_number AS veh_vin_number,
    veh.model_nm AS veh_model_nm,
    veh.variant_cd AS veh_variant_cd
FROM production_orders m
LEFT JOIN vehicle_master veh
    ON m.vehicle_id = veh.vehicle_id;