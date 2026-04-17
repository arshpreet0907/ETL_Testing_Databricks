-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: fact_production
-- ============================================================
-- Dialect   : Snowflake
-- Generated : 2026-04-17 15:38
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
--   pk_range: WHERE production_order_key >= {LOWER} AND/OR production_order_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE production_order_key IN ({PK_SET})
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
    production_order_key,
    vehicle_key,
    plant_code,
    order_date,
    planned_start_date,
    actual_start_date,
    planned_end_date,
    actual_end_date,
    planned_quantity,
    produced_quantity,
    rejected_quantity,
    shift_name,
    production_line,
    supervisor_key,
    order_status,
    priority_level,
    downtime_minutes,
    scrap_cost,
    rework_hours,
    efficiency_percent,
    yield_rate_pct,
    target_takt_seconds,
    takt_variance_pct,
    created_at,
    updated_at,
    load_ts,
    vehicle_vin,
    vehicle_model,
    vehicle_variant
FROM fact_production;