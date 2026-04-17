-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: fact_paint
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
--   pk_range: WHERE paint_log_key >= {LOWER} AND/OR paint_log_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE paint_log_key IN ({PK_SET})
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
    paint_log_key,
    production_order_key,
    vehicle_key,
    plant_code,
    paint_line,
    shift_name,
    color_code,
    color_name,
    oven_temperature_c,
    bake_duration_mins,
    thickness_um,
    gloss_level,
    has_defect,
    defect_type,
    requires_rework,
    paint_cost,
    operator_key,
    process_start_ts,
    process_end_ts,
    humidity_percent,
    created_at,
    load_ts,
    order_date,
    planned_completion,
    order_status
FROM fact_paint;