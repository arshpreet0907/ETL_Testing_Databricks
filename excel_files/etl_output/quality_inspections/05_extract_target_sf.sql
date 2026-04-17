-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: fact_quality
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
--   pk_range: WHERE inspection_key >= {LOWER} AND/OR inspection_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE inspection_key IN ({PK_SET})
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
    inspection_key,
    production_order_key,
    vehicle_key,
    inspector_key,
    inspection_date,
    inspection_type,
    defect_type,
    defect_description,
    severity_level,
    inspection_result,
    quality_grade,
    score,
    rework_required,
    rework_hours,
    rework_cost,
    plant_code,
    shift_name,
    production_line,
    checkpoint_number,
    is_defect_flag,
    created_at,
    updated_at,
    load_ts,
    inspector_name
FROM fact_quality;