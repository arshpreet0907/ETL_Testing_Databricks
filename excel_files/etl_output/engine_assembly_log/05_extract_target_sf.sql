-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: fact_engine_assembly
-- ============================================================
-- Dialect   : Snowflake
-- Generated : 2026-04-16 02:58
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
--   pk_range: WHERE assembly_log_key >= {LOWER} AND/OR assembly_log_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE assembly_log_key IN ({PK_SET})
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
    assembly_log_key,
    production_order_key,
    engine_serial,
    vehicle_key,
    engine_type,
    plant_code,
    assembly_line,
    shift_name,
    operator_key,
    assembly_start_ts,
    assembly_end_ts,
    torque_nm,
    compression_ratio,
    idle_rpm,
    max_rpm,
    oil_pressure_bar,
    coolant_temp_c,
    test_result,
    has_defect,
    defect_description,
    rework_hours,
    assembly_cost,
    created_at,
    updated_at,
    load_ts,
    order_date,
    planned_completion,
    order_status
FROM fact_engine_assembly;