-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: dim_employee
-- ============================================================
-- Dialect   : Snowflake
-- Generated : 2026-04-19 00:35
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
--   pk_range: WHERE employee_key >= {LOWER} AND/OR employee_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE employee_key IN ({PK_SET})
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
    employee_key,
    employee_code,
    first_name,
    last_name,
    full_name,
    date_of_birth,
    gender,
    joining_date,
    department,
    job_role,
    grade,
    plant_code,
    shift_name,
    basic_salary,
    hra,
    gross_salary,
    pf_percent,
    employee_status,
    manager_key,
    created_at,
    updated_at,
    created_by,
    load_ts,
    manager_name
FROM dim_employee;