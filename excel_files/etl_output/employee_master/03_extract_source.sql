-- ============================================================
-- STEP 3 : EXTRACT  |  source: employee_master
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-19 00:35
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): pan_no, aadhaar_no, phone_no, remarks
--
-- Enrichment joins:
--   LEFT   JOIN employee_master  AS mgr  ON m.mgr_emp_id = mgr.emp_id
--          fetches: first_nm, last_nm
--
--
-- [[FILTER_PLACEHOLDER]]
-- The following WHERE clause is injected at runtime by custom_execution.py.
-- Do NOT edit this block manually — it is overwritten on each run.
--
-- PK filter modes (PK_FILTER_MODE in custom_execution.py):
--   full    : no PK WHERE clause
--   pk_range: WHERE m.emp_id >= {LOWER} AND/OR m.emp_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE m.emp_id IN ({PK_SET})
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
    m.emp_id,
    m.emp_code,
    m.first_nm,
    m.last_nm,
    m.dob_dt,
    m.gender_cd,
    m.join_dt,
    m.dept_nm,
    m.role_nm,
    m.grade_cd,
    m.plant_cd,
    m.shift_cd,
    m.basic_salary_amt,
    m.hra_amt,
    m.pf_pct,
    m.status_cd,
    m.mgr_emp_id,
    m.created_at,
    m.updated_at,
    m.created_by,
    mgr.first_nm AS mgr_first_nm,
    mgr.last_nm AS mgr_last_nm
FROM employee_master m
LEFT JOIN employee_master mgr
    ON m.mgr_emp_id = mgr.emp_id;