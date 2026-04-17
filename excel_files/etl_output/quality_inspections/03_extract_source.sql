-- ============================================================
-- STEP 3 : EXTRACT  |  source: quality_inspections
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-17 15:17
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): tool_id, photo_ref_id, batch_id
--
-- Enrichment joins:
--   LEFT   JOIN employee_master  AS emp  ON m.inspector_emp_id = emp.emp_id
--          fetches: first_nm, last_nm
--
--
-- [[FILTER_PLACEHOLDER]]
-- The following WHERE clause is injected at runtime by custom_execution.py.
-- Do NOT edit this block manually — it is overwritten on each run.
--
-- PK filter modes (PK_FILTER_MODE in custom_execution.py):
--   full    : no PK WHERE clause
--   pk_range: WHERE m.inspection_id >= {LOWER} AND/OR m.inspection_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE m.inspection_id IN ({PK_SET})
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
    m.inspection_id,
    m.prod_order_id,
    m.vehicle_id,
    m.inspector_emp_id,
    m.inspection_dt,
    m.inspection_type_cd,
    m.defect_type_cd,
    m.defect_desc,
    m.severity_cd,
    m.result_cd,
    m.grade_cd,
    m.inspection_score,
    m.rework_required_flag,
    m.rework_hrs,
    m.rework_cost_amt,
    m.plant_cd,
    m.shift_cd,
    m.line_no,
    m.checkpoint_no,
    m.created_at,
    m.updated_at,
    emp.first_nm AS emp_first_nm,
    emp.last_nm AS emp_last_nm
FROM quality_inspections m
LEFT JOIN employee_master emp
    ON m.inspector_emp_id = emp.emp_id;