-- ============================================================
-- STEP 3 : EXTRACT  |  source: supplier_master
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-19 03:34
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): tax_id, bank_account_no, internal_notes, account_mgr_emp_id
--
-- Enrichment joins:
--   LEFT   JOIN employee_master  AS mgr  ON m.account_mgr_emp_id = mgr.emp_id
--          fetches: first_nm, last_nm, emp_code
--
--
-- [[FILTER_PLACEHOLDER]]
-- The following WHERE clause is injected at runtime by custom_execution.py.
-- Do NOT edit this block manually — it is overwritten on each run.
--
-- PK filter modes (PK_FILTER_MODE in custom_execution.py):
--   full    : no PK WHERE clause
--   pk_range: WHERE m.supplier_id >= {LOWER} AND/OR m.supplier_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE m.supplier_id IN ({PK_SET})
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
    m.supplier_id,
    m.supplier_nm,
    m.supplier_code,
    m.contact_person,
    m.email_addr,
    m.phone_no,
    m.country_cd,
    m.city_nm,
    m.address_txt,
    m.payment_terms_cd,
    m.credit_limit_amt,
    m.currency_cd,
    m.rating_score,
    m.is_approved_flag,
    m.approval_dt,
    m.contract_start_dt,
    m.contract_end_dt,
    m.status_cd,
    m.tier_level,
    m.created_at,
    m.updated_at,
    m.created_by,
    m.account_mgr_emp_id,
    mgr.first_nm AS mgr_first_nm,
    mgr.last_nm AS mgr_last_nm,
    mgr.emp_code AS mgr_emp_code
FROM supplier_master m
LEFT JOIN employee_master mgr
    ON m.account_mgr_emp_id = mgr.emp_id;