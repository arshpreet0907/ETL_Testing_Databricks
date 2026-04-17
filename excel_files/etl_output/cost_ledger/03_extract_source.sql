-- ============================================================
-- STEP 3 : EXTRACT  |  source: cost_ledger
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-17 15:17
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): internal_notes
--
-- Enrichment joins:
--   LEFT   JOIN supplier_master  AS sup  ON m.supplier_id = sup.supplier_id
--          fetches: supplier_nm, country_cd
--
--
-- [[FILTER_PLACEHOLDER]]
-- The following WHERE clause is injected at runtime by custom_execution.py.
-- Do NOT edit this block manually — it is overwritten on each run.
--
-- PK filter modes (PK_FILTER_MODE in custom_execution.py):
--   full    : no PK WHERE clause
--   pk_range: WHERE m.ledger_id >= {LOWER} AND/OR m.ledger_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE m.ledger_id IN ({PK_SET})
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
    m.ledger_id,
    m.prod_order_id,
    m.vehicle_id,
    m.plant_cd,
    m.cost_type_cd,
    m.cost_category_cd,
    m.gl_account_no,
    m.cost_center_cd,
    m.posting_dt,
    m.fiscal_yr,
    m.fiscal_period,
    m.currency_cd,
    m.amount_lc,
    m.amount_usd,
    m.exchange_rate,
    m.qty_consumed,
    m.uom_cd,
    m.part_id,
    m.supplier_id,
    m.approved_flag,
    m.approved_by,
    m.created_at,
    m.updated_at,
    m.journal_ref_no,
    sup.supplier_nm AS sup_supplier_nm,
    sup.country_cd AS sup_country_cd
FROM cost_ledger m
LEFT JOIN supplier_master sup
    ON m.supplier_id = sup.supplier_id;