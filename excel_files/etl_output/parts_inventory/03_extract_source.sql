-- ============================================================
-- STEP 3 : EXTRACT  |  source: parts_inventory
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-19 00:43
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): internal_ref_cd, remarks
--
-- Enrichment joins:
--   LEFT   JOIN supplier_master  AS sup  ON m.supplier_id = sup.supplier_id
--          fetches: supplier_nm, country_cd, contact_person
--
--
-- [[FILTER_PLACEHOLDER]]
-- The following WHERE clause is injected at runtime by custom_execution.py.
-- Do NOT edit this block manually — it is overwritten on each run.
--
-- PK filter modes (PK_FILTER_MODE in custom_execution.py):
--   full    : no PK WHERE clause
--   pk_range: WHERE m.part_id >= {LOWER} AND/OR m.part_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE m.part_id IN ({PK_SET})
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
    m.part_id,
    m.part_no,
    m.part_nm,
    m.part_category,
    m.supplier_id,
    m.unit_cost_amt,
    m.currency_cd,
    m.qty_on_hand,
    m.reorder_point,
    m.reorder_qty,
    m.lead_time_days,
    m.storage_loc_cd,
    m.weight_gm,
    m.is_critical_flag,
    m.last_receipt_dt,
    m.expiry_dt,
    m.status_cd,
    m.country_of_origin,
    m.tariff_code,
    m.hsn_code,
    m.uom_cd,
    m.created_at,
    sup.supplier_nm AS sup_supplier_nm,
    sup.country_cd AS sup_country_cd,
    sup.contact_person AS sup_contact_person
FROM parts_inventory m
LEFT JOIN supplier_master sup
    ON m.supplier_id = sup.supplier_id;