-- ============================================================
-- STEP 3 : EXTRACT  |  source: warranty_claims
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-17 15:38
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): internal_notes
--
-- Enrichment joins:
--   LEFT   JOIN vehicle_master  AS veh  ON m.vehicle_id = veh.vehicle_id
--          fetches: vin_number, model_nm, engine_type_cd
--
--
-- [[FILTER_PLACEHOLDER]]
-- The following WHERE clause is injected at runtime by custom_execution.py.
-- Do NOT edit this block manually — it is overwritten on each run.
--
-- PK filter modes (PK_FILTER_MODE in custom_execution.py):
--   full    : no PK WHERE clause
--   pk_range: WHERE m.claim_id >= {LOWER} AND/OR m.claim_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE m.claim_id IN ({PK_SET})
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
    m.claim_id,
    m.vin_number,
    m.vehicle_id,
    m.customer_id,
    m.claim_dt,
    m.reported_dt,
    m.defect_type_cd,
    m.defect_desc,
    m.part_id,
    m.repair_cost_amt,
    m.labour_cost_amt,
    m.parts_cost_amt,
    m.dealer_id,
    m.service_center_id,
    m.technician_emp_id,
    m.repair_start_dt,
    m.repair_end_dt,
    m.claim_status_cd,
    m.supplier_liability_flag,
    m.mileage_km,
    m.odometer_reading,
    m.created_at,
    m.approved_by,
    veh.vin_number AS veh_vin_number,
    veh.model_nm AS veh_model_nm,
    veh.engine_type_cd AS veh_engine_type_cd
FROM warranty_claims m
LEFT JOIN vehicle_master veh
    ON m.vehicle_id = veh.vehicle_id;