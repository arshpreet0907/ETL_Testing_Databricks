-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: fact_warranty
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
--   pk_range: WHERE claim_key >= {LOWER} AND/OR claim_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE claim_key IN ({PK_SET})
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
    claim_key,
    vin,
    vehicle_key,
    customer_key,
    claim_date,
    reported_date,
    defect_type,
    defect_description,
    part_key,
    repair_cost,
    labour_cost,
    parts_cost,
    total_claim_amt,
    dealer_key,
    service_center_key,
    technician_key,
    repair_start_date,
    repair_end_date,
    claim_status,
    supplier_liable,
    mileage_km,
    odometer_km,
    created_at,
    approved_by,
    load_ts,
    vehicle_model,
    engine_type
FROM fact_warranty;