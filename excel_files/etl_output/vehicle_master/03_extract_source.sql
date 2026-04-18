-- ============================================================
-- STEP 3 : EXTRACT  |  source: vehicle_master
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-19 03:34
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): internal_notes
--
--
-- [[FILTER_PLACEHOLDER]]
-- The following WHERE clause is injected at runtime by custom_execution.py.
-- Do NOT edit this block manually — it is overwritten on each run.
--
-- PK filter modes (PK_FILTER_MODE in custom_execution.py):
--   full    : no PK WHERE clause
--   pk_range: WHERE vehicle_id >= {LOWER} AND/OR vehicle_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE vehicle_id IN ({PK_SET})
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
    vehicle_id,
    vin_number,
    model_nm,
    variant_cd,
    model_yr,
    color_desc,
    engine_type_cd,
    transmission_cd,
    plant_cd,
    base_price_amt,
    launch_dt,
    discontinue_dt,
    status_cd,
    fuel_economy_kmpl,
    gross_wt_kg,
    seating_capacity,
    country_of_origin,
    safety_rating,
    warranty_yrs,
    is_electric_flag,
    created_at,
    updated_at,
    created_by,
    payload_kg
FROM vehicle_master;