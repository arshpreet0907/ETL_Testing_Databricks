-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: dim_vehicle
-- ============================================================
-- Dialect   : Snowflake
-- Generated : 2026-04-19 01:14
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
--   pk_range: WHERE vehicle_key >= {LOWER} AND/OR vehicle_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE vehicle_key IN ({PK_SET})
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
    vehicle_key,
    vin,
    model_name,
    variant_name,
    model_year,
    color_name,
    engine_type,
    transmission_type,
    manufacturing_plant,
    base_price_inr,
    launch_date,
    discontinue_date,
    vehicle_status,
    fuel_economy,
    gross_weight_kg,
    seating_capacity,
    origin_country,
    safety_rating,
    warranty_years,
    is_electric,
    created_at,
    updated_at,
    created_by,
    payload_kg,
    load_ts,
    batch_id
FROM dim_vehicle;