-- ============================================================
-- STEP 5 : EXTRACT TARGET  |  target: fact_shipment
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
--   pk_range: WHERE shipment_key >= {LOWER} AND/OR shipment_key <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE shipment_key IN ({PK_SET})
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
    shipment_key,
    production_order_key,
    sales_order_key,
    vehicle_key,
    origin_plant,
    destination_dealer_key,
    carrier_name,
    shipment_date,
    estimated_arrival,
    actual_arrival,
    transport_mode,
    tracking_number,
    vehicle_count,
    freight_cost,
    insurance_cost,
    total_cost,
    shipment_status,
    delay_reason,
    distance_km,
    is_delayed_flag,
    cost_per_vehicle,
    created_at,
    load_ts,
    vehicle_vin,
    vehicle_model,
    engine_type
FROM fact_shipment;