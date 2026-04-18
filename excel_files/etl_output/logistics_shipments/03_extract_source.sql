-- ============================================================
-- STEP 3 : EXTRACT  |  source: logistics_shipments
-- ============================================================
-- Dialect    : MySQL 8+
-- Generated  : 2026-04-19 00:02
--
-- Columns fetched from main source table (dropped cols excluded).
-- Excluded (drop): driver_nm, driver_phone, internal_ref_no
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
--   pk_range: WHERE m.shipment_id >= {LOWER} AND/OR m.shipment_id <= {UPPER}
--             (either bound may be None → single-sided limit)
--   pk_set  : WHERE m.shipment_id IN ({PK_SET})
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
    m.shipment_id,
    m.prod_order_id,
    m.sales_order_id,
    m.vehicle_id,
    m.origin_plant_cd,
    m.dest_dealer_id,
    m.carrier_nm,
    m.shipment_dt,
    m.estimated_arrival_dt,
    m.actual_arrival_dt,
    m.transport_mode_cd,
    m.tracking_no,
    m.vehicle_count,
    m.freight_cost_amt,
    m.insurance_cost_amt,
    m.total_cost_amt,
    m.status_cd,
    m.delay_reason_cd,
    m.distance_km,
    m.created_at,
    veh.vin_number AS veh_vin_number,
    veh.model_nm AS veh_model_nm,
    veh.engine_type_cd AS veh_engine_type_cd
FROM logistics_shipments m
LEFT JOIN vehicle_master veh
    ON m.vehicle_id = veh.vehicle_id;