-- ============================================================
-- Target table : fact_shipment
-- Dialect      : Snowflake
-- Generated    : 2026-04-19 00:35
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_shipment (
    shipment_key                         NUMBER(19,0)               NOT NULL,  -- PK
    production_order_key                 NUMBER(19,0)              ,  -- FK to fact_production
    sales_order_key                      NUMBER(19,0)              ,  -- FK to fact_sales
    vehicle_key                          NUMBER(19,0)               NOT NULL,
    origin_plant                         VARCHAR(10)                NOT NULL,
    destination_dealer_key               NUMBER(19,0)               NOT NULL,  -- FK to dim_dealer
    carrier_name                         VARCHAR(50)                NOT NULL,
    shipment_date                        DATE                       NOT NULL,
    estimated_arrival                    DATE                      ,
    actual_arrival                       DATE                      ,
    transport_mode                       VARCHAR(20)                NOT NULL,
    tracking_number                      VARCHAR(30)               ,
    vehicle_count                        NUMBER(10,0)               NOT NULL,
    freight_cost                         FLOAT                      NOT NULL,
    insurance_cost                       FLOAT                      NOT NULL,
    total_cost                           FLOAT                      NOT NULL,
    shipment_status                      VARCHAR(20)                NOT NULL,
    delay_reason                         VARCHAR(50)               ,
    distance_km                          FLOAT                     ,
    is_delayed_flag                      NUMBER(3,0)                NOT NULL DEFAULT 0,  -- 1 if arrived after ETA
    cost_per_vehicle                     FLOAT                     ,  -- Total cost ÷ vehicle count
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- constant: CURRENT_TIMESTAMP
    vehicle_vin                          VARCHAR(17)               ,  -- VIN on shipment
    vehicle_model                        VARCHAR(50)               ,  -- Vehicle model on shipment
    engine_type                          VARCHAR(20)               ,  -- Engine type
    PRIMARY KEY (shipment_key)
);