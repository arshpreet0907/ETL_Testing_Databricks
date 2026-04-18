-- ============================================================
-- Source table : logistics_shipments
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-19 01:14
-- ============================================================

CREATE TABLE IF NOT EXISTS logistics_shipments (
    shipment_id                        INT                        NOT NULL,  -- PK
    prod_order_id                      INT                       ,
    sales_order_id                     INT                       ,
    vehicle_id                         INT                        NOT NULL,
    origin_plant_cd                    VARCHAR(10)                NOT NULL,
    dest_dealer_id                     INT                        NOT NULL,
    carrier_nm                         VARCHAR(50)                NOT NULL,
    shipment_dt                        DATE                       NOT NULL,
    estimated_arrival_dt               DATE                      ,
    actual_arrival_dt                  DATE                      ,
    transport_mode_cd                  VARCHAR(20)                NOT NULL,
    tracking_no                        VARCHAR(30)               ,
    vehicle_count                      INT                        NOT NULL,
    freight_cost_amt                   DECIMAL(12,2)              NOT NULL,
    insurance_cost_amt                 DECIMAL(10,2)              NOT NULL,
    total_cost_amt                     DECIMAL(12,2)              NOT NULL,
    status_cd                          VARCHAR(20)                NOT NULL,
    delay_reason_cd                    VARCHAR(50)               ,
    distance_km                        DECIMAL(8,2)              ,
    created_at                         TIMESTAMP                  NOT NULL,
    driver_nm                          VARCHAR(60)               ,
    driver_phone                       VARCHAR(15)               ,
    internal_ref_no                    VARCHAR(20)               ,
    PRIMARY KEY (shipment_id)
);