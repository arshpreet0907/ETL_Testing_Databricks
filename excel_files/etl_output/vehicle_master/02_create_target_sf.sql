-- ============================================================
-- Target table : dim_vehicle
-- Dialect      : Snowflake
-- Generated    : 2026-04-16 02:58
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_vehicle (
    vehicle_key                          NUMBER(19,0)               NOT NULL,  -- PK; Surrogate key
    vin                                  VARCHAR(17)                NOT NULL,  -- Vehicle Identification Number
    model_name                           VARCHAR(50)                NOT NULL,  -- Full model name
    variant_name                         VARCHAR(20)                NOT NULL,  -- Variant / trim level
    model_year                           NUMBER(10,0)               NOT NULL,  -- Manufacturing year
    color_name                           VARCHAR(30)               ,
    engine_type                          VARCHAR(20)                NOT NULL,
    transmission_type                    VARCHAR(20)                NOT NULL,
    manufacturing_plant                  VARCHAR(10)                NOT NULL,
    base_price_inr                       FLOAT                      NOT NULL,  -- Price rounded to 2dp
    launch_date                          DATE                       NOT NULL,
    discontinue_date                     DATE                      ,
    vehicle_status                       VARCHAR(20)                NOT NULL,
    fuel_economy                         NUMBER(5,2)               ,
    gross_weight_kg                      NUMBER(8,2)               ,
    seating_capacity                     NUMBER(10,0)               NOT NULL,
    origin_country                       VARCHAR(30)                NOT NULL,
    safety_rating                        NUMBER(3,1)               ,
    warranty_years                       NUMBER(10,0)               NOT NULL,
    is_electric                          NUMBER(3,0)                NOT NULL DEFAULT 0,  -- Y/N → 1/0 boolean
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    updated_at                           TIMESTAMP_NTZ              NOT NULL,
    created_by                           VARCHAR(30)               ,
    payload_kg                           NUMBER(8,2)               ,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- ETL pipeline load timestamp
    batch_id                             VARCHAR(50)                NOT NULL,  -- ETL batch identifier
    PRIMARY KEY (vehicle_key)
);