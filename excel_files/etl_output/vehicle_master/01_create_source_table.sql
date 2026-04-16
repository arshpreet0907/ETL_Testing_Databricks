-- ============================================================
-- Source table : vehicle_master
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-16 02:58
-- ============================================================

CREATE TABLE IF NOT EXISTS vehicle_master (
    vehicle_id                         INT                        NOT NULL,  -- PK
    vin_number                         VARCHAR(17)                NOT NULL,
    model_nm                           VARCHAR(50)                NOT NULL,
    variant_cd                         VARCHAR(20)                NOT NULL,
    model_yr                           INT                        NOT NULL,
    color_desc                         VARCHAR(30)               ,
    engine_type_cd                     VARCHAR(20)                NOT NULL,
    transmission_cd                    VARCHAR(20)                NOT NULL,
    plant_cd                           VARCHAR(10)                NOT NULL,
    base_price_amt                     DECIMAL(14,2)              NOT NULL,
    launch_dt                          DATE                       NOT NULL,
    discontinue_dt                     DATE                      ,
    status_cd                          VARCHAR(20)                NOT NULL,
    fuel_economy_kmpl                  DECIMAL(5,2)              ,
    gross_wt_kg                        DECIMAL(8,2)              ,
    seating_capacity                   INT                        NOT NULL,
    country_of_origin                  VARCHAR(30)                NOT NULL,
    safety_rating                      DECIMAL(3,1)              ,
    warranty_yrs                       INT                        NOT NULL,
    is_electric_flag                   CHAR(1)                    NOT NULL,
    created_at                         TIMESTAMP                  NOT NULL,
    updated_at                         TIMESTAMP                  NOT NULL,
    created_by                         VARCHAR(30)               ,
    payload_kg                         DECIMAL(8,2)              ,
    internal_notes                     VARCHAR(200)              ,
    PRIMARY KEY (vehicle_id)
);