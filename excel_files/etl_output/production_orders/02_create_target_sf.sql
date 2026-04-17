-- ============================================================
-- Target table : fact_production
-- Dialect      : Snowflake
-- Generated    : 2026-04-17 15:38
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_production (
    production_order_key                 NUMBER(19,0)               NOT NULL,  -- PK
    vehicle_key                          NUMBER(19,0)               NOT NULL,  -- FK to dim_vehicle
    plant_code                           VARCHAR(10)                NOT NULL,
    order_date                           DATE                       NOT NULL,
    planned_start_date                   DATE                       NOT NULL,
    actual_start_date                    DATE                      ,
    planned_end_date                     DATE                       NOT NULL,
    actual_end_date                      DATE                      ,
    planned_quantity                     NUMBER(10,0)               NOT NULL,
    produced_quantity                    NUMBER(10,0)               NOT NULL,
    rejected_quantity                    NUMBER(10,0)               NOT NULL DEFAULT 0,
    shift_name                           VARCHAR(15)                NOT NULL,
    production_line                      NUMBER(10,0)               NOT NULL,
    supervisor_key                       NUMBER(19,0)              ,  -- FK to dim_employee
    order_status                         VARCHAR(20)                NOT NULL,
    priority_level                       NUMBER(10,0)               NOT NULL,
    downtime_minutes                     NUMBER(10,0)               NOT NULL DEFAULT 0,
    scrap_cost                           FLOAT                      NOT NULL DEFAULT 0,
    rework_hours                         FLOAT                      NOT NULL DEFAULT 0,
    efficiency_percent                   FLOAT                     ,
    yield_rate_pct                       FLOAT                     ,  -- Derived: produced/planned %
    target_takt_seconds                  NUMBER(10,0)               NOT NULL,  -- Standard takt time in seconds for the line
    takt_variance_pct                    FLOAT                     ,  -- Takt time deviation %
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    updated_at                           TIMESTAMP_NTZ              NOT NULL,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- constant: CURRENT_TIMESTAMP
    vehicle_vin                          VARCHAR(17)               ,  -- VIN from vehicle_master
    vehicle_model                        VARCHAR(50)               ,  -- Model name from vehicle_master
    vehicle_variant                      VARCHAR(20)               ,  -- Variant from vehicle_master
    PRIMARY KEY (production_order_key)
);