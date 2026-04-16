-- ============================================================
-- Target table : fact_paint
-- Dialect      : Snowflake
-- Generated    : 2026-04-16 02:58
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_paint (
    paint_log_key                        NUMBER(19,0)               NOT NULL,  -- PK
    production_order_key                 NUMBER(19,0)               NOT NULL,  -- FK to fact_production
    vehicle_key                          NUMBER(19,0)               NOT NULL,
    plant_code                           VARCHAR(10)                NOT NULL,
    paint_line                           NUMBER(10,0)               NOT NULL,
    shift_name                           VARCHAR(15)                NOT NULL,
    color_code                           VARCHAR(20)                NOT NULL,
    color_name                           VARCHAR(30)                NOT NULL,
    oven_temperature_c                   FLOAT                      NOT NULL,
    bake_duration_mins                   NUMBER(10,0)               NOT NULL,
    thickness_um                         FLOAT                     ,  -- Microns
    gloss_level                          FLOAT                     ,
    has_defect                           NUMBER(3,0)                NOT NULL DEFAULT 0,  -- Y/N → 1/0
    defect_type                          VARCHAR(20)               ,
    requires_rework                      NUMBER(3,0)                NOT NULL DEFAULT 0,  -- Y/N → 1/0
    paint_cost                           FLOAT                      NOT NULL,
    operator_key                         NUMBER(19,0)              ,  -- FK to dim_employee
    process_start_ts                     TIMESTAMP_NTZ              NOT NULL,
    process_end_ts                       TIMESTAMP_NTZ              NOT NULL,
    humidity_percent                     FLOAT                     ,
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- constant: CURRENT_TIMESTAMP
    order_date                           DATE                      ,  -- Order date from production_orders
    planned_completion                   DATE                      ,  -- Planned end date from production_orders
    order_status                         VARCHAR(20)               ,  -- Order status from production_orders
    PRIMARY KEY (paint_log_key)
);