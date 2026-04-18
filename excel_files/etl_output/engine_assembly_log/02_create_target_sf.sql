-- ============================================================
-- Target table : fact_engine_assembly
-- Dialect      : Snowflake
-- Generated    : 2026-04-19 00:02
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_engine_assembly (
    assembly_log_key                     NUMBER(19,0)               NOT NULL,  -- PK
    production_order_key                 NUMBER(19,0)               NOT NULL,  -- FK to fact_production
    engine_serial                        VARCHAR(20)                NOT NULL,
    vehicle_key                          NUMBER(19,0)               NOT NULL,
    engine_type                          VARCHAR(20)                NOT NULL,
    plant_code                           VARCHAR(10)                NOT NULL,
    assembly_line                        NUMBER(10,0)               NOT NULL,
    shift_name                           VARCHAR(15)                NOT NULL,
    operator_key                         NUMBER(19,0)              ,  -- FK to dim_employee
    assembly_start_ts                    TIMESTAMP_NTZ              NOT NULL,
    assembly_end_ts                      TIMESTAMP_NTZ              NOT NULL,
    torque_nm                            FLOAT                     ,
    compression_ratio                    FLOAT                     ,
    idle_rpm                             NUMBER(10,0)              ,
    max_rpm                              NUMBER(10,0)              ,
    oil_pressure_bar                     FLOAT                     ,
    coolant_temp_c                       FLOAT                     ,
    test_result                          VARCHAR(10)                NOT NULL,  -- PASS/FAIL/RETEST
    has_defect                           NUMBER(3,0)                NOT NULL DEFAULT 0,  -- Y/N → 1/0
    defect_description                   VARCHAR(200)              ,
    rework_hours                         FLOAT                      NOT NULL DEFAULT 0,
    assembly_cost                        FLOAT                      NOT NULL,
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    updated_at                           TIMESTAMP_NTZ              NOT NULL,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- constant: CURRENT_TIMESTAMP
    order_date                           DATE                      ,  -- Order date from production_orders
    planned_completion                   DATE                      ,  -- Planned end date
    order_status                         VARCHAR(20)               ,  -- Order status
    PRIMARY KEY (assembly_log_key)
);