-- ============================================================
-- Target table : fact_quality
-- Dialect      : Snowflake
-- Generated    : 2026-04-17 15:38
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_quality (
    inspection_key                       NUMBER(19,0)               NOT NULL,  -- PK
    production_order_key                 NUMBER(19,0)               NOT NULL,  -- FK to fact_production
    vehicle_key                          NUMBER(19,0)               NOT NULL,
    inspector_key                        NUMBER(19,0)               NOT NULL,  -- FK to dim_employee
    inspection_date                      DATE                       NOT NULL,
    inspection_type                      VARCHAR(20)                NOT NULL,
    defect_type                          VARCHAR(20)               ,
    defect_description                   VARCHAR(200)              ,
    severity_level                       VARCHAR(10)               ,
    inspection_result                    VARCHAR(10)                NOT NULL,  -- PASS/FAIL/REWORK/HOLD
    quality_grade                        CHAR(1)                   ,
    score                                FLOAT                     ,
    rework_required                      NUMBER(3,0)                NOT NULL DEFAULT 0,  -- Y/N → 1/0
    rework_hours                         FLOAT                      NOT NULL DEFAULT 0,
    rework_cost                          FLOAT                      NOT NULL DEFAULT 0,
    plant_code                           VARCHAR(10)                NOT NULL,
    shift_name                           VARCHAR(15)                NOT NULL,
    production_line                      NUMBER(10,0)               NOT NULL,
    checkpoint_number                    NUMBER(10,0)               NOT NULL,
    is_defect_flag                       NUMBER(3,0)                NOT NULL DEFAULT 0,  -- 1 if any defect recorded
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    updated_at                           TIMESTAMP_NTZ              NOT NULL,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- constant: CURRENT_TIMESTAMP
    inspector_name                       VARCHAR(100)              ,  -- Inspector full name from employee_master
    PRIMARY KEY (inspection_key)
);