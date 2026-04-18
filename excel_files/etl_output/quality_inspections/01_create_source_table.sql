-- ============================================================
-- Source table : quality_inspections
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-19 01:14
-- ============================================================

CREATE TABLE IF NOT EXISTS quality_inspections (
    inspection_id                      INT                        NOT NULL,  -- PK
    prod_order_id                      INT                        NOT NULL,
    vehicle_id                         INT                        NOT NULL,
    inspector_emp_id                   INT                        NOT NULL,
    inspection_dt                      DATE                       NOT NULL,
    inspection_type_cd                 VARCHAR(20)                NOT NULL,
    defect_type_cd                     VARCHAR(20)               ,
    defect_desc                        VARCHAR(200)              ,
    severity_cd                        VARCHAR(10)               ,
    result_cd                          VARCHAR(10)                NOT NULL,
    grade_cd                           CHAR(1)                   ,
    inspection_score                   DECIMAL(5,2)              ,
    rework_required_flag               CHAR(1)                    NOT NULL,
    rework_hrs                         DECIMAL(6,2)               NOT NULL,
    rework_cost_amt                    DECIMAL(12,2)              NOT NULL,
    plant_cd                           VARCHAR(10)                NOT NULL,
    shift_cd                           VARCHAR(15)                NOT NULL,
    line_no                            INT                        NOT NULL,
    checkpoint_no                      INT                        NOT NULL,
    created_at                         TIMESTAMP                  NOT NULL,
    updated_at                         TIMESTAMP                  NOT NULL,
    tool_id                            VARCHAR(20)               ,
    photo_ref_id                       VARCHAR(50)               ,
    batch_id                           VARCHAR(20)               ,
    PRIMARY KEY (inspection_id)
);