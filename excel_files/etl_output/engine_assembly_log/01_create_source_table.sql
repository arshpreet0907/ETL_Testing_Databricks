-- ============================================================
-- Source table : engine_assembly_log
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-17 15:17
-- ============================================================

CREATE TABLE IF NOT EXISTS engine_assembly_log (
    assembly_log_id                    INT                        NOT NULL,  -- PK
    prod_order_id                      INT                        NOT NULL,
    engine_serial_no                   VARCHAR(20)                NOT NULL,
    vehicle_id                         INT                        NOT NULL,
    engine_type_cd                     VARCHAR(20)                NOT NULL,
    plant_cd                           VARCHAR(10)                NOT NULL,
    assembly_line_no                   INT                        NOT NULL,
    shift_cd                           VARCHAR(15)                NOT NULL,
    operator_emp_id                    INT                       ,
    start_ts                           TIMESTAMP                  NOT NULL,
    end_ts                             TIMESTAMP                  NOT NULL,
    torque_nm                          DECIMAL(8,2)              ,
    compression_ratio                  DECIMAL(4,1)              ,
    idle_rpm                           INT                       ,
    max_rpm                            INT                       ,
    oil_pressure_bar                   DECIMAL(4,2)              ,
    coolant_temp_c                     DECIMAL(5,2)              ,
    test_result_cd                     VARCHAR(10)                NOT NULL,
    defect_flag                        CHAR(1)                    NOT NULL,
    defect_desc                        VARCHAR(200)              ,
    rework_hrs                         DECIMAL(6,2)               NOT NULL,
    assembly_cost_amt                  DECIMAL(12,2)              NOT NULL,
    created_at                         TIMESTAMP                  NOT NULL,
    updated_at                         TIMESTAMP                  NOT NULL,
    batch_id                           VARCHAR(20)               ,
    PRIMARY KEY (assembly_log_id)
);