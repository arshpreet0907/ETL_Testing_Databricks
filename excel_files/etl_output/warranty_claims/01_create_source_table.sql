-- ============================================================
-- Source table : warranty_claims
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-19 00:35
-- ============================================================

CREATE TABLE IF NOT EXISTS warranty_claims (
    claim_id                           INT                        NOT NULL,  -- PK
    vin_number                         VARCHAR(17)                NOT NULL,
    vehicle_id                         INT                        NOT NULL,
    customer_id                        INT                        NOT NULL,
    claim_dt                           DATE                       NOT NULL,
    reported_dt                        DATE                       NOT NULL,
    defect_type_cd                     VARCHAR(20)                NOT NULL,
    defect_desc                        VARCHAR(200)              ,
    part_id                            INT                       ,
    repair_cost_amt                    DECIMAL(12,2)              NOT NULL,
    labour_cost_amt                    DECIMAL(10,2)              NOT NULL,
    parts_cost_amt                     DECIMAL(10,2)              NOT NULL,
    dealer_id                          INT                       ,
    service_center_id                  INT                       ,
    technician_emp_id                  INT                       ,
    repair_start_dt                    DATE                      ,
    repair_end_dt                      DATE                      ,
    claim_status_cd                    VARCHAR(20)                NOT NULL,
    supplier_liability_flag            CHAR(1)                    NOT NULL,
    mileage_km                         INT                       ,
    odometer_reading                   INT                       ,
    created_at                         TIMESTAMP                  NOT NULL,
    approved_by                        VARCHAR(30)               ,
    internal_notes                     VARCHAR(200)              ,
    PRIMARY KEY (claim_id)
);