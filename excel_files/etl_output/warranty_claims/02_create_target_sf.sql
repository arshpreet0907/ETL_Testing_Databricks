-- ============================================================
-- Target table : fact_warranty
-- Dialect      : Snowflake
-- Generated    : 2026-04-19 01:14
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_warranty (
    claim_key                            NUMBER(19,0)               NOT NULL,  -- PK
    vin                                  VARCHAR(17)                NOT NULL,
    vehicle_key                          NUMBER(19,0)               NOT NULL,
    customer_key                         NUMBER(19,0)               NOT NULL,
    claim_date                           DATE                       NOT NULL,
    reported_date                        DATE                       NOT NULL,
    defect_type                          VARCHAR(20)                NOT NULL,
    defect_description                   VARCHAR(200)              ,
    part_key                             NUMBER(19,0)              ,  -- FK to dim_parts
    repair_cost                          FLOAT                      NOT NULL,
    labour_cost                          FLOAT                      NOT NULL,
    parts_cost                           FLOAT                      NOT NULL,
    total_claim_amt                      FLOAT                      NOT NULL,  -- Sum of all cost components
    dealer_key                           NUMBER(19,0)              ,
    service_center_key                   NUMBER(19,0)              ,
    technician_key                       NUMBER(19,0)              ,  -- FK to dim_employee
    repair_start_date                    DATE                      ,
    repair_end_date                      DATE                      ,
    claim_status                         VARCHAR(20)                NOT NULL,
    supplier_liable                      NUMBER(3,0)                NOT NULL DEFAULT 0,  -- Y/N → 1/0
    mileage_km                           NUMBER(10,0)              ,
    odometer_km                          NUMBER(10,0)              ,
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    approved_by                          VARCHAR(30)               ,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- constant: CURRENT_TIMESTAMP
    vehicle_model                        VARCHAR(50)               ,  -- Vehicle model on claim
    engine_type                          VARCHAR(20)               ,  -- Engine type on claim
    PRIMARY KEY (claim_key)
);