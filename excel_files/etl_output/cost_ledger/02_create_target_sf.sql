-- ============================================================
-- Target table : fact_cost
-- Dialect      : Snowflake
-- Generated    : 2026-04-19 01:14
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_cost (
    ledger_key                           NUMBER(19,0)               NOT NULL,  -- PK
    production_order_key                 NUMBER(19,0)              ,  -- FK to fact_production
    vehicle_key                          NUMBER(19,0)              ,  -- FK to dim_vehicle
    plant_code                           VARCHAR(10)                NOT NULL,
    cost_type                            VARCHAR(30)                NOT NULL,  -- Material/Labour/Overhead etc.
    cost_category                        VARCHAR(30)                NOT NULL,  -- Direct/Indirect/Fixed/Variable
    gl_account                           VARCHAR(20)                NOT NULL,
    cost_center                          VARCHAR(20)                NOT NULL,
    posting_date                         DATE                       NOT NULL,
    fiscal_year                          NUMBER(10,0)               NOT NULL,
    fiscal_period                        NUMBER(10,0)               NOT NULL,  -- 1–12
    currency_code                        VARCHAR(5)                 NOT NULL,
    amount_local                         FLOAT                      NOT NULL,
    amount_usd                           FLOAT                     ,
    fx_rate                              FLOAT                     ,  -- Default 1.0 if null
    quantity                             FLOAT                     ,
    unit_of_measure                      VARCHAR(10)               ,
    part_key                             NUMBER(19,0)              ,  -- FK to dim_parts
    supplier_key                         NUMBER(19,0)              ,  -- FK to dim_supplier
    is_approved                          NUMBER(3,0)                NOT NULL DEFAULT 0,  -- Y/N → 1/0
    approved_by                          VARCHAR(30)               ,
    fiscal_yr_month                      VARCHAR(7)                 NOT NULL,  -- e.g. 2024-03
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    updated_at                           TIMESTAMP_NTZ              NOT NULL,
    journal_reference                    VARCHAR(20)               ,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- constant: CURRENT_TIMESTAMP
    supplier_name                        VARCHAR(100)              ,  -- Supplier name on cost entry
    supplier_country                     VARCHAR(30)               ,  -- Supplier country
    PRIMARY KEY (ledger_key)
);