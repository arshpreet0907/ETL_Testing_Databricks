-- ============================================================
-- Target table : dim_parts
-- Dialect      : Snowflake
-- Generated    : 2026-04-17 15:17
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_parts (
    part_key                             NUMBER(19,0)               NOT NULL,  -- PK
    part_number                          VARCHAR(20)                NOT NULL,
    part_name                            VARCHAR(100)               NOT NULL,
    category_name                        VARCHAR(30)                NOT NULL,
    supplier_key                         NUMBER(19,0)               NOT NULL,  -- FK to dim_supplier
    unit_cost                            FLOAT                      NOT NULL,
    currency_code                        VARCHAR(5)                 NOT NULL,
    stock_quantity                       NUMBER(10,0)               NOT NULL,
    reorder_threshold                    NUMBER(10,0)               NOT NULL,
    reorder_quantity                     NUMBER(10,0)               NOT NULL,
    lead_time_days                       NUMBER(10,0)               NOT NULL,
    storage_location                     VARCHAR(20)               ,
    weight_grams                         NUMBER(10,2)              ,
    is_critical                          NUMBER(3,0)                NOT NULL DEFAULT 0,  -- Y/N → 1/0
    last_receipt_date                    DATE                      ,
    expiry_date                          DATE                      ,
    part_status                          VARCHAR(20)                NOT NULL,
    origin_country                       VARCHAR(30)               ,
    tariff_code                          VARCHAR(15)               ,
    hsn_code                             VARCHAR(10)               ,
    unit_of_measure                      VARCHAR(10)                NOT NULL,
    stock_value_amt                      FLOAT                      NOT NULL,  -- Stock qty × unit cost
    below_reorder_flag                   NUMBER(3,0)                NOT NULL DEFAULT 0,  -- 1 if stock below threshold
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- constant: CURRENT_TIMESTAMP
    supplier_name                        VARCHAR(100)              ,  -- Supplier name from supplier_master
    supplier_country                     VARCHAR(30)               ,  -- Supplier country uppercased
    supplier_contact                     VARCHAR(60)               ,  -- Supplier contact person
    PRIMARY KEY (part_key)
);