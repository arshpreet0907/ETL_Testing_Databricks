-- ============================================================
-- Target table : dim_supplier
-- Dialect      : Snowflake
-- Generated    : 2026-04-19 01:14
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_key                         NUMBER(19,0)               NOT NULL,  -- PK
    supplier_name                        VARCHAR(100)               NOT NULL,
    supplier_code                        VARCHAR(20)                NOT NULL,
    contact_name                         VARCHAR(60)               ,
    email                                VARCHAR(100)              ,
    phone                                VARCHAR(20)               ,
    country                              VARCHAR(30)                NOT NULL,
    city                                 VARCHAR(50)               ,
    address                              VARCHAR(200)              ,
    payment_terms                        VARCHAR(10)                NOT NULL,
    credit_limit                         FLOAT                     ,
    currency_code                        VARCHAR(5)                 NOT NULL,
    supplier_rating                      NUMBER(3,1)               ,
    is_approved                          NUMBER(3,0)                NOT NULL DEFAULT 0,  -- Y/N → 1/0
    approval_date                        DATE                      ,
    contract_start                       DATE                      ,
    contract_end                         DATE                      ,
    supplier_status                      VARCHAR(20)                NOT NULL,
    tier_level                           NUMBER(10,0)               NOT NULL,  -- 1=Tier1, 2=Tier2, 3=Tier3
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    updated_at                           TIMESTAMP_NTZ              NOT NULL,
    created_by                           VARCHAR(30)               ,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- constant: CURRENT_TIMESTAMP
    account_manager_name                 VARCHAR(100)              ,  -- Account manager full name
    account_manager_code                 VARCHAR(20)               ,  -- Account manager employee code
    PRIMARY KEY (supplier_key)
);