-- ============================================================
-- Source table : supplier_master
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-16 02:58
-- ============================================================

CREATE TABLE IF NOT EXISTS supplier_master (
    supplier_id                        INT                        NOT NULL,  -- PK
    supplier_nm                        VARCHAR(100)               NOT NULL,
    supplier_code                      VARCHAR(20)                NOT NULL,
    contact_person                     VARCHAR(60)               ,
    email_addr                         VARCHAR(100)              ,
    phone_no                           VARCHAR(20)               ,
    country_cd                         VARCHAR(30)                NOT NULL,
    city_nm                            VARCHAR(50)               ,
    address_txt                        VARCHAR(200)              ,
    payment_terms_cd                   VARCHAR(10)                NOT NULL,
    credit_limit_amt                   DECIMAL(14,2)             ,
    currency_cd                        VARCHAR(5)                 NOT NULL,
    rating_score                       DECIMAL(3,1)              ,
    is_approved_flag                   CHAR(1)                    NOT NULL,
    approval_dt                        DATE                      ,
    contract_start_dt                  DATE                      ,
    contract_end_dt                    DATE                      ,
    status_cd                          VARCHAR(20)                NOT NULL,
    tier_level                         INT                        NOT NULL,
    tax_id                             VARCHAR(20)               ,
    bank_account_no                    VARCHAR(20)               ,
    created_at                         TIMESTAMP                  NOT NULL,
    updated_at                         TIMESTAMP                  NOT NULL,
    created_by                         VARCHAR(30)               ,
    internal_notes                     VARCHAR(200)              ,
    account_mgr_emp_id                 INT                       ,
    PRIMARY KEY (supplier_id)
);