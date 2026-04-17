-- ============================================================
-- Source table : cost_ledger
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-17 15:17
-- ============================================================

CREATE TABLE IF NOT EXISTS cost_ledger (
    ledger_id                          INT                        NOT NULL,  -- PK
    prod_order_id                      INT                       ,
    vehicle_id                         INT                       ,
    plant_cd                           VARCHAR(10)                NOT NULL,
    cost_type_cd                       VARCHAR(30)                NOT NULL,
    cost_category_cd                   VARCHAR(30)                NOT NULL,
    gl_account_no                      VARCHAR(20)                NOT NULL,
    cost_center_cd                     VARCHAR(20)                NOT NULL,
    posting_dt                         DATE                       NOT NULL,
    fiscal_yr                          INT                        NOT NULL,
    fiscal_period                      INT                        NOT NULL,
    currency_cd                        VARCHAR(5)                 NOT NULL,
    amount_lc                          DECIMAL(16,2)              NOT NULL,
    amount_usd                         DECIMAL(14,2)             ,
    exchange_rate                      DECIMAL(10,4)             ,
    qty_consumed                       DECIMAL(12,2)             ,
    uom_cd                             VARCHAR(10)               ,
    part_id                            INT                       ,
    supplier_id                        INT                       ,
    approved_flag                      CHAR(1)                    NOT NULL,
    approved_by                        VARCHAR(30)               ,
    created_at                         TIMESTAMP                  NOT NULL,
    updated_at                         TIMESTAMP                  NOT NULL,
    journal_ref_no                     VARCHAR(20)               ,
    internal_notes                     VARCHAR(200)              ,
    PRIMARY KEY (ledger_id)
);