-- ============================================================
-- Source table : parts_inventory
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-16 02:58
-- ============================================================

CREATE TABLE IF NOT EXISTS parts_inventory (
    part_id                            INT                        NOT NULL,  -- PK
    part_no                            VARCHAR(20)                NOT NULL,
    part_nm                            VARCHAR(100)               NOT NULL,
    part_category                      VARCHAR(30)                NOT NULL,
    supplier_id                        INT                        NOT NULL,
    unit_cost_amt                      DECIMAL(12,2)              NOT NULL,
    currency_cd                        VARCHAR(5)                 NOT NULL,
    qty_on_hand                        INT                        NOT NULL,
    reorder_point                      INT                        NOT NULL,
    reorder_qty                        INT                        NOT NULL,
    lead_time_days                     INT                        NOT NULL,
    storage_loc_cd                     VARCHAR(20)               ,
    weight_gm                          DECIMAL(10,2)             ,
    is_critical_flag                   CHAR(1)                    NOT NULL,
    last_receipt_dt                    DATE                      ,
    expiry_dt                          DATE                      ,
    status_cd                          VARCHAR(20)                NOT NULL,
    country_of_origin                  VARCHAR(30)               ,
    tariff_code                        VARCHAR(15)               ,
    hsn_code                           VARCHAR(10)               ,
    uom_cd                             VARCHAR(10)                NOT NULL,
    created_at                         TIMESTAMP                  NOT NULL,
    internal_ref_cd                    VARCHAR(20)               ,
    remarks                            VARCHAR(200)              ,
    PRIMARY KEY (part_id)
);