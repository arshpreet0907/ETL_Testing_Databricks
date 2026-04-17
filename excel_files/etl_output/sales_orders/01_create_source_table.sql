-- ============================================================
-- Source table : sales_orders
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-17 15:17
-- ============================================================

CREATE TABLE IF NOT EXISTS sales_orders (
    sales_order_id                     INT                        NOT NULL,  -- PK
    vehicle_id                         INT                        NOT NULL,
    dealer_id                          INT                        NOT NULL,
    customer_id                        INT                        NOT NULL,
    order_dt                           DATE                       NOT NULL,
    delivery_dt                        DATE                      ,
    invoice_no                         VARCHAR(20)               ,
    invoice_dt                         DATE                      ,
    sale_price_amt                     DECIMAL(14,2)              NOT NULL,
    discount_pct                       DECIMAL(5,2)               NOT NULL,
    tax_amt                            DECIMAL(12,2)              NOT NULL,
    insurance_amt                      DECIMAL(10,2)              NOT NULL,
    accessories_amt                    DECIMAL(10,2)              NOT NULL,
    total_invoice_amt                  DECIMAL(14,2)              NOT NULL,
    payment_mode_cd                    VARCHAR(20)                NOT NULL,
    finance_bank_nm                    VARCHAR(50)               ,
    vin_allocated                      VARCHAR(17)               ,
    region_cd                          VARCHAR(10)                NOT NULL,
    sales_rep_emp_id                   INT                       ,
    order_status_cd                    VARCHAR(20)                NOT NULL,
    cancel_reason_cd                   VARCHAR(50)               ,
    source_channel_cd                  VARCHAR(20)                NOT NULL,
    created_at                         TIMESTAMP                  NOT NULL,
    internal_ref_no                    VARCHAR(20)               ,
    PRIMARY KEY (sales_order_id)
);