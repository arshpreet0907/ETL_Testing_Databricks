-- ============================================================
-- Target table : fact_sales
-- Dialect      : Snowflake
-- Generated    : 2026-04-19 03:34
-- ============================================================

CREATE TABLE IF NOT EXISTS fact_sales (
    sales_order_key                      NUMBER(19,0)               NOT NULL,  -- PK
    vehicle_key                          NUMBER(19,0)               NOT NULL,  -- FK to dim_vehicle
    dealer_key                           NUMBER(19,0)               NOT NULL,
    customer_key                         NUMBER(19,0)               NOT NULL,
    order_date                           DATE                       NOT NULL,
    delivery_date                        DATE                      ,
    invoice_number                       VARCHAR(20)               ,
    invoice_date                         DATE                      ,
    sale_price                           FLOAT                      NOT NULL,
    discount_percent                     FLOAT                      NOT NULL DEFAULT 0,
    discount_amt                         FLOAT                      NOT NULL DEFAULT 0,  -- Absolute discount value
    tax_amount                           FLOAT                      NOT NULL,
    insurance_amount                     FLOAT                      NOT NULL DEFAULT 0,
    accessories_amount                   FLOAT                      NOT NULL DEFAULT 0,
    total_invoice                        FLOAT                      NOT NULL,
    payment_mode                         VARCHAR(20)                NOT NULL,
    finance_bank                         VARCHAR(50)               ,
    vin_number                           VARCHAR(17)               ,
    region                               VARCHAR(10)                NOT NULL,
    sales_rep_key                        NUMBER(19,0)              ,  -- FK to dim_employee
    order_status                         VARCHAR(20)                NOT NULL,
    cancellation_reason                  VARCHAR(50)               ,
    sales_channel                        VARCHAR(20)                NOT NULL,
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- constant: CURRENT_TIMESTAMP
    vehicle_vin                          VARCHAR(17)               ,  -- VIN on this sale
    vehicle_model                        VARCHAR(50)               ,  -- Vehicle model name
    vehicle_variant                      VARCHAR(20)               ,  -- Vehicle variant
    PRIMARY KEY (sales_order_key)
);