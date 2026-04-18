-- ============================================================
-- Source table : production_orders
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-19 01:14
-- ============================================================

CREATE TABLE IF NOT EXISTS production_orders (
    prod_order_id                      INT                        NOT NULL,  -- PK
    vehicle_id                         INT                        NOT NULL,
    plant_cd                           VARCHAR(10)                NOT NULL,
    order_dt                           DATE                       NOT NULL,
    planned_start_dt                   DATE                       NOT NULL,
    actual_start_dt                    DATE                      ,
    planned_end_dt                     DATE                       NOT NULL,
    actual_end_dt                      DATE                      ,
    qty_planned                        INT                        NOT NULL,
    qty_produced                       INT                        NOT NULL,
    qty_rejected                       INT                        NOT NULL,
    shift_cd                           VARCHAR(15)                NOT NULL,
    line_no                            INT                        NOT NULL,
    supervisor_emp_id                  INT                       ,
    order_status_cd                    VARCHAR(20)                NOT NULL,
    priority_lvl                       INT                        NOT NULL,
    downtime_mins                      INT                        NOT NULL,
    scrap_cost_amt                     DECIMAL(12,2)              NOT NULL,
    rework_hrs                         DECIMAL(6,2)               NOT NULL,
    efficiency_pct                     DECIMAL(5,2)              ,
    target_takt_secs                   INT                        NOT NULL,
    actual_takt_secs                   INT                        NOT NULL,
    created_at                         TIMESTAMP                  NOT NULL,
    updated_at                         TIMESTAMP                  NOT NULL,
    batch_flag                         CHAR(1)                   ,
    PRIMARY KEY (prod_order_id)
);