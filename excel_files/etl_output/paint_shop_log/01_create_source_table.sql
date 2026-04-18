-- ============================================================
-- Source table : paint_shop_log
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-19 00:43
-- ============================================================

CREATE TABLE IF NOT EXISTS paint_shop_log (
    paint_log_id                       INT                        NOT NULL,  -- PK
    prod_order_id                      INT                        NOT NULL,
    vehicle_id                         INT                        NOT NULL,
    plant_cd                           VARCHAR(10)                NOT NULL,
    paint_line_no                      INT                        NOT NULL,
    shift_cd                           VARCHAR(15)                NOT NULL,
    color_cd                           VARCHAR(20)                NOT NULL,
    color_desc                         VARCHAR(30)                NOT NULL,
    oven_temp_celsius                  DECIMAL(5,2)               NOT NULL,
    bake_duration_mins                 INT                        NOT NULL,
    paint_thickness_um                 DECIMAL(6,2)              ,
    gloss_level                        DECIMAL(5,2)              ,
    defect_flag                        CHAR(1)                    NOT NULL,
    defect_type_cd                     VARCHAR(20)               ,
    rework_flag                        CHAR(1)                    NOT NULL,
    paint_cost_amt                     DECIMAL(12,2)              NOT NULL,
    operator_emp_id                    INT                       ,
    start_ts                           TIMESTAMP                  NOT NULL,
    end_ts                             TIMESTAMP                  NOT NULL,
    humidity_pct                       DECIMAL(4,1)              ,
    created_at                         TIMESTAMP                  NOT NULL,
    primer_batch_no                    VARCHAR(20)               ,
    topcoat_batch_no                   VARCHAR(20)               ,
    clear_coat_batch_no                VARCHAR(20)               ,
    batch_id                           VARCHAR(20)               ,
    PRIMARY KEY (paint_log_id)
);