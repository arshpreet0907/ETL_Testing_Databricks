-- ============================================================
-- Source table : employee_master
-- Dialect      : MySQL 8+
-- Generated    : 2026-04-19 00:43
-- ============================================================

CREATE TABLE IF NOT EXISTS employee_master (
    emp_id                             INT                        NOT NULL,  -- PK
    emp_code                           VARCHAR(20)                NOT NULL,
    first_nm                           VARCHAR(50)                NOT NULL,
    last_nm                            VARCHAR(50)                NOT NULL,
    dob_dt                             DATE                      ,
    gender_cd                          CHAR(1)                   ,
    join_dt                            DATE                       NOT NULL,
    dept_nm                            VARCHAR(50)                NOT NULL,
    role_nm                            VARCHAR(50)                NOT NULL,
    grade_cd                           CHAR(2)                    NOT NULL,
    plant_cd                           VARCHAR(10)                NOT NULL,
    shift_cd                           VARCHAR(15)                NOT NULL,
    basic_salary_amt                   DECIMAL(12,2)              NOT NULL,
    hra_amt                            DECIMAL(10,2)              NOT NULL,
    pf_pct                             DECIMAL(4,1)               NOT NULL,
    status_cd                          VARCHAR(20)                NOT NULL,
    mgr_emp_id                         INT                       ,
    created_at                         TIMESTAMP                  NOT NULL,
    updated_at                         TIMESTAMP                  NOT NULL,
    created_by                         VARCHAR(30)               ,
    pan_no                             VARCHAR(10)               ,
    aadhaar_no                         VARCHAR(12)               ,
    phone_no                           VARCHAR(15)               ,
    remarks                            VARCHAR(200)              ,
    PRIMARY KEY (emp_id)
);