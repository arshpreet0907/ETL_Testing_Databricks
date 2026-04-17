-- ============================================================
-- Target table : dim_employee
-- Dialect      : Snowflake
-- Generated    : 2026-04-17 15:17
-- ============================================================

CREATE TABLE IF NOT EXISTS dim_employee (
    employee_key                         NUMBER(19,0)               NOT NULL,  -- PK
    employee_code                        VARCHAR(20)                NOT NULL,
    first_name                           VARCHAR(50)                NOT NULL,
    last_name                            VARCHAR(50)                NOT NULL,
    full_name                            VARCHAR(100)               NOT NULL,  -- Concatenate first + last
    date_of_birth                        DATE                      ,
    gender                               CHAR(1)                   ,
    joining_date                         DATE                       NOT NULL,
    department                           VARCHAR(50)                NOT NULL,
    job_role                             VARCHAR(50)                NOT NULL,
    grade                                CHAR(2)                    NOT NULL,
    plant_code                           VARCHAR(10)                NOT NULL,
    shift_name                           VARCHAR(15)                NOT NULL,
    basic_salary                         FLOAT                      NOT NULL,
    hra                                  FLOAT                      NOT NULL,
    gross_salary                         FLOAT                      NOT NULL,  -- Basic + HRA
    pf_percent                           FLOAT                      NOT NULL,
    employee_status                      VARCHAR(20)                NOT NULL,
    manager_key                          NUMBER(19,0)              ,  -- Self-ref FK
    created_at                           TIMESTAMP_NTZ              NOT NULL,
    updated_at                           TIMESTAMP_NTZ              NOT NULL,
    created_by                           VARCHAR(30)               ,
    load_ts                              TIMESTAMP_NTZ              NOT NULL,  -- constant: CURRENT_TIMESTAMP
    manager_name                         VARCHAR(100)              ,  -- Manager full name (self-join)
    PRIMARY KEY (employee_key)
);