-- Target schema for the assignment (PostgreSQL syntax; adapt to SQLite/DuckDB/Spark if you prefer).
-- You may add columns, indexes or tables if your design needs them. Explain any change in the README.

CREATE TABLE employee (
    source_system        VARCHAR(10)  NOT NULL,   -- 'A' or 'B'
    source_employee_id   VARCHAR(20)  NOT NULL,
    full_name            VARCHAR(200) NOT NULL,
    email                VARCHAR(200),
    hire_date            DATE         NOT NULL,
    grade                VARCHAR(5)   NOT NULL,   -- G1 .. G8
    department           VARCHAR(100),
    location             VARCHAR(100),
    country              VARCHAR(100),
    gender               VARCHAR(1),              -- 'M' / 'F'
    manager_employee_id  VARCHAR(20),             -- source id of the manager, same source system
    employment_status    VARCHAR(20)  NOT NULL,   -- 'Active' / 'Terminated'
    first_loaded_at      TIMESTAMP    NOT NULL,
    last_updated_at      TIMESTAMP    NOT NULL,
    PRIMARY KEY (source_system, source_employee_id)
);

CREATE TABLE salary (
    source_system        VARCHAR(10)   NOT NULL,
    source_employee_id   VARCHAR(20)   NOT NULL,
    effective_date       DATE          NOT NULL,  -- date this salary became current in our system
    annual_base_local    NUMERIC(18,2) NOT NULL,  -- always ANNUAL, in local currency
    currency             VARCHAR(3)    NOT NULL,
    annual_base_usd      NUMERIC(18,2) NOT NULL,
    is_current           BOOLEAN       NOT NULL,
    PRIMARY KEY (source_system, source_employee_id, effective_date)
);

CREATE TABLE load_run (
    run_id               VARCHAR(40)  PRIMARY KEY,
    source_file          VARCHAR(100) NOT NULL,
    started_at           TIMESTAMP    NOT NULL,
    finished_at          TIMESTAMP,
    rows_expected        INTEGER,                 -- from manifest.csv
    rows_read            INTEGER,
    rows_loaded          INTEGER,
    rows_rejected        INTEGER,
    status               VARCHAR(20)              -- your choice of values
);

CREATE TABLE reject (
    run_id               VARCHAR(40)  NOT NULL,
    source_file          VARCHAR(100) NOT NULL,
    source_row_number    INTEGER,
    source_employee_id   VARCHAR(20),
    reason               VARCHAR(200) NOT NULL
);
