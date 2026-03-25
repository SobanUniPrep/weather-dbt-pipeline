-- ============================================================
-- WEATHER PIPELINE - SNOWFLAKE SETUP
-- ============================================================

-- ------------------------------------------------------------
-- WAREHOUSE
-- ------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS WEATHER_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for weather pipeline';

-- ------------------------------------------------------------
-- DATABASE
-- ------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS WEATHER_DB
    COMMENT = 'Main database for weather pipeline';

-- ------------------------------------------------------------
-- SCHEMATA
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS WEATHER_DB.RAW
    COMMENT = 'Raw ingested data from Open-Meteo API';

CREATE SCHEMA IF NOT EXISTS WEATHER_DB.STAGING
    COMMENT = 'Cleaned and typed staging models';

CREATE SCHEMA IF NOT EXISTS WEATHER_DB.MARTS
    COMMENT = 'Business-ready mart models';

CREATE SCHEMA IF NOT EXISTS WEATHER_DB.VAULT
    COMMENT = 'Data Vault - hubs, links, satellites';

-- ------------------------------------------------------------
-- ROLES
-- ------------------------------------------------------------
CREATE ROLE IF NOT EXISTS WEATHER_ADMIN;
CREATE ROLE IF NOT EXISTS WEATHER_DBT;
CREATE ROLE IF NOT EXISTS WEATHER_ANALYST;

-- Role hierarchy
GRANT ROLE WEATHER_DBT TO ROLE WEATHER_ADMIN;
GRANT ROLE WEATHER_ANALYST TO ROLE WEATHER_ADMIN;

-- ------------------------------------------------------------
-- GRANTS - WAREHOUSE
-- ------------------------------------------------------------
GRANT USAGE ON WAREHOUSE WEATHER_WH TO ROLE WEATHER_ADMIN;
GRANT USAGE ON WAREHOUSE WEATHER_WH TO ROLE WEATHER_DBT;
GRANT USAGE ON WAREHOUSE WEATHER_WH TO ROLE WEATHER_ANALYST;

-- ------------------------------------------------------------
-- GRANTS - DATABASE
-- ------------------------------------------------------------
GRANT ALL ON DATABASE WEATHER_DB TO ROLE WEATHER_ADMIN;
GRANT USAGE ON DATABASE WEATHER_DB TO ROLE WEATHER_DBT;
GRANT USAGE ON DATABASE WEATHER_DB TO ROLE WEATHER_ANALYST;

-- ------------------------------------------------------------
-- GRANTS - SCHEMATA
-- ------------------------------------------------------------
GRANT ALL ON SCHEMA WEATHER_DB.RAW TO ROLE WEATHER_ADMIN;
GRANT ALL ON SCHEMA WEATHER_DB.RAW TO ROLE WEATHER_DBT;

GRANT ALL ON SCHEMA WEATHER_DB.STAGING TO ROLE WEATHER_DBT;
GRANT USAGE ON SCHEMA WEATHER_DB.STAGING TO ROLE WEATHER_ANALYST;

GRANT ALL ON SCHEMA WEATHER_DB.MARTS TO ROLE WEATHER_DBT;
GRANT USAGE ON SCHEMA WEATHER_DB.MARTS TO ROLE WEATHER_ANALYST;

GRANT ALL ON SCHEMA WEATHER_DB.VAULT TO ROLE WEATHER_DBT;
GRANT USAGE ON SCHEMA WEATHER_DB.VAULT TO ROLE WEATHER_ANALYST;

-- ------------------------------------------------------------
-- GRANTS - TABLES (future)
-- ------------------------------------------------------------
GRANT SELECT ON FUTURE TABLES IN SCHEMA WEATHER_DB.STAGING TO ROLE WEATHER_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA WEATHER_DB.MARTS TO ROLE WEATHER_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA WEATHER_DB.VAULT TO ROLE WEATHER_ANALYST;

-- ------------------------------------------------------------
-- ZERO-COPY CLONE - DEV
-- ------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS WEATHER_DB_DEV
    CLONE WEATHER_DB
    COMMENT = 'Dev clone of WEATHER_DB';

-- ------------------------------------------------------------
-- STREAM
-- ------------------------------------------------------------
CREATE STREAM IF NOT EXISTS WEATHER_DB.RAW.STREAM_RAW_WEATHER
    ON TABLE WEATHER_DB.RAW.RAW_WEATHER
    APPEND_ONLY = TRUE
    COMMENT = 'Captures new rows in RAW_WEATHER for downstream processing';

-- ------------------------------------------------------------
-- TASK
-- ------------------------------------------------------------
CREATE TASK IF NOT EXISTS WEATHER_DB.RAW.TASK_TRANSFORM_WEATHER
    WAREHOUSE = WEATHER_WH
    SCHEDULE = '60 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('WEATHER_DB.RAW.STREAM_RAW_WEATHER')
AS
    SELECT 'Airflow is primary orchestrator - this task is monitoring fallback';

-- Grant task ownership to admin
GRANT ALL ON TASK WEATHER_DB.RAW.TASK_TRANSFORM_WEATHER TO ROLE WEATHER_ADMIN;