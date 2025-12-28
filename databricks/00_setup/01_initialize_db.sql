-- -------------------------------------------------------------------------
-- 00_SETUP: INITIALIZE DATABASE
-- -------------------------------------------------------------------------

-- 1. Create the Catalog (If you are using Unity Catalog)
-- In Community Edition, 'workspace' is often the default, but this ensures it exists.
CREATE CATALOG IF NOT EXISTS workspace;

-- 2. Switch to that Catalog
USE CATALOG workspace;

-- 3. Create the Schema (The "Container" for your tables)
CREATE SCHEMA IF NOT EXISTS nordic_pulse_db;

-- 4. Set it as default so you don't have to type 'workspace.nordic_pulse_db' every time
USE SCHEMA nordic_pulse_db;

-- 5. Verification
SELECT current_catalog(), current_schema();