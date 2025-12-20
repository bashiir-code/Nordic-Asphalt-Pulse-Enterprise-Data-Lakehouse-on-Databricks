-- 1. Create the Container (The Database)
-- This corresponds to the 'Project Initialization' epic on your Notion board.
CREATE DATABASE IF NOT EXISTS nordic_pulse_db;

-- 2. Set the Context
USE nordic_pulse_db;

-- 3. Create Bronze Traffic (The Landing Zone)
-- Strategy: Schema-on-Read. We store the full JSON string in 'raw_payload'.
CREATE TABLE IF NOT EXISTS bronze_traffic_raw (
    ingest_id STRING,           -- UUID for this specific run
    ingest_timestamp TIMESTAMP, -- When did we grab it?
    source_system STRING,       -- e.g. "digitraffic"
    raw_payload STRING          -- <--- The Full JSON Dump
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true'); 
-- Enabling Change Data Feed allows us to easily process "new rows only" later!

-- 4. Create Bronze Weather
CREATE TABLE IF NOT EXISTS bronze_weather_raw (
    ingest_id STRING,
    ingest_timestamp TIMESTAMP,
    city_name STRING,
    raw_payload STRING
)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');