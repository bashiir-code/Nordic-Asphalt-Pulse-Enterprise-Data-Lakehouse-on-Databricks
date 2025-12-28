# -------------------------------------------------------------------------
# 🥇 GOLD: TRAFFIC ENRICHED (Latest Snapshot Fix)
# -------------------------------------------------------------------------
TARGET_TABLE = "workspace.nordic_pulse_db.gold_traffic_enriched"

gold_query = """
WITH LatestIngest AS (
    -- 1. Find the timestamp of the MOST RECENT weather update
    SELECT MAX(ingest_timestamp) as latest_ts FROM workspace.nordic_pulse_db.silver_weather
),

WeatherSnapshot AS (
    -- 2. Get Weather ONLY from that latest batch
    -- We do NOT use AVG() anymore. We take the raw real values.
    SELECT 
        w.city_name,
        w.temperature_c as temp,
        w.snowfall_cm as snow,
        w.weather_code as code,
        w.wind_speed_kmh as wind
    FROM workspace.nordic_pulse_db.silver_weather w
    INNER JOIN LatestIngest l ON w.ingest_timestamp = l.latest_ts
),

StationDistances AS (
    -- 3. CALCULATE DISTANCE (Station -> 50 Cities)
    SELECT 
        s.station_id,
        loc.city_name,
        SQRT(POW(s.latitude - loc.w_lat, 2) + POW(s.longitude - loc.w_lon, 2)) as dist_score
    FROM workspace.nordic_pulse_db.silver_stations s
    CROSS JOIN workspace.nordic_pulse_db.dim_weather_locations loc
),

NearestCity AS (
    -- 4. RANK & PICK NEAREST
    SELECT station_id, city_name
    FROM (
        SELECT 
            station_id, 
            city_name, 
            ROW_NUMBER() OVER(PARTITION BY station_id ORDER BY dist_score ASC) as rank
        FROM StationDistances
    ) 
    WHERE rank = 1
)

-- 5. FINAL ASSEMBLY
SELECT 
    t.measured_time,
    t.station_id,
    nc.city_name as region_name, 
    s.latitude, 
    s.longitude,
    
    -- Traffic
    CAST(AVG(CASE WHEN t.sensor_name LIKE '%KESKINOPEUS%' THEN t.sensor_value END) AS DECIMAL(10,1)) as avg_speed_kmh,
    CAST(SUM(CASE WHEN t.sensor_name LIKE '%OHITUKSET%' THEN t.sensor_value END) AS LONG) as total_volume_cars,

    -- Weather (Smart Join)
    nc.city_name as weather_source, 
    -- ⚠️ IMPORTANT: We removed the COALESCE(x, 0) for temperature.
    -- If data is missing, let it be NULL. Defaulting to 0°C is dangerous in winter!
    w.temp as temperature,
    COALESCE(w.snow, 0) as snowfall,
    COALESCE(w.wind, 0) as wind_speed,
    COALESCE(w.code, 0) as weather_code

FROM workspace.nordic_pulse_db.silver_traffic t
JOIN workspace.nordic_pulse_db.silver_stations s ON t.station_id = s.station_id
JOIN NearestCity nc ON t.station_id = nc.station_id
LEFT JOIN WeatherSnapshot w ON nc.city_name = w.city_name

WHERE 
    t.sensor_name LIKE '%60MIN%' 
    AND (t.sensor_name LIKE '%KESKINOPEUS%' OR t.sensor_name LIKE '%OHITUKSET%')

GROUP BY 
    t.measured_time, t.station_id, nc.city_name, s.latitude, s.longitude,
    w.temp, w.snow, w.wind, w.code
"""

# Execute
df_gold = spark.sql(gold_query)

print("🔍 Debugging Temperature Extremes:")
# Let's see the REAL difference now
df_gold.select("region_name", "temperature").filter("region_name IN ('Helsinki', 'Rovaniemi', 'Oulu')").distinct().show()

# Save
df_gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(TARGET_TABLE)