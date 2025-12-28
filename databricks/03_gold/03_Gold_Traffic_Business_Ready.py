# -------------------------------------------------------------------------
# 📊 ANALYTICS LAYER: Dashboard Ready (50-City Enabled)
# -------------------------------------------------------------------------
TARGET_TABLE = "workspace.nordic_pulse_db.gold_analytics_ready"

print("🚀 Applying Analyst Logic (With 50-Region Support)...")

analytics_query = """
SELECT 
    measured_time,
    date_trunc('hour', measured_time) as match_hour,
    
    station_id,
    region_name, -- 🌍 CRITICAL: This enables filtering by the 50 Cities!
    latitude, 
    longitude,
    
    -- Metrics
    avg_speed_kmh,
    total_volume_cars,
    temperature,
    snowfall,
    wind_speed,

    -- 🏷️ TAG 1: TRAFFIC STATUS
    CASE 
        WHEN avg_speed_kmh < 60 THEN 'CONGESTED'
        WHEN avg_speed_kmh BETWEEN 60 AND 85 THEN 'SLOW'
        ELSE 'FREE_FLOW'
    END AS traffic_status,

    -- 🏷️ TAG 2: ROAD CONDITION
    CASE 
        WHEN snowfall > 0 THEN 'SNOWY'
        WHEN temperature < 0 AND snowfall = 0 THEN 'ICY_COLD'
        WHEN temperature >= 0 AND snowfall = 0 THEN 'DRY_NORMAL'
        ELSE 'UNKNOWN'
    END AS road_condition,

    -- 🏷️ TAG 3: WIND WARNING
    CASE 
        WHEN wind_speed > 15 THEN 'HIGH_WIND'
        ELSE 'NORMAL_WIND'
    END AS wind_warning,

    -- 🧠 TAG 4: ROOT CAUSE (The "Why")
    CASE 
        WHEN avg_speed_kmh < 60 AND snowfall > 0 THEN 'WEATHER_IMPACT'
        WHEN avg_speed_kmh < 60 AND total_volume_cars > 800 THEN 'HIGH_VOLUME'
        WHEN avg_speed_kmh < 60 THEN 'UNKNOWN_CONGESTION'
        ELSE 'NORMAL'
    END AS probable_cause

FROM workspace.nordic_pulse_db.gold_traffic_enriched

WHERE 
    avg_speed_kmh > 0 
    AND avg_speed_kmh < 150
"""

# Execute
df_analytics = spark.sql(analytics_query)

# Check results (Verify 'region_name' is visible)
print("   ...Previewing Dashboard Data:")
df_analytics.select("measured_time", "region_name", "traffic_status", "probable_cause").show(10, truncate=False)

# Save
df_analytics.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"✅ Success! 50-City Analytics data saved to {TARGET_TABLE}")