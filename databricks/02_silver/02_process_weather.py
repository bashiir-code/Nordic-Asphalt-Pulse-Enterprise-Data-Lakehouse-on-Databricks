# -------------------------------------------------------------------------
# 🥈 SILVER LAYER: WEATHER DATA (Parse & Select)
# -------------------------------------------------------------------------
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

# --- 1. CONFIGURATION ---
SOURCE_TABLE = "workspace.nordic_pulse_db.bronze_weather_raw"
TARGET_TABLE = "workspace.nordic_pulse_db.silver_weather"

# --- 2. DEFINE THE MAP (Schema) ---
# We only care about the "current" object inside the JSON
current_weather_schema = StructType([
    StructField("time", StringType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("wind_speed_10m", DoubleType(), True),
    StructField("snowfall", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("weather_code", LongType(), True)
])

# The root JSON has "current" inside it
json_schema = StructType([
    StructField("current", current_weather_schema, True)
])

print(f"🚀 Processing Bronze Weather -> Silver...")

# --- 3. PROCESSING LOGIC ---
df_bronze = spark.read.table(SOURCE_TABLE)

# A. Parse the JSON
df_parsed = df_bronze.withColumn("jsonData", from_json(col("raw_payload"), json_schema))

# B. Select & Rename (Business Logic Applied Here)
df_silver = df_parsed.select(
    col("ingest_timestamp"),
    col("city_name"),  # Keep the city name!
    
    # Extract details from the parsed JSON
    to_timestamp(col("jsonData.current.time")).alias("forecast_time"),
    col("jsonData.current.temperature_2m").alias("temperature_c"),
    col("jsonData.current.wind_speed_10m").alias("wind_speed_kmh"),
    col("jsonData.current.snowfall").alias("snowfall_cm"),
    col("jsonData.current.rain").alias("rain_mm"),
    col("jsonData.current.weather_code").alias("weather_code")
)

# --- 4. PREVIEW ---
print("   ...Previewing Data:")
df_silver.show(5,truncate = False)

# --- 5. WRITE ---
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"✅ Success! Weather data saved to {TARGET_TABLE}")