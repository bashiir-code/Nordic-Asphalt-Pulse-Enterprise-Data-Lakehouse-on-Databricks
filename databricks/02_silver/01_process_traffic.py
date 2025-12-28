# -------------------------------------------------------------------------
# 🥈 SILVER LAYER: TRAFFIC DATA (Parse, Explode, & Select)
# -------------------------------------------------------------------------
from pyspark.sql.functions import from_json, col, explode, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType, LongType

# --- 1. CONFIGURATION ---
SOURCE_TABLE = "workspace.nordic_pulse_db.bronze_traffic_raw"
TARGET_TABLE = "workspace.nordic_pulse_db.silver_traffic"

# --- 2. DEFINE THE MAP (Schema) ---
# We tell Spark exactly what the JSON looks like so it can parse it efficiently.

sensor_schema = StructType([
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),         # e.g., "KESKINOPEUS_60MIN_..."
    StructField("shortName", StringType(), True),    # e.g., "km/h"
    StructField("value", DoubleType(), True),        # e.g., 85.4
    StructField("measuredTime", StringType(), True)  # Timestamp as string
])

station_schema = StructType([
    StructField("id", LongType(), True),             # Station ID
    StructField("sensorValues", ArrayType(sensor_schema), True) # Nested List
])

json_schema = StructType([
    StructField("stations", ArrayType(station_schema), True)
])

# --- 3. PROCESSING LOGIC ---
print(f"🚀 Processing Bronze -> Silver...")

# A. Read Bronze
df_bronze = spark.read.table(SOURCE_TABLE)

# B. Parse the JSON String
print("   ...Parsing JSON structure")
df_parsed = df_bronze.withColumn("jsonData", from_json(col("raw_payload"), json_schema))

# C. EXPLODE 1: Stations (1 Batch -> Many Stations)
df_stations = df_parsed.select(
    col("ingest_timestamp"),
    explode(col("jsonData.stations")).alias("station")
)

# D. EXPLODE 2: Sensors (1 Station -> Many Sensors)
df_sensors = df_stations.select(
    col("ingest_timestamp"),
    col("station.id").alias("station_id"),
    explode(col("station.sensorValues")).alias("sensor")
)

# E. SELECT & RENAME (The Business Logic)
# We flatten the structure into a clean table format.
df_silver = df_sensors.select(
    col("ingest_timestamp"),
    col("station_id"),
    col("sensor.name").alias("sensor_name"),
    col("sensor.value").alias("sensor_value"),
    col("sensor.shortName").alias("unit"),
    # Convert string time to real Timestamp
    to_timestamp(col("sensor.measuredTime")).alias("measured_time") 
)

# --- 4. DATA QUALITY CHECK (Peek at the result) ---
print("   ...Previewing Data (First 5 rows):")
df_silver.show(5, truncate=False)

# --- 5. WRITE TO SILVER TABLE ---
df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"✅ Success! Transformed data saved to {TARGET_TABLE}")