# -------------------------------------------------------------------------
# 🥈 SILVER: STATIONS (Schema Update with Municipality)
# -------------------------------------------------------------------------
from pyspark.sql.functions import col, from_json, explode, get_json_object, trim
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

SOURCE_TABLE = "workspace.nordic_pulse_db.bronze_stations_raw"
TARGET_TABLE = "workspace.nordic_pulse_db.silver_stations"

print(f"🚀 Updating Silver Stations Schema...")

df_bronze = spark.read.table(SOURCE_TABLE)
json_schema = StructType([StructField("features", ArrayType(StringType()), True)])

# 1. Parse & Explode
df_exploded = df_bronze.select(from_json(col("raw_payload"), json_schema).alias("d")) \
                       .select(explode(col("d.features")).alias("feature"))

# 2. Extract New Columns (Name & Municipality)
df_stations = df_exploded.select(
    get_json_object(col("feature"), "$.id").cast("long").alias("station_id"),
    get_json_object(col("feature"), "$.geometry.coordinates[0]").cast("double").alias("longitude"),
    get_json_object(col("feature"), "$.geometry.coordinates[1]").cast("double").alias("latitude")
).filter(
    col("latitude").between(59, 72) & col("longitude").between(19, 33)
)

# 3. FORCE SAVE (Overwrite Schema)
df_stations.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(TARGET_TABLE)

print(f"✅ Success! Schema updated. Columns are now: {df_stations.columns}")