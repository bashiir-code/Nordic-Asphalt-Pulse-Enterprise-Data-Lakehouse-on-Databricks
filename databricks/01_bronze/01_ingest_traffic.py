# -------------------------------------------------------------------------
# 🧱 BRONZE INGESTION: TRAFFIC DATA
# -------------------------------------------------------------------------
import requests
import json
import uuid
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# --- 1. CONFIGURATION ---
SOURCE_URL = "https://tie.digitraffic.fi/api/tms/v1/stations/data"
TARGET_TABLE = "nordic_pulse_db.bronze_traffic_raw"

def ingest_traffic_data():
    print(f"🚀 Starting Ingestion from: {SOURCE_URL}")
    
    try:
        # A. Fetch the Data
        # We grab the entire JSON response as a single text block
        response = requests.get(SOURCE_URL)
        response.raise_for_status()
        raw_json_string = response.text
        
        print(f"📦 Data fetched successfully. Size: {len(raw_json_string)/1024:.2f} KB")

        # B. Prepare for Delta Lake
        # We wrap the huge JSON string into a list so Spark can understand it
        data_payload = [{
            "ingest_id": str(uuid.uuid4()),         # Unique ID for this batch
            "source_system": "digitraffic_tms_v1",  # Where did it come from?
            "raw_payload": raw_json_string          # The DATA (No transformation yet!)
        }]
        
        # C. Define Schema
        # We force the schema to ensure 'raw_payload' is treated as a simple String
        schema = StructType([
            StructField("ingest_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("raw_payload", StringType(), True)
        ])
        
        # D. Create DataFrame
        df = spark.createDataFrame(data_payload, schema)
        
        # Add the 'ingest_timestamp' automatically (Server Time)
        df_final = df.withColumn("ingest_timestamp", current_timestamp())

        # E. Write to Delta Table
        # mode("append") adds this new batch to the history without deleting old data
        df_final.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(TARGET_TABLE)
            
        print(f"✅ Success! Data written to {TARGET_TABLE}")

    except Exception as e:
        print(f"❌ Ingestion Failed: {e}")
        raise e

# --- 2. EXECUTE ---
ingest_traffic_data()