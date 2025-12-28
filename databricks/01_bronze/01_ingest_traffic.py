# -------------------------------------------------------------------------
# 🧱 BRONZE INGESTION: TRAFFIC DATA (Catalog: workspace)
# -------------------------------------------------------------------------
import requests
import json
import uuid
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# --- 1. CONFIGURATION ---
# 🎯 We point exactly to your 'workspace' catalog
TARGET_TABLE = "workspace.nordic_pulse_db.bronze_traffic_raw"
SOURCE_URL = "https://tie.digitraffic.fi/api/tms/v1/stations/data"

# 🎛️ CONTROL PANEL
# True = Overwrite (Limits DB size to 1 batch for testing)
DEV_MODE = True  

def ingest_traffic_data():
    print(f"🚀 Starting Traffic Ingestion (Dev Mode: {DEV_MODE})...")
    print(f"   Target: {TARGET_TABLE}")
    
    try:
        # A. Fetch Data
        response = requests.get(SOURCE_URL)
        response.raise_for_status()
        raw_json_string = response.text
        
        # B. 🕵️‍♂️ DATA INSPECTOR (Quality Check)
        print("\n🔎 INSPECTING RAW JSON (First Station Only):")
        try:
            parsed = json.loads(raw_json_string)
            # Peek at the first station to confirm the data looks right
            first_station = parsed.get("stations", [])[0]
            print(json.dumps(first_station, indent=2))
        except Exception as e:
            print(f"⚠️ Inspection skipped: {e}")
        print("-" * 50)

        # C. Prepare DataFrame
        data_payload = [{
            "ingest_id": str(uuid.uuid4()),
            "source_system": "digitraffic_tms_v1",
            "raw_payload": raw_json_string
        }]
        
        schema = StructType([
            StructField("ingest_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("raw_payload", StringType(), True)
        ])
        
        df = spark.createDataFrame(data_payload, schema)
        df_final = df.withColumn("ingest_timestamp", current_timestamp())

        # D. Save to Unity Catalog
        # Since the table doesn't exist yet, this will CREATE it automatically.
        save_mode = "overwrite" if DEV_MODE else "append"
        
        df_final.write \
            .format("delta") \
            .mode(save_mode) \
            .saveAsTable(TARGET_TABLE)
            
        print(f"\n✅ Success! Table created and data saved to {TARGET_TABLE}")

    except Exception as e:
        print(f"❌ Ingestion Failed: {e}")

# --- 2. EXECUTE ---
ingest_traffic_data()