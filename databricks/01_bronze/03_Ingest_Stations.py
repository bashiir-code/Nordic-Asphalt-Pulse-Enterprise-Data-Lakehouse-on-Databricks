# -------------------------------------------------------------------------
# 🧱 BRONZE INGESTION: STATIONS (Catalog: workspace)
# -------------------------------------------------------------------------
import requests
import json
import uuid
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

# --- 1. CONFIGURATION ---
# 🎯 Pointing to your 'workspace' catalog
TARGET_TABLE = "workspace.nordic_pulse_db.bronze_stations_raw"
URL = "https://tie.digitraffic.fi/api/tms/v1/stations"

def ingest_stations():
    print(f"🚀 Fetching Stations...")
    print(f"   Target: {TARGET_TABLE}")
    
    try:
        response = requests.get(URL)
        response.raise_for_status()
        raw_content = response.text
        
        # 🕵️‍♂️ INSPECTOR
        print("\n🔎 INSPECTING STATION JSON (First Feature):")
        try:
            parsed = json.loads(raw_content)
            first_feature = parsed.get("features", [])[0]
            print(json.dumps(first_feature, indent=2))
        except Exception as e:
            print(f"⚠️ Inspection skipped: {e}")
        print("-" * 50)
        
        # Save
        row_data = [{
            "ingest_id": str(uuid.uuid4()),
            "source_system": "digitraffic_stations_v1",
            "raw_payload": raw_content
        }]
        
        schema = StructType([
            StructField("ingest_id", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("raw_payload", StringType(), True)
        ])
        
        df = spark.createDataFrame(row_data, schema)
        df.withColumn("ingest_timestamp", current_timestamp()) \
          .write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
            
        print(f"\n✅ Success! Stations data saved to {TARGET_TABLE}")
        
    except Exception as e:
        print(f"❌ Error: {e}")

# --- 2. EXECUTE ---
ingest_stations()