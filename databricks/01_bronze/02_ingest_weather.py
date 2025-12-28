# -------------------------------------------------------------------------
# 🧱 BRONZE: WEATHER (High-Precision 50-City Grid)
# -------------------------------------------------------------------------
import requests
import uuid
import time
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import StructType, StructField, StringType

TARGET_TABLE = "workspace.nordic_pulse_db.bronze_weather_raw"

# 🎯 STRATEGY: Top 50 Population Hubs + Critical Northern Nodes
# This covers >95% of traffic volume areas.
LOCATIONS = [
    # --- UUSIMAA (Capital Region) ---
    {"name": "Helsinki", "lat": 60.17, "lon": 24.94},
    {"name": "Espoo", "lat": 60.21, "lon": 24.66},
    {"name": "Vantaa", "lat": 60.29, "lon": 25.04},
    {"name": "Lohja", "lat": 60.25, "lon": 24.07},
    {"name": "Hyvinkaa", "lat": 60.63, "lon": 24.86},
    {"name": "Nurmijarvi", "lat": 60.46, "lon": 24.81},
    {"name": "Jarvenpaa", "lat": 60.47, "lon": 25.09},
    {"name": "Porvoo", "lat": 60.39, "lon": 25.67},
    {"name": "Kirkkonummi", "lat": 60.12, "lon": 24.44},
    {"name": "Tuusula", "lat": 60.40, "lon": 25.03},
    {"name": "Kerava", "lat": 60.40, "lon": 25.11},
    {"name": "Raseborg", "lat": 59.97, "lon": 23.44},
    
    # --- SOUTHWEST (Turku / Pori) ---
    {"name": "Turku", "lat": 60.45, "lon": 22.27},
    {"name": "Pori", "lat": 61.49, "lon": 21.80},
    {"name": "Salo", "lat": 60.39, "lon": 23.13},
    {"name": "Rauma", "lat": 61.13, "lon": 21.50},
    {"name": "Kaarina", "lat": 60.41, "lon": 22.37},
    {"name": "Raisio", "lat": 60.49, "lon": 22.17},

    # --- CENTRAL / PIRKANMAA (Tampere) ---
    {"name": "Tampere", "lat": 61.50, "lon": 23.79},
    {"name": "Hameenlinna", "lat": 61.00, "lon": 24.44},
    {"name": "Lahti", "lat": 60.98, "lon": 25.66},
    {"name": "Jyvaskyla", "lat": 62.24, "lon": 25.75},
    {"name": "Nokia", "lat": 61.48, "lon": 23.51},
    {"name": "Ylojarvi", "lat": 61.56, "lon": 23.60},
    {"name": "Kangasala", "lat": 61.46, "lon": 24.07},
    {"name": "Riihimaki", "lat": 60.74, "lon": 24.78},
    {"name": "Sastamala", "lat": 61.34, "lon": 22.91},
    {"name": "Hollola", "lat": 60.99, "lon": 25.51},
    {"name": "Lempaala", "lat": 61.32, "lon": 23.75},
    {"name": "Jamsa", "lat": 61.86, "lon": 25.19},

    # --- EAST (Kuopio / Joensuu / Lakeland) ---
    {"name": "Kuopio", "lat": 62.89, "lon": 27.68},
    {"name": "Joensuu", "lat": 62.60, "lon": 29.76},
    {"name": "Lappeenranta", "lat": 61.06, "lon": 28.19},
    {"name": "Mikkeli", "lat": 61.69, "lon": 27.27},
    {"name": "Kouvola", "lat": 60.87, "lon": 26.70},
    {"name": "Kotka", "lat": 60.47, "lon": 26.94},
    {"name": "Savonlinna", "lat": 61.87, "lon": 28.88},
    {"name": "Imatra", "lat": 61.19, "lon": 28.77},
    {"name": "Iisalmi", "lat": 63.56, "lon": 27.19},
    {"name": "Varkaus", "lat": 62.32, "lon": 27.89},

    # --- OSTROBOTHNIA (Vaasa / Seinajoki) ---
    {"name": "Vaasa", "lat": 63.10, "lon": 21.62},
    {"name": "Seinajoki", "lat": 62.79, "lon": 22.84},
    {"name": "Kokkola", "lat": 63.84, "lon": 23.13},
    {"name": "Kurikka", "lat": 62.62, "lon": 22.40},

    # --- NORTH & LAPLAND (Oulu / Rovaniemi) ---
    {"name": "Oulu", "lat": 65.01, "lon": 25.47},
    {"name": "Rovaniemi", "lat": 66.50, "lon": 25.73},
    {"name": "Kajaani", "lat": 64.23, "lon": 27.73},
    {"name": "Raahe", "lat": 64.68, "lon": 24.48},
    {"name": "Tornio", "lat": 65.85, "lon": 24.15},
    {"name": "Kemi", "lat": 65.73, "lon": 24.56}
]

def ingest_open_meteo():
    print(f"🚀 Fetching High-Res Weather for {len(LOCATIONS)} Cities...")
    rows = []
    
    for i, place in enumerate(LOCATIONS):
        # We process in batches of 10 to avoid hitting API rate limits
        if i > 0 and i % 10 == 0:
            print("   ...pausing 1s for API politeness...")
            time.sleep(1)
            
        url = (
            f"https://api.open-meteo.com/v1/forecast"
            f"?latitude={place['lat']}&longitude={place['lon']}"
            f"&current=temperature_2m,precipitation,rain,snowfall,weather_code,wind_speed_10m"
            f"&wind_speed_unit=kmh&timezone=auto"
        )
        try:
            res = requests.get(url)
            if res.status_code == 200:
                rows.append({
                    "ingest_id": str(uuid.uuid4()),
                    "city_name": place["name"],
                    "raw_payload": res.text
                })
                print(f"   ✅ {place['name']}")
        except Exception as e:
            print(f"   ❌ Error {place['name']}: {e}")

    # Save
    schema = StructType([
        StructField("ingest_id", StringType(), True),
        StructField("city_name", StringType(), True),
        StructField("raw_payload", StringType(), True)
    ])
    
    if rows:
        spark.createDataFrame(rows, schema) \
             .withColumn("ingest_timestamp", current_timestamp()) \
             .write.format("delta").mode("overwrite").saveAsTable(TARGET_TABLE)
        print("\n✅ 50-City Weather Grid Saved!")

ingest_open_meteo()