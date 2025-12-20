import requests
import json

# --- 1. CONFIGURATION ---
# Traffic Data (Finland)
TRAFFIC_URL = "https://tie.digitraffic.fi/api/tms/v1/stations/data"


def inspect_api(url, name):
    print(f"\n🔎 INSPECTING: {name}")
    print("=" * 60)
    try:
        response = requests.get(url)
        response.raise_for_status() # Check for errors (404, 500)
        
        data = response.json()
        
        # Check if it's a Dictionary or a List
        if isinstance(data, dict):
            print(f"✅ Type: Object (Dictionary)")
            # Print keys to see the top-level structure
            print(f"🔑 Top-Level Keys: {list(data.keys())}")
        elif isinstance(data, list):
            print(f"✅ Type: Array (List) with {len(data)} items")
            # If it's a list, let's look at the FIRST item only
            data = data[0]
            
        print("-" * 60)
        print("👇 HERE IS A SAMPLE RECORD (First 500 chars):")
        # Pretty print the JSON so we can read it
        print(json.dumps(data, indent=2)[0:5000] + "...") 
        
    except Exception as e:
        print(f"❌ Error fetching {name}: {e}")

# --- 2. RUN THE INSPECTION ---
inspect_api(TRAFFIC_URL, "tms-data (Traffic)")
# inspect_api(WEATHER_URL, "OpenWeather (Weather)")