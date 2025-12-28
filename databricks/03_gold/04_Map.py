%pip install folium
import folium

# 1. Fetch Data (Limit to 1000 for browser performance)
df_map = spark.sql("""
    SELECT 
        latitude, longitude, region_name, 
        avg_speed_kmh, total_volume_cars, 
        traffic_status, probable_cause, road_condition, temperature
    FROM workspace.nordic_pulse_db.gold_analytics_ready
    WHERE latitude IS NOT NULL
""").limit(1000).toPandas()

print(f"📍 Plotting {len(df_map)} stations...")

# 2. Create Base Map (Centered on Central Finland)
m = folium.Map(location=[64.0, 26.0], zoom_start=5, tiles="CartoDB dark_matter")

# 3. Add Smart Markers
for i, row in df_map.iterrows():
    
    # 🎨 Color Logic (Speed)
    if row['traffic_status'] == 'CONGESTED':
        color = '#ff3333' # Red
    elif row['traffic_status'] == 'SLOW':
        color = '#ff9933' # Orange
    else:
        color = '#33cc33' # Green

    # 📏 Size Logic (Volume)
    # Minimum radius 2, adds size based on volume (Max radius ~10)
    # We use (volume / 200) as a scaling factor
    radius_size = 2 + (row['total_volume_cars'] / 200)
    if radius_size > 15: radius_size = 15 # Cap size

    # 💬 Tooltip Logic (The "Why")
    tooltip_html = f"""
    <b>Region:</b> {row['region_name']}<br>
    <b>Status:</b> {row['traffic_status']}<br>
    <b>Speed:</b> {row['avg_speed_kmh']} km/h<br>
    <b>Volume:</b> {row['total_volume_cars']} cars<br>
    <hr>
    <b>Condition:</b> {row['road_condition']} ({row['temperature']}°C)<br>
    <b>Reason:</b> {row['probable_cause']}
    """

    folium.CircleMarker(
        location=[row['latitude'], row['longitude']],
        radius=radius_size,
        color=color,
        fill=True,
        fill_color=color,
        fill_opacity=0.6,
        popup=folium.Popup(tooltip_html, max_width=300),
        tooltip=f"{row['region_name']}: {row['avg_speed_kmh']} km/h"
    ).add_to(m)

# 4. Show Map
m