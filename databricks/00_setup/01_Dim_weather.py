# -------------------------------------------------------------------------
# 🛠️ SETUP: Create 50-City Reference Table (Run Once)
# -------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# The "High Definition" Grid of Finland
weather_locs = [
    # --- UUSIMAA ---
    ("Helsinki", 60.17, 24.94), ("Espoo", 60.21, 24.66), ("Vantaa", 60.29, 25.04),
    ("Lohja", 60.25, 24.07), ("Hyvinkaa", 60.63, 24.86), ("Porvoo", 60.39, 25.67),
    ("Kerava", 60.40, 25.11), ("Jarvenpaa", 60.47, 25.09), ("Nurmijarvi", 60.46, 24.81),
    ("Kirkkonummi", 60.12, 24.44), ("Tuusula", 60.40, 25.03), ("Raseborg", 59.97, 23.44),

    # --- SOUTHWEST ---
    ("Turku", 60.45, 22.27), ("Salo", 60.39, 23.13), ("Kaarina", 60.41, 22.37),
    ("Raisio", 60.49, 22.17), ("Pori", 61.49, 21.80), ("Rauma", 61.13, 21.50),

    # --- CENTRAL / TAMPERE REGION ---
    ("Tampere", 61.50, 23.79), ("Nokia", 61.48, 23.51), ("Kangasala", 61.46, 24.07),
    ("Ylojarvi", 61.56, 23.60), ("Lempaala", 61.32, 23.75), ("Hameenlinna", 61.00, 24.44),
    ("Lahti", 60.98, 25.66), ("Hollola", 60.99, 25.51), ("Riihimaki", 60.74, 24.78),
    ("Jyvaskyla", 62.24, 25.75), ("Jamsa", 61.86, 25.19),

    # --- EASTERN FINLAND ---
    ("Kuopio", 62.89, 27.68), ("Varkaus", 62.32, 27.89), ("Iisalmi", 63.56, 27.19),
    ("Joensuu", 62.60, 29.76), ("Mikkeli", 61.69, 27.27), ("Savonlinna", 61.87, 28.88),
    ("Lappeenranta", 61.06, 28.19), ("Imatra", 61.19, 28.77), ("Kouvola", 60.87, 26.70),
    ("Kotka", 60.47, 26.94),

    # --- OSTROBOTHNIA ---
    ("Vaasa", 63.10, 21.62), ("Seinajoki", 62.79, 22.84), ("Kurikka", 62.62, 22.40),
    ("Kokkola", 63.84, 23.13),

    # --- NORTH & LAPLAND ---
    ("Oulu", 65.01, 25.47), ("Raahe", 64.68, 24.48), ("Kajaani", 64.23, 27.73),
    ("Rovaniemi", 66.50, 25.73), ("Kemi", 65.73, 24.56), ("Tornio", 65.85, 24.15)
]

schema = StructType([
    StructField("city_name", StringType(), True),
    StructField("w_lat", DoubleType(), True),
    StructField("w_lon", DoubleType(), True)
])

df_dim = spark.createDataFrame(weather_locs, schema)
df_dim.write.mode("overwrite").saveAsTable("workspace.nordic_pulse_db.dim_weather_locations")

print(f"✅ Created Dimension Table with {df_dim.count()} cities.")
df_dim.show(5)