# 🚦 Nordic Asphalt Pulse: Real-Time Traffic & Weather Analytics for Finland

![Project Status](https://img.shields.io/badge/Status-Complete-green) ![Databricks](https://img.shields.io/badge/Platform-Databricks-orange) ![Python](https://img.shields.io/badge/Language-Python_3.9-blue)

## 📖 What This Project Is About

**Nordic Asphalt Pulse** is my attempt at building a real, production-grade data pipeline that actually solves something interesting: figuring out why traffic jams happen in Finland. Is it because of a snowstorm? Rush hour? Both?

Here's the challenge I wanted to tackle. Most data engineering tutorials give you nice, clean CSV files that are perfectly formatted. Real life isn't like that. In the real world, you're dealing with messy JSON from IoT sensors, APIs that sometimes fail, and datasets that have absolutely nothing in common except maybe a timestamp.

So I built a pipeline that pulls live data from 50 cities across Finland (from Helsinki all the way up to Lapland), combines traffic sensor readings with weather data, and figures out what's actually causing congestion. The tricky part? These two data sources don't share any common identifiers. I had to build a custom geospatial algorithm to match each traffic sensor with its nearest weather station.

### 🎯 What Makes This Interesting

The pipeline doesn't just collect data. It actually thinks about what it's seeing:

When traffic slows down, it automatically tags the cause. Is it snowy and icy outside? Then it's probably a weather-related slowdown. Clear skies but 800+ cars passing through? That's just rush hour doing its thing.

I also had to solve some real data quality issues. For example, the weather API was averaging historical temperature data in a way that made Finnish winters look warm (spoiler: they're not). I fixed that by switching to time-windowed snapshots instead of historical averages.

The geospatial matching algorithm was particularly fun to build. Instead of hardcoding which weather station belongs to which region, it calculates the actual distance between every traffic sensor and all 50 weather observation points, then picks the closest one. So if a sensor is sitting on the highway in Vantaa, it automatically gets matched with Helsinki-Vantaa weather data.

## 🏗️ How It's Built

I followed the Medallion Architecture pattern on Databricks with Delta Lake. Think of it like a three-layer cake:

### 🥉 Bronze Layer (The Raw Stuff)

This is where everything lands first. I'm pulling data from two sources: IoT traffic sensors via Digitraffic.fi and weather data from Open-Meteo covering 50 cities. Everything gets stored exactly as it arrives in Delta tables, no transformations yet.

Why keep the raw data? Because if I need to change my business logic later (and I always do), I can reprocess everything from scratch without having to fetch it all over again. It's like having an infinite undo button.

### 🥈 Silver Layer (Making Sense of the Mess)

Here's where I clean things up. The raw data comes in as nested JSON, so I parse it, flatten those arrays, and standardize the schema. 

Some sensors don't report their municipality names, so I derive locations from coordinates. I also filter out obvious sensor errors (cars going 250 km/h or negative speeds, because physics still matters).

### 🥇 Gold Layer (The Smart Stuff)

This is where the magic happens. The pipeline calculates distances between traffic sensors and weather stations using Euclidean geometry, then links them together. 

Once the data is enriched, I apply business logic to categorize everything. Traffic is labeled as CONGESTED (under 60 km/h), SLOW, or FREE_FLOW. Roads are tagged as SNOWY, ICY, or DRY based on current conditions. And then comes the root cause analysis: is this jam happening because of high volume, or is Mother Nature making the roads terrible?

## 🛠️ The Tech Stack

| Component | What I Used |
|-----------|-------------|
| **Cloud Platform** | Databricks Community Edition |
| **Storage** | Delta Lake for ACID transactions and schema enforcement |
| **Compute** | Apache Spark with PySpark |
| **Languages** | Python 3.9 and SQL |
| **Visualization** | Folium for interactive maps |
| **Orchestration** | Databricks Notebooks |

## 📊 The Data Sources

### Traffic Data from Digitraffic.fi

This is live IoT sensor data from every major Finnish highway (think E18, E75, and friends). Each sensor reports its average speed, traffic volume, exact GPS coordinates, and a unique sensor ID.

### Weather Data from Open-Meteo

Real-time weather information from 50 strategic points across Finland. I chose these locations based on population centers and critical northern nodes. Each point gives me temperature, snowfall, wind speed, and a weather condition code.

## 🚀 Want to Try It Yourself?

If you want to run this in your own Databricks environment:

1. Clone the repository:
```bash
git clone https://github.com/[YourUsername]/nordic_pulse.git
```

2. Import the `databricks/` folder into your Databricks workspace.

3. Set up the database by running `00_setup/00_initialize_db.sql`, then run `00_setup/01_Dim_weather.py` to create the 50-city reference grid.

4. Execute the pipeline in order: start with the `01_bronze/` notebooks to fetch data, move to `02_silver/` for processing, then `03_gold/` for the geospatial magic.

5. Check out the analytics in the `04_analytics` folder to see the interactive Folium maps and root cause analysis.

## 📜 A Quick Note

This project is MIT licensed and free to use. If you end up using this code for your own portfolio or learning, I'd appreciate a mention:

> Bashiir Muhamed. (2025). Nordic Asphalt Pulse: Real-Time Traffic & Weather Data Pipeline. GitHub.

I'm an aspiring data engineer, and this project represents what I love about this field: taking messy, real-world data and turning it into something that actually answers questions.