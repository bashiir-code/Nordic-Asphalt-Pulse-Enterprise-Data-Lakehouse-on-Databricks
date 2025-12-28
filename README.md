# 🚦 Nordic Asphalt Pulse: Enterprise Data Lakehouse

**Real-Time Traffic & Weather Analytics Pipeline for Finland**

![Project Status](https://img.shields.io/badge/Status-Complete-green) ![Databricks](https://img.shields.io/badge/Platform-Databricks-orange) ![Python](https://img.shields.io/badge/Language-Python_3.9-blue)

## 📖 Project Overview

**Nordic Asphalt Pulse** is an end-to-end Data Engineering project simulating a production-grade Enterprise Data Lakehouse. It solves a classic "Big Data" problem: **How do we link two unrelated, real-time data streams—Physical Traffic Sensors and Atmospheric Weather Data—to explain traffic congestion?**

Unlike standard tutorials using clean, static CSVs, this project tackles the "messy" reality of live data engineering:
* **Scale:** Ingests live data from **50 strategic cities** across Finland (Helsinki to Lapland).
* **Complexity:** Joins datasets that share no common ID using **Geospatial Nearest Neighbor** logic.
* **Quality:** Handles real-world data issues (e.g., sensor outages, missing municipality tags, and winter temperature averaging errors).

### 🎯 Key Engineering Achievements
* **Automated Root Cause Analysis:** The pipeline automatically tags traffic jams as "Weather Impact" (Snow/Ice) vs. "High Volume" (Rush Hour).
* **Spatio-Temporal Linkage:** Implemented a custom algorithm to map 500+ traffic stations to the nearest weather observation point dynamically.
* **Data Quality Recovery:** Fixed "Warm Winter" data anomalies by switching aggregation logic from historical averages to strict time-windowed snapshots.

---

## 🏗️ Technical Architecture

The project follows the **Medallion Architecture** on **Databricks (Delta Lake)**.

### 🥉 Bronze Layer (Raw Ingestion)
* **Strategy:** ELT (Extract, Load, Transform).
* **Sources:**
    * **IoT Sensors:** Digitraffic.fi (GeoJSON payloads).
    * **Weather API:** Open-Meteo (50-City High-Res Grid).
* **Storage:** Delta Tables (`bronze_traffic_raw`, `bronze_weather_raw`).
* **Design Pattern:** "Infinite Undo Button" — Raw JSON is stored as-is to allow reprocessing historical data if business logic changes.

### 🥈 Silver Layer (Cleaning & Parsing)
* **Strategy:** Parse, Explode, and Standardize.
* **Transformations:**
    * Parsing nested JSON arrays into flat PySpark DataFrames.
    * **Schema Evolution:** Handling missing metadata (e.g., null municipality names) by deriving location from coordinates.
    * **Data Quality:** Filtering out sensor errors (e.g., speeds > 200 km/h or < 0 km/h).

### 🥇 Gold Layer (Enrichment & Business Logic)
* **Strategy:** Intelligent Enrichment & Dimension Modeling.
* **The "Nearest Neighbor" Algorithm:**
    * Instead of hardcoding regions, the pipeline calculates the Euclidean distance between every traffic sensor and all 50 weather hubs.
    * It automatically assigns the closest weather source (e.g., a sensor in *Vantaa* automatically inherits *Helsinki-Vantaa* weather).
* **Business Logic:**
    * **Traffic Status:** `CONGESTED` (< 60 km/h), `SLOW`, `FREE_FLOW`.
    * **Road Condition:** `SNOWY`, `ICY`, `DRY`.
    * **Root Cause:** Logic to determine if a jam is caused by Volume (`cars > 800`) or Weather (`snow > 0`).

---

## 🛠️ Tech Stack

| Component | Technology Used |
| :--- | :--- |
| **Cloud Platform** | Databricks (Community Edition) |
| **Storage Engine** | Delta Lake (ACID Transactions, Schema Enforcement) |
| **Compute** | Apache Spark (PySpark) |
| **Languages** | Python 3.9, SQL |
| **Visualization** | Folium (Interactive Geospatial Mapping) |
| **Orchestration** | Databricks Notebook Workflows |

---

## 📊 Dataset Overview

### 1. Traffic Data (Digitraffic.fi)
* **Type:** IoT Sensor Stream.
* **Coverage:** All major Finnish highways (E18, E75, etc.).
* **Key Metrics:** `average_speed`, `volume`, `sensor_id`, `lat/lon`.

### 2. Environmental Data (Open-Meteo)
* **Type:** Real-time Weather API.
* **Grid:** 50 Strategic Hubs (Top population centers + Critical Northern Nodes).
* **Key Metrics:** `temperature_c`, `snowfall_cm`, `wind_speed`, `weather_code`.

---

## 🚀 How to Run

To replicate this project in your own Databricks environment:

1.  **Clone the Repo:**
    ```bash
    git clone [https://github.com/](https://github.com/)[YourUsername]/nordic_pulse.git
    ```
2.  **Import to Databricks:**
    * Upload the `databricks/` folder to your Databricks Workspace.
3.  **Initialize Database:**
    * Run `00_setup/01_initialize_db.sql`.
    * Run `00_setup/02_create_weather_dimensions.py` (Creates the 50-city reference grid).
4.  **Execute Pipeline:**
    * Run `01_bronze/` notebooks to fetch data.
    * Run `02_silver/` to process JSON.
    * Run `03_gold/` to execute the Nearest Neighbor logic.
5.  **View Analytics:**
    * Open `04_analytics/01_dashboard_view` to see the interactive Folium map and root cause analysis.

---

## 📜 License & Citation

**License:** MIT License.

**Author:**
**Bashiir Muhamed** - *Aspiring Data Engineer*

If you use this code for your own portfolio or research, please cite:
> Bashiir. (2025). Nordic Asphalt Pulse: Enterprise Data Engineering Project. GitHub.