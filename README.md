# General Information

**Project Title:** Nordic Asphalt Pulse: Enterprise Data Lakehouse on Databricks

**Author / Lead Engineer:**
* **Name:** Bashiir Muhamed
* **Role:** Aspiring Data Engineer

**Project Status:**
* **Start Date:** December 2024
* **Status:** Active / In Development

**Geographic Focus:**
Finland (Helsinki Metropolitan Area)

**Keywords:**
Data Engineering, Medallion Architecture, Databricks, Delta Lake, PySpark, Spatio-Temporal Joins, API Ingestion, ETL Pipelines.

**Project Inspiration & Goal:**
I built this project to simulate a real-world Enterprise Data Engineering scenario. Most tutorials use clean, static CSVs. I wanted to tackle the "messy" reality of combining two unrelated live data streams:
1.  **IoT Sensor Data** (Traffic flow from Digitraffic).
2.  **Environmental Data** (Weather conditions from OpenWeather).

The core engineering challenge was **Spatio-Temporal linkage**: Traffic sensors and weather stations do not share a common ID. I implemented a geospatial logic layer to join these datasets dynamically based on proximity and time windows, creating a unified view of how weather impacts urban mobility.

---

# Technical Architecture

This project implements a strict **Medallion Architecture** using **Delta Lake** on Databricks.

### 🏗️ Data Pipeline Layers

1.  **🥉 Bronze Layer (Raw Ingestion)**
    * **Strategy:** Schema-on-Read / ELT.
    * **Storage:** Delta Tables (`bronze_traffic_raw`, `bronze_weather_raw`).
    * **Logic:** Raw JSON payloads are ingested via Python requests and stored as-is. This ensures an "Infinite Undo Button"—if business logic changes, we can re-process historical data without re-fetching it.

2.  **🥈 Silver Layer (Transformation & Enrichment)**
    * **Strategy:** Clean, Deduplicate, Link.
    * **Storage:** Delta Tables (`silver_traffic`, `silver_weather`).
    * **Key Engineering:**
        * Parsing complex nested JSON into flat PySpark DataFrames.
        * **Geospatial Join:** Implementing the Haversine formula to map every traffic sensor to its nearest weather station (< 10km radius).

3.  **🥇 Gold Layer (Business Analytics)**
    * **Strategy:** Star Schema (Kimball Methodology).
    * **Storage:** Delta Tables (`gold_road_conditions`, `dim_sensors`).
    * **Output:** Aggregated metrics (e.g., "Impact of Snowfall on Average Highway Speed") ready for Power BI or Tableau.

---

# Tech Stack

| Component | Technology Used |
| :--- | :--- |
| **Cloud Platform** | Databricks (Community Edition) |
| **Storage Format** | Delta Lake (ACID Transactions, Time Travel) |
| **Compute** | Apache Spark (PySpark) |
| **Language** | Python 3.9, SQL |
| **Orchestration** | Databricks Workflows |
| **Version Control** | Git / GitHub |

---

# Dataset & File Overview

### 1. Traffic Data (Source: Digitraffic.fi)
* **Update Frequency:** Every 10 minutes.
* **Format:** Complex GeoJSON.
* **Key Variables:** `sensor_id`, `timestamp`, `average_speed`, `vehicle_count`.
* **License:** CC BY 4.0.

### 2. Weather Data (Source: OpenWeatherMap)
* **Update Frequency:** Every 10 minutes.
* **Format:** JSON.
* **Key Variables:** `station_name`, `temperature`, `snow_1h` (mm), `visibility`.
* **License:** OpenWeather Open Data License.

---

# Sharing and Access

**License:**
This project is open-source under the **MIT License**.

**Citation:**
If you use this code for your own portfolio or research, please credit:
> Bashiir. (2024). Nordic Asphalt Pulse [Source Code]. GitHub.

---

# Setup & Installation

To replicate this project in your own Databricks environment:

1.  **Clone the Repo:**
    ```bash
    git clone [https://github.com/](https://github.com/)[YourUsername]/nordic_pulse.git
    ```
2.  **Import to Databricks:**
    * Use "Databricks Repos" to sync this folder structure.
3.  **Initialize Database:**
    * Run `databricks/00_setup/01_initialize_db.sql`.
4.  **Configure Secrets:**
    * Add your OpenWeather API key to the Bronze Ingestion script.