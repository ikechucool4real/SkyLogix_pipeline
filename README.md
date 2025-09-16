# Weather Data ETL Pipeline

This project builds an end-to-end ETL pipeline to extract weather data from an API, store it in MongoDB, and load it into a cloud-based data warehouse (MotherDuck) using Airbyte.

---

## Project Overview

**Goal:** Automate weather data ingestion and transformation for analytics.
**Data Flow:**

```
Weather API --> Raw JSON --> MongoDB (raw) --> Transformation --> MongoDB (clean) --> Airbyte --> MotherDuck
```

**Components:**

* **Python ETL Script**: Extraction, transformation, and MongoDB load.
* **MongoDB**: Stores raw and clean weather data.
* **Airbyte**: Connects MongoDB to MotherDuck for data warehouse ingestion.
* **MotherDuck**: Cloud data warehouse for analytics.

---

## Project Structure

```
project_root/
├─ app/
│  ├─ main.py                 # Python ETL script
├─ sql/
│  ├─ silver_weather.sql      # Table creation & insertion
│  ├─ analytical_queries.sql      # Table creation & insertion
├─ raw_data/                  # Directory for raw JSON files
├─ .env                       # Environment variables
├─ README.md
└─ requirements.txt
```

---

## Setup & Usage

### 1. Clone the repository

```bash
git clone <repo_url>
cd project_root
```

### 2. Configure environment variables (`.env`)

```text
api_key=<YOUR_WEATHER_API_KEY>
api_url=<API_URL>
db_uri=<MONGODB_URI>
db_name=<DB_NAME>
raw_collection=<RAW_COLLECTION_NAME>
clean_collection=<CLEAN_COLLECTION_NAME>
raw_data=<RAW_DATA_PATH>
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Run ETL script

```bash
python app/main.py
```

* Raw JSON will be saved in `raw_data/`.
* Raw and clean data will be loaded into MongoDB.
* Airbyte will replicate clean data to MotherDuck.

---

## Notes & Best Practices

* Use `_ingested_at_` for auditing and incremental loads.
* Store both raw and clean data for traceability.
* Airbyte ensures smooth replication from MongoDB to MotherDuck.
* Timestamps in UTC prevent timezone issues.
* For large datasets, consider batching inserts to MongoDB.

---

## Future Enhancements

* Add support for multiple cities.
* Schedule ETL pipeline with Airflow or cron for automation.
* Add data quality checks and validation.
* Implement alerting for missing or delayed API data.
