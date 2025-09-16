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

## Database Schema (MotherDuck)

**Table:** `silver_weather`

| Column                          | Type      |
| ------------------------------- | --------- |
| weather\_sk                     | bigint PK |
| city\_name                      | varchar   |
| country\_code                   | varchar   |
| datetime                        | timestamp |
| temperature                     | float     |
| app\_temperature                | float     |
| dew\_point\_temperature         | float     |
| cloud\_cover                    | int       |
| high\_cloud\_cover              | int       |
| mid\_cloud\_cover               | int       |
| low\_cloud\_cover               | int       |
| probability\_of\_precipitation  | int       |
| precipitation                   | float     |
| snow                            | int       |
| snow\_depth                     | int       |
| pressure                        | int       |
| sea\_level\_pressure            | int       |
| relative\_humidity              | int       |
| diffuse\_horizontal\_irradiance | int       |
| direct\_normal\_irradiance      | int       |
| global\_horizontal\_irradiance  | int       |
| solar\_radiation                | float     |
| uv                              | int       |
| visibility                      | float     |
| wind\_cardinal\_direction       | varchar   |
| wind\_direction                 | int       |
| gust\_speed                     | float     |
| wind\_speed                     | float     |
| part\_of\_day                   | varchar   |
| ozone                           | int       |
| weather\_icon                   | varchar   |
| weather\_code                   | varchar   |
| weather\_description            | varchar   |
| ingested\_at                    | timestamp |

---

## Sample SQL Queries for Analytics

### Total rows

```sql
SELECT COUNT(*) AS total_rows FROM my_db.main.silver_weather;
```

### Average temperature & humidity by city

```sql
SELECT city_name AS city,
       ROUND(AVG(temperature), 2) AS avg_temp,
       ROUND(AVG(relative_humidity), 2) AS avg_humidity
FROM my_db.main.silver_weather
GROUP BY city_name
ORDER BY avg_temp DESC;
```

### Rainy hours by city

```sql
SELECT city_name AS city, COUNT(*) AS rainy_hours
FROM my_db.main.silver_weather
WHERE precipitation > 0
GROUP BY city_name;
```

### High UV or hot temperature events

```sql
SELECT city_name AS city, datetime AS dt, temperature AS temp_c, uv AS uv_index
FROM my_db.main.silver_weather
WHERE uv >= 8 OR temperature >= 35
ORDER BY datetime DESC;
```

### Max & average wind speed by city

```sql
SELECT city_name AS city,
       ROUND(MAX(wind_speed), 2) AS max_wind_speed,
       ROUND(AVG(wind_speed), 2) AS avg_wind_speed
FROM my_db.main.silver_weather
GROUP BY city_name;
```

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
