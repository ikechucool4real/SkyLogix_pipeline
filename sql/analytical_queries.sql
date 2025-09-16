-- Count total rows
SELECT COUNT(*) AS total_rows 
FROM my_db.main.silver_weather;

-- Count rows by city
SELECT city_name AS city, COUNT(*) AS n
FROM my_db.main.silver_weather
GROUP BY city_name
ORDER BY n DESC;

-- Latest timestamp by city
SELECT city_name AS city, MAX(datetime) AS latest_timestamp
FROM my_db.main.silver_weather
GROUP BY city_name;

-- Average temperature and humidity by city
SELECT city_name AS city,
       ROUND(AVG(temperature), 2) AS avg_temp,
       ROUND(AVG(relative_humidity), 2) AS avg_humidity
FROM my_db.main.silver_weather
GROUP BY city_name
ORDER BY avg_temp DESC;

-- Hours with measurable rain
SELECT city_name AS city, COUNT(*) AS rainy_hours
FROM my_db.main.silver_weather
WHERE precipitation > 0
GROUP BY city_name;

-- Average probability of precipitation (pop)
SELECT city_name AS city, ROUND(AVG(probability_of_precipitation), 1) AS avg_pop_pct
FROM my_db.main.silver_weather
GROUP BY city_name;

-- Max and average wind speed by city
SELECT city_name AS city,
       ROUND(MAX(wind_speed), 2) AS max_wind_speed,
       ROUND(AVG(wind_speed), 2) AS avg_wind_speed
FROM my_db.main.silver_weather
GROUP BY city_name;

-- Find duplicate primary keys (should be none if identity works)
SELECT weather_sk AS _id, COUNT(*) AS count_of_rows
FROM my_db.main.silver_weather
GROUP BY weather_sk
HAVING COUNT(*) > 1;

-- High UV or hot temperature events
SELECT city_name, datetime, temperature, uv AS uv_index
FROM my_db.main.silver_weather
WHERE uv >= 8 OR temperature >= 35
ORDER BY datetime DESC;

-- Aggregates for recent data (since 2025-08-01)
SELECT city_name AS city,
       ROUND(AVG(temperature), 1) AS avg_temp,
       ROUND(AVG(relative_humidity), 1) AS avg_humidity,
       ROUND(AVG(precipitation), 2) AS avg_precip
FROM my_db.main.silver_weather
WHERE CAST(datetime AS DATE) >= DATE '2025-08-01'
GROUP BY city_name
ORDER BY avg_temp DESC;
