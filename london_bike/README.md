# London Bike Rental Analysis and Dashboard
Application: BigQuery, Google Colab, Google Spreadsheet, Tableau
Skills: SQL, Data Analysis, Dashboard

This is a solo project outside bootcamp. Please refer to the pdf deck in this directory for further info.

## Problem:
I want to explore:
1. Total bike rental.
2. Average duration of bike rental.
3. Most crowded day and hour.
4. Most common starting station?
5. Most common destination?
6. Most common ride route?
7. Most common station for same station trip and different station trip.

## Method:
### Query data from BigQuery
SELECT
rental_id
, end_date
, end_station_id
, start_date
, start_station_id
FROM `bigquery-public-data.london_bicycles.cycle_hire`
WHERE start_station_id IS NOT NULL
AND EXTRACT(YEAR FROM DATE(start_date)) = 2016;

SELECT
rental_id
, duration
, EXTRACT(DAYOFWEEK FROM DATE(start_date)) AS
start_date_day
, IF(
EXTRACT(DAYOFWEEK FROM DATE(start_date)) = 1
OR
EXTRACT(DAYOFWEEK FROM DATE(start_date)) = 7,
"weekend", "weekday") AS is_weekday
, EXTRACT(HOUR FROM DATETIME(start_date)) AS start_hour
, EXTRACT(HOUR FROM DATETIME(end_date)) AS end_hour
FROM `bigquery-public-data.london_bicycles.cycle_hire`
WHERE start_station_id IS NOT NULL
AND EXTRACT(YEAR FROM DATE(start_date)) = 2016;

SELECT
id
, name
, latitude
, longitude
FROM `bigquery-public-data.london_bicycles.cycle_stations`;

### Data Cleaning
- Merging is done in Google Colab since BigQuery have restrictions on maximum output size.
- No missing data
- No duplicate
- Date column is leaved as it is since Tableau is able to detect datetime automatically

### Data Analysis
- Please refer to deck for further detail.

### Insight
- Most people rental bike to travel to different stations.
- The top 2 starting point and destination is near subway/train station.
- Most common trip route is to travel around the park.
- By analyzing trip daily and hourly, it is found that different station trip resembles travel for workday activities while same station trip pattern resembles leisure activities.

### Dashboard
- [Dashboard link](https://www.google.com/url?q=https://public.tableau.com/views/TryBikeLondon/Dashboard1?:language%3Den-US%26:display_count%3Dn%26:origin%3Dviz_share_link&sa=D&source=apps-viewer-frontend&ust=1696042325246050&usg=AOvVaw2x0V8Zfck7ma6fLvOTP9j7&hl=en)
