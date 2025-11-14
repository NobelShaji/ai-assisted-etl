-- 04_ml_features_materialized.sql
-- Materialize ML-ready features from silver.taxi_sample into a gold table.

CREATE SCHEMA IF NOT EXISTS gold;

CREATE OR REPLACE TABLE gold.taxi_trip_features AS
SELECT
  -- Keys & timestamps
  vendor_id,
  CAST(pickup_datetime  AS TIMESTAMP) AS pickup_ts,
  CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_ts,
  DATE(pickup_datetime) AS pickup_date,
  EXTRACT(hour FROM CAST(pickup_datetime AS TIMESTAMP)) AS pickup_hour,

  -- Core trip metrics
  passenger_count,
  trip_distance,
  -- trip duration in minutes
  DATE_DIFF(
    'minute',
    CAST(pickup_datetime  AS TIMESTAMP),
    CAST(dropoff_datetime AS TIMESTAMP)
  ) AS duration_min,

  -- Avoid divide-by-zero: compute speed only when duration_min > 0
  CASE
    WHEN DATE_DIFF(
      'minute',
      CAST(pickup_datetime  AS TIMESTAMP),
      CAST(dropoff_datetime AS TIMESTAMP)
    ) > 0
    THEN 60.0 * trip_distance
         / DATE_DIFF(
             'minute',
             CAST(pickup_datetime  AS TIMESTAMP),
             CAST(dropoff_datetime AS TIMESTAMP)
           )
    ELSE NULL
  END AS speed_mph,

  -- Money features
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  congestion_surcharge,
  total_amount,
  CASE
    WHEN total_amount > 0 THEN tip_amount / total_amount
    ELSE NULL
  END AS tip_pct,

  -- Categorical flags useful for models
  rate_code_id,
  payment_type,
  pickup_location_id,
  dropoff_location_id,

  CASE WHEN trip_distance >= 10 THEN 1 ELSE 0 END AS is_long_trip,
  CASE WHEN total_amount >= 100 THEN 1 ELSE 0 END AS is_high_fare,

  -- Time-of-day buckets
  CASE
    WHEN EXTRACT(hour FROM CAST(pickup_datetime AS TIMESTAMP)) BETWEEN 6 AND 11
      THEN 'morning'
    WHEN EXTRACT(hour FROM CAST(pickup_datetime AS TIMESTAMP)) BETWEEN 12 AND 17
      THEN 'afternoon'
    WHEN EXTRACT(hour FROM CAST(pickup_datetime AS TIMESTAMP)) BETWEEN 18 AND 22
      THEN 'evening'
    ELSE 'night'
  END AS pickup_daypart

FROM silver.taxi_sample
WHERE
  trip_distance > 0
  AND total_amount > 0
  AND CAST(dropoff_datetime AS TIMESTAMP) >= CAST(pickup_datetime AS TIMESTAMP);
