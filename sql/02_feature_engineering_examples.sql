-- 02_feature_engineering_examples.sql
-- Feature engineering examples on silver.taxi_sample (DuckDB)

------------------------------------------------------------
-- 1) Per-trip engineered features
--    Useful for modeling or granular BI.
------------------------------------------------------------
SELECT
  vendor_id,
  CAST(pickup_datetime AS TIMESTAMP)   AS pickup_ts,
  CAST(dropoff_datetime AS TIMESTAMP)  AS dropoff_ts,
  passenger_count,
  trip_distance,
  DATE_DIFF(
    'minute',
    CAST(pickup_datetime AS TIMESTAMP),
    CAST(dropoff_datetime AS TIMESTAMP)
  ) AS duration_min,
  trip_distance
    / NULLIF(
        DATE_DIFF(
          'minute',
          CAST(pickup_datetime AS TIMESTAMP),
          CAST(dropoff_datetime AS TIMESTAMP)
        ) / 60.0,
        0
      ) AS speed_mph,
  total_amount,
  tip_amount,
  CASE
    WHEN total_amount > 0 THEN tip_amount / total_amount
    ELSE NULL
  END AS tip_pct,
  CASE
    WHEN trip_distance >= 10 THEN 1
    ELSE 0
  END AS is_long_trip
FROM silver.taxi_sample
LIMIT 100;


------------------------------------------------------------
-- 2) Hourly demand & revenue profile
--    Good for staffing & pricing strategy.
------------------------------------------------------------
SELECT
  DATE_TRUNC('hour', CAST(pickup_datetime AS TIMESTAMP)) AS pickup_hour,
  COUNT(*)                      AS trips,
  SUM(total_amount)             AS revenue,
  AVG(trip_distance)            AS avg_distance_miles,
  AVG(total_amount)             AS avg_fare
FROM silver.taxi_sample
GROUP BY 1
ORDER BY pickup_hour
LIMIT 24;


------------------------------------------------------------
-- 3) Tip behaviour buckets
--    Ready-made feature for customer / driver segmentation.
------------------------------------------------------------
SELECT
  CASE
    WHEN total_amount <= 0 OR tip_amount IS NULL
      THEN 'no fare / no tip info'
    WHEN tip_amount = 0
      THEN '0% (no tip)'
    WHEN tip_amount / total_amount < 0.10
      THEN '<10% tip'
    WHEN tip_amount / total_amount < 0.20
      THEN '10–20% tip'
    ELSE '20%+ tip'
  END AS tip_bucket,
  COUNT(*)           AS trips,
  AVG(total_amount)  AS avg_fare
FROM silver.taxi_sample
GROUP BY 1
ORDER BY trips DESC;
