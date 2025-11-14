-- 01_analytics_examples.sql
-- Example analytics queries on silver.taxi_sample (DuckDB)

------------------------------------------------------------
-- 1) Daily trips and revenue
------------------------------------------------------------
SELECT
  DATE(pickup_datetime) AS trip_date,
  COUNT(*) AS trips,
  SUM(total_amount) AS revenue
FROM silver.taxi_sample
GROUP BY 1
ORDER BY trip_date
LIMIT 10;


------------------------------------------------------------
-- 2) Top 10 pickup zones by revenue
------------------------------------------------------------
SELECT
  pickup_location_id,
  COUNT(*) AS trips,
  SUM(total_amount) AS revenue
FROM silver.taxi_sample
GROUP BY 1
ORDER BY revenue DESC
LIMIT 10;


------------------------------------------------------------
-- 3) Trip distance distribution buckets
------------------------------------------------------------
SELECT
  CASE
    WHEN trip_distance < 1 THEN '<1 mile'
    WHEN trip_distance < 3 THEN '1–3 miles'
    WHEN trip_distance < 5 THEN '3–5 miles'
    WHEN trip_distance < 10 THEN '5–10 miles'
    ELSE '10+ miles'
  END AS distance_bucket,
  COUNT(*) AS trips,
  AVG(total_amount) AS avg_fare
FROM silver.taxi_sample
GROUP BY 1
ORDER BY trips DESC;
