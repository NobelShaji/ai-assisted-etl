-- 03_data_quality_checks.sql
-- Data quality checks on silver.taxi_sample (DuckDB)

------------------------------------------------------------
-- 1) Null count summary across all columns
------------------------------------------------------------
SELECT
  column_name,
  SUM(null_count) AS nulls
FROM (
  SELECT
    column_name,
    COUNT(*) FILTER (WHERE column_value IS NULL) AS null_count
  FROM (
    SELECT
      'vendor_id'              AS column_name, vendor_id              AS column_value FROM silver.taxi_sample
    UNION ALL
    SELECT 'pickup_datetime',        pickup_datetime        FROM silver.taxi_sample
    UNION ALL
    SELECT 'dropoff_datetime',       dropoff_datetime       FROM silver.taxi_sample
    UNION ALL
    SELECT 'passenger_count',        passenger_count        FROM silver.taxi_sample
    UNION ALL
    SELECT 'trip_distance',          trip_distance          FROM silver.taxi_sample
    UNION ALL
    SELECT 'rate_code_id',           rate_code_id           FROM silver.taxi_sample
    UNION ALL
    SELECT 'store_and_fwd_flag',     store_and_fwd_flag     FROM silver.taxi_sample
    UNION ALL
    SELECT 'pickup_location_id',     pickup_location_id     FROM silver.taxi_sample
    UNION ALL
    SELECT 'dropoff_location_id',    dropoff_location_id    FROM silver.taxi_sample
    UNION ALL
    SELECT 'payment_type',           payment_type           FROM silver.taxi_sample
    UNION ALL
    SELECT 'fare_amount',            fare_amount            FROM silver.taxi_sample
    UNION ALL
    SELECT 'extra',                  extra                  FROM silver.taxi_sample
    UNION ALL
    SELECT 'mta_tax',                mta_tax                FROM silver.taxi_sample
    UNION ALL
    SELECT 'tip_amount',             tip_amount             FROM silver.taxi_sample
    UNION ALL
    SELECT 'tolls_amount',           tolls_amount           FROM silver.taxi_sample
    UNION ALL
    SELECT 'improvement_surcharge',  improvement_surcharge  FROM silver.taxi_sample
    UNION ALL
    SELECT 'total_amount',           total_amount           FROM silver.taxi_sample
    UNION ALL
    SELECT 'congestion_surcharge',   congestion_surcharge   FROM silver.taxi_sample
  ) t
  GROUP BY column_name
)
GROUP BY column_name
ORDER BY nulls DESC;


------------------------------------------------------------
-- 2) Trips with clearly invalid distances or fares
------------------------------------------------------------
SELECT *
FROM silver.taxi_sample
WHERE trip_distance < 0
   OR total_amount < 0
   OR passenger_count < 0
LIMIT 20;


------------------------------------------------------------
-- 3) Outlier detection for trip distance (simple z-score)
------------------------------------------------------------
WITH stats AS (
  SELECT
    AVG(trip_distance) AS mean,
    STDDEV(trip_distance) AS sd
  FROM silver.taxi_sample
)
SELECT
  vendor_id,
  pickup_datetime,
  trip_distance,
  (trip_distance - stats.mean)/stats.sd AS zscore
FROM silver.taxi_sample, stats
WHERE ABS((trip_distance - stats.mean)/stats.sd) > 4
ORDER BY zscore DESC
LIMIT 20;


------------------------------------------------------------
-- 4) Revenue sanity check (flag suspiciously high fares)
------------------------------------------------------------
SELECT
  pickup_datetime,
  trip_distance,
  total_amount,
  CASE
    WHEN total_amount > 200 THEN 'Suspicious — very high'
    WHEN total_amount > 100 THEN 'High'
    ELSE 'Normal'
  END AS fare_flag
FROM silver.taxi_sample
WHERE total_amount > 50
ORDER BY total_amount DESC
LIMIT 20;
