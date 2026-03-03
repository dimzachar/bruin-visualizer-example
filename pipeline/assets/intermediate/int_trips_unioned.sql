/* @bruin
name: intermediate.int_trips_unioned
type: duckdb.sql

depends:
  - staging.stg_green_tripdata
  - staging.stg_yellow_tripdata

materialization:
  type: table
@bruin */

-- Union green and yellow taxi data into a single dataset
WITH green_trips AS (
    SELECT
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        'Green' AS service_type
    FROM staging.stg_green_tripdata
),

yellow_trips AS (
    SELECT
        vendor_id,
        rate_code_id,
        pickup_location_id,
        dropoff_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        CAST(1 AS INTEGER) AS trip_type,  -- Yellow taxis only do street-hail (code 1)
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        CAST(0 AS DECIMAL(10,2)) AS ehail_fee,  -- Yellow taxis don't have ehail_fee
        improvement_surcharge,
        total_amount,
        payment_type,
        'Yellow' AS service_type
    FROM staging.stg_yellow_tripdata
)

SELECT * FROM green_trips
UNION ALL
SELECT * FROM yellow_trips;
