/* @bruin
name: staging.stg_yellow_tripdata
type: duckdb.sql

depends:
  - ingestion.trips

materialization:
  type: table
@bruin */

-- Staging for yellow taxi data with all columns
SELECT
    -- identifiers
    CAST(vendor_id AS INTEGER) AS vendor_id,
    CAST(ratecode_id AS INTEGER) AS rate_code_id,
    CAST(pu_location_id AS INTEGER) AS pickup_location_id,
    CAST(do_location_id AS INTEGER) AS dropoff_location_id,

    -- timestamps
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,

    -- trip info
    CAST(store_and_fwd_flag AS VARCHAR) AS store_and_fwd_flag,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS DECIMAL(10,2)) AS trip_distance,

    -- payment info
    CAST(fare_amount AS DECIMAL(10,2)) AS fare_amount,
    CAST(extra AS DECIMAL(10,2)) AS extra,
    CAST(mta_tax AS DECIMAL(10,2)) AS mta_tax,
    CAST(tip_amount AS DECIMAL(10,2)) AS tip_amount,
    CAST(tolls_amount AS DECIMAL(10,2)) AS tolls_amount,
    CAST(improvement_surcharge AS DECIMAL(10,2)) AS improvement_surcharge,
    CAST(total_amount AS DECIMAL(10,2)) AS total_amount,
    CAST(payment_type AS INTEGER) AS payment_type

FROM ingestion.trips
WHERE taxi_type = 'yellow'
    AND vendor_id IS NOT NULL;
