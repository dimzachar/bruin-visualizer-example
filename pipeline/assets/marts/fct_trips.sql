/* @bruin
name: marts.fct_trips
type: duckdb.sql

depends:
  - intermediate.int_trips
  - marts.dim_zones

materialization:
  type: table
@bruin */

-- Fact table containing all taxi trips enriched with zone information
SELECT
    -- Trip identifiers
    trips.trip_id,
    trips.vendor_id,
    trips.service_type,
    trips.rate_code_id,

    -- Location details (enriched with human-readable zone names from dimension)
    trips.pickup_location_id,
    pz.borough AS pickup_borough,
    pz.zone AS pickup_zone,
    trips.dropoff_location_id,
    dz.borough AS dropoff_borough,
    dz.zone AS dropoff_zone,

    -- Trip timing
    trips.pickup_datetime,
    trips.dropoff_datetime,
    trips.store_and_fwd_flag,

    -- Trip metrics
    trips.passenger_count,
    trips.trip_distance,
    trips.trip_type,
    ROUND(EXTRACT(EPOCH FROM (trips.dropoff_datetime - trips.pickup_datetime)) / 60, 2) AS trip_duration_minutes,

    -- Payment breakdown
    trips.fare_amount,
    trips.extra,
    trips.mta_tax,
    trips.tip_amount,
    trips.tolls_amount,
    trips.ehail_fee,
    trips.improvement_surcharge,
    trips.total_amount,
    trips.payment_type,
    trips.payment_type_description

FROM intermediate.int_trips AS trips
-- LEFT JOIN preserves all trips even if zone information is missing or unknown
LEFT JOIN marts.dim_zones AS pz
    ON trips.pickup_location_id = pz.location_id
LEFT JOIN marts.dim_zones AS dz
    ON trips.dropoff_location_id = dz.location_id;
