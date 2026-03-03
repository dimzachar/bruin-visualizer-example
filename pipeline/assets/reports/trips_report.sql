/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - marts.fct_trips

materialization:
  type: table
@bruin */

-- Daily trip summary report by service type and payment type
SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    service_type,
    payment_type_description AS payment_type,
    COUNT(*) AS trip_count,
    SUM(fare_amount) AS total_fare,
    AVG(fare_amount) AS avg_fare,
    SUM(total_amount) AS total_revenue,
    AVG(trip_distance) AS avg_trip_distance,
    AVG(trip_duration_minutes) AS avg_trip_duration_minutes
FROM marts.fct_trips
GROUP BY trip_date, service_type, payment_type_description;
