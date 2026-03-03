/* @bruin
name: marts.dim_zones
type: duckdb.sql

depends:
  - ingestion.taxi_zone_lookup

materialization:
  type: table
@bruin */

-- Dimension table for NYC taxi zones
SELECT
    locationid AS location_id,
    borough,
    zone,
    service_zone
FROM ingestion.taxi_zone_lookup;
