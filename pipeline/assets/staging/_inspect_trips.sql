/* @bruin
name: staging._inspect_trips
type: duckdb.sql

depends:
  - ingestion.trips

materialization:
  type: view
@bruin */

-- Inspect the actual columns in ingestion.trips
SELECT * FROM ingestion.trips LIMIT 1;
