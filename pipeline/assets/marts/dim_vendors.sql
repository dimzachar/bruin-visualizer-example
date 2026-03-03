/* @bruin
name: marts.dim_vendors
type: duckdb.sql

depends:
  - marts.fct_trips

materialization:
  type: table
@bruin */

-- Dimension table for taxi technology vendors
WITH trips AS (
    SELECT * FROM marts.fct_trips
),

vendors AS (
    SELECT DISTINCT
        vendor_id,
        CASE
            WHEN vendor_id = 1 THEN 'Creative Mobile Technologies'
            WHEN vendor_id = 2 THEN 'VeriFone Inc.'
            ELSE 'Unknown'
        END AS vendor_name
    FROM trips
)

SELECT * FROM vendors;
