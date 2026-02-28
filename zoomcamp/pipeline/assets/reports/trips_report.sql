/* @bruin

# Docs:
# - SQL assets: https://getbruin.com/docs/bruin/assets/sql
# - Materialization: https://getbruin.com/docs/bruin/assets/materialization
# - Quality checks: https://getbruin.com/docs/bruin/quality/available_checks

# TODO: Set the asset name (recommended: reports.trips_report).
name: reports.trips_report

# TODO: Set platform type.
# Docs: https://getbruin.com/docs/bruin/assets/sql
# suggested type: duckdb.sql
type: duckdb.sql

# TODO: Declare dependency on the staging asset(s) this report reads from.
depends:
  - staging.trips


# TODO: Choose materialization strategy.
# For reports, `time_interval` is a good choice to rebuild only the relevant time window.
# Important: Use the same `incremental_key` as staging (e.g., pickup_datetime) for consistency.
materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date


custom_checks:
  - name: row_count_greater_than_zero
    value: 1
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM reports.trips_report



# TODO: Define report columns + primary key(s) at your chosen level of aggregation.
columns:
  - name: vendor_id
    type: INTEGER
    description: Taxi vendor ID
    primary_key: true
  - name: pickup_date
    type: DATE
    description: Trip pickup date
    primary_key: true
  - name: trip_count
    type: BIGINT
    description: Total number of trips
    checks:
      - name: non_negative
  - name: total_distance
    type: DOUBLE
    description: Total distance traveled in miles
    checks:
      - name: non_negative
  - name: avg_distance
    type: DOUBLE
    description: Average distance per trip in miles
    checks:
      - name: non_negative
  - name: total_fare
    type: DOUBLE
    description: Total fare amount in dollars
    checks:
      - name: non_negative
  - name: avg_fare
    type: DOUBLE
    description: Average fare per trip in dollars
    checks:
      - name: non_negative
  - name: total_passengers
    type: BIGINT
    description: Total passenger count across all trips
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  vendor_id,
  pickup_datetime::DATE AS pickup_date,
  COUNT(*) AS trip_count,
  SUM(trip_distance) AS total_distance,
  AVG(trip_distance) AS avg_distance,
  SUM(total_amount) AS total_fare,
  AVG(total_amount) AS avg_fare,
  SUM(passenger_count) AS total_passengers
FROM staging.trips
GROUP BY vendor_id, pickup_datetime::DATE
ORDER BY pickup_date DESC, vendor_id
