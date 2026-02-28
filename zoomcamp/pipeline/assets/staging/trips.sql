/* @bruin

name: staging.trips
type: duckdb.sql

materialization:
  type: table

depends:
  - ingestion.trips
  - ingestion.payment_lookup


custom_checks:
  - name: row_count_greater_than_zero
    value: 1
    query: |
      SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END
      FROM staging.trips

columns:
  - name: vendor_id
    type: INTEGER
    description: Taxi vendor ID (1 = Yellow Cab, 2 = Limousine)

  - name: pickup_datetime
    type: TIMESTAMP
    description: Trip start date and time

  - name: dropoff_datetime
    type: TIMESTAMP
    description: Trip end date and time

  - name: passenger_count
    type: INTEGER
    description: Number of passengers in the trip

  - name: trip_distance
    type: DOUBLE
    description: Distance traveled in miles (validated >= 0)

  - name: rate_code
    type: INTEGER
    description: Rate code applied during the trip

  - name: store_and_fwd_flag
    type: VARCHAR
    description: Flag indicating if trip record was held in vehicle memory before sending ('Y' or 'N')

  - name: pu_location_id
    type: INTEGER
    description: Pickup location ID (taxi zone)

  - name: do_location_id
    type: INTEGER
    description: Dropoff location ID (taxi zone)

  - name: payment_type
    type: INTEGER
    description: Payment method ID (1 = Credit card, 2 = Cash, 3 = No charge, 4 = Dispute)

  - name: payment_type_name
    type: VARCHAR
    description: Human-readable payment method name

  - name: fare_amount
    type: DOUBLE
    description: Base fare amount in dollars

  - name: extra
    type: DOUBLE
    description: Extra charges (rush hour, overnight, etc.) in dollars

  - name: mta_tax
    type: DOUBLE
    description: MTA tax amount in dollars

  - name: tip_amount
    type: DOUBLE
    description: Tip amount in dollars

  - name: tolls_amount
    type: DOUBLE
    description: Total toll amount in dollars

  - name: total_amount
    type: DOUBLE
    description: Total payment amount (fare + extra + tax + tip + tolls) in dollars

  - name: extracted_at
    type: TIMESTAMP
    description: Timestamp when the record was extracted from the source

@bruin */



WITH cleaned AS (
  SELECT
    vendor_id,
    tpep_pickup_datetime AS pickup_datetime,
    tpep_dropoff_datetime AS dropoff_datetime,
    passenger_count,
    trip_distance,
    ratecode_id AS rate_code,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    total_amount,
    extracted_at,
    ROW_NUMBER() OVER (
      PARTITION BY vendor_id, tpep_pickup_datetime, pu_location_id, do_location_id, trip_distance
      ORDER BY extracted_at DESC
    ) AS rn
  FROM ingestion.trips
  WHERE 
    passenger_count IS NOT NULL
    AND trip_distance IS NOT NULL
    AND trip_distance >= 0
)

SELECT
  c.vendor_id,
  c.pickup_datetime,
  c.dropoff_datetime,
  c.passenger_count,
  c.trip_distance,
  c.rate_code,
  c.store_and_fwd_flag,
  c.pu_location_id,
  c.do_location_id,
  c.payment_type,
  pl.payment_type_name,
  c.fare_amount,
  c.extra,
  c.mta_tax,
  c.tip_amount,
  c.tolls_amount,
  c.total_amount,
  c.extracted_at
FROM cleaned c
LEFT JOIN ingestion.payment_lookup pl
  ON c.payment_type = pl.payment_type_id
WHERE c.rn = 1;
