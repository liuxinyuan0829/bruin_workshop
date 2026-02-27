"""@bruin

name: ingestion.trips
type: python
image: python:3.11
connection: duckdb-default

description: Fetch NYC Taxi trip data from TLC public endpoint for specified date range and taxi types.

materialization:
  type: table
  strategy: append

columns:
  - name: VendorID
    type: int64
    description: A code indicating the TPEP provider that provided the record
  - name: tpep_pickup_datetime
    type: timestamp
    description: The date and time when the meter was engaged
  - name: tpep_dropoff_datetime
    type: timestamp
    description: The date and time when the meter was disengaged
  - name: passenger_count
    type: int64
    description: The number of passengers in the vehicle
  - name: trip_distance
    type: float64
    description: The elapsed trip distance in miles
  - name: RatecodeID
    type: int64
    description: The final rate code in effect at the end of the trip
  - name: store_and_fwd_flag
    type: string
    description: This flag indicates whether the trip record was held in vehicle memory
  - name: PULocationID
    type: int64
    description: TLC Taxi Zone in which the pickup occurred
  - name: DOLocationID
    type: int64
    description: TLC Taxi Zone in which the dropoff occurred
  - name: payment_type
    type: int64
    description: A numeric code signifying how the passenger paid for the trip
  - name: fare_amount
    type: float64
    description: The time-and-distance fare calculated by the meter
  - name: extra
    type: float64
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float64
    description: 0.50 MTA tax that is automatically triggered
  - name: tip_amount
    type: float64
    description: Tip amount - automatically populated for credit card tips
  - name: tolls_amount
    type: float64
    description: Total amount of all tolls paid in trip
  - name: total_amount
    type: float64
    description: The total amount charged to the passenger
  - name: extracted_at
    type: timestamp
    description: Timestamp when the record was extracted

@bruin"""

import os
import json
from datetime import datetime, timedelta
from typing import List
import pandas as pd


def get_date_range():
    """Extract start and end dates from environment variables."""
    start_date_str = os.getenv("BRUIN_START_DATE", "")
    end_date_str = os.getenv("BRUIN_END_DATE", "")
    
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d") if start_date_str else datetime.now() - timedelta(days=1)
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d") if end_date_str else datetime.now()
    
    return start_date, end_date


def get_taxi_types() -> List[str]:
    """Extract taxi types from pipeline variables."""
    bruin_vars_str = os.getenv("BRUIN_VARS", "{}")
    bruin_vars = json.loads(bruin_vars_str)
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])
    return taxi_types


def generate_urls(start_date: datetime, end_date: datetime, taxi_types: List[str]) -> List[str]:
    """Generate TLC parquet URLs for the given date range and taxi types."""
    #base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    base_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/"


    urls = []
    
    current = start_date.replace(day=1)
    while current <= end_date:
        year = current.year
        month = current.month
        
        for taxi_type in taxi_types:
            #filename = f"{{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
            filename = f"{taxi_type}/{taxi_type}_tripdata_{year:04d}-{month:02d}.csv.gz"
            url = base_url + filename
            urls.append(url)
        
        # Move to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    
    return urls


def materialize():
    """
    Fetch NYC Taxi trip data from TLC public endpoint.
    
    Uses date range (BRUIN_START_DATE/BRUIN_END_DATE) and taxi types from pipeline variables
    to download parquet files from the TLC endpoint and return raw data.
    """
    start_date, end_date = get_date_range()
    taxi_types = get_taxi_types()
    urls = generate_urls(start_date, end_date, taxi_types)
    
    print(f"Ingesting NYC Taxi data from {start_date.date()} to {end_date.date()}")
    print(f"Taxi types: {taxi_types}")
    print(f"Fetching {len(urls)} files...")
    
    dataframes = []
    extracted_at = datetime.utcnow()
    
    for url in urls:
        try:
            print(f"  Fetching: {url}")
            #df = pd.read_parquet(url)
            df = pd.read_csv(url)
            df["extracted_at"] = extracted_at
            dataframes.append(df)
            print(f"    ✓ Loaded {len(df):,} rows")
        except Exception as e:
            print(f"    ✗ Error: {e}")
    
    if not dataframes:
        print("No data fetched. Returning empty DataFrame.")
        return pd.DataFrame()
    
    final_df = pd.concat(dataframes, ignore_index=True)
    print(f"\nTotal rows ingested: {len(final_df):,}")
    
    return final_df


