import os
import duckdb

BASE_DIR = os.path.expanduser("~/MyProject/holistic_de/taxi-weather")
RAW_TAXI = os.path.join(BASE_DIR, "data/raw/taxi/yellow_tripdata_2026-01.parquet")
PROCESSED_VALID = os.path.join(BASE_DIR, "data/processed/taxi_trips_2026-01.parquet")
PROCESSED_REJECTED = os.path.join(BASE_DIR, "data/processed/taxi_trips_rejected_2026-01.parquet")

def transform_trips():
    print("Initializing DuckDB in-memory transformation engine...")
    con = duckdb.connect()

    # Step 1: Stage raw trips with derived duration and data quality flags
    dq_query = f"""
    CREATE OR REPLACE TEMP TABLE staged_trips AS
    SELECT
        VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        ROUND(date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0, 2) AS trip_duration_minutes,
        trip_distance,
        PULocationID,
        DOLocationID,
        COALESCE(passenger_count, 1) AS passenger_count,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        Airport_fee,
        cbd_congestion_fee,
        total_amount,
        payment_type,
        CASE
            WHEN tpep_pickup_datetime < '2026-01-01 00:00:00' OR tpep_pickup_datetime >= '2026-02-01 00:00:00'
                THEN 'OUT_OF_BOUNDS_PICKUP_TIMESTAMP'
            WHEN tpep_dropoff_datetime <= tpep_pickup_datetime
                THEN 'INVALID_DROPOFF_BEFORE_PICKUP'
            WHEN date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0 < 1.0
                THEN 'TRIP_TOO_SHORT_UNDER_1_MIN'
            WHEN date_diff('second', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0 > 1440.0
                THEN 'TRIP_TOO_LONG_OVER_24_HOURS'
            WHEN trip_distance <= 0.0 OR trip_distance >= 100.0
                THEN 'INVALID_TRIP_DISTANCE'
            WHEN fare_amount < 2.50 OR fare_amount >= 500.0
                THEN 'INVALID_FARE_AMOUNT'
            WHEN total_amount <= 0.0
                THEN 'INVALID_TOTAL_AMOUNT'
            WHEN PULocationID NOT BETWEEN 1 AND 263 OR DOLocationID NOT BETWEEN 1 AND 263
                THEN 'UNKNOWN_OR_OUT_OF_BOUNDS_ZONE'
            ELSE 'VALID'
        END AS dq_status
    FROM read_parquet('{RAW_TAXI}');
    """
    con.execute(dq_query)

    # Step 2: Materialize Valid Trips to Processed Parquet
    print(f"Writing valid records to: {PROCESSED_VALID}")
    con.execute(f"""
    COPY (
        SELECT * EXCLUDE (dq_status)
        FROM staged_trips
        WHERE dq_status = 'VALID'
    ) TO '{PROCESSED_VALID}' (FORMAT PARQUET, COMPRESSION SNAPPY);
    """)

    # Step 3: Materialize Quarantined / Rejected Trips
    print(f"Writing rejected records to: {PROCESSED_REJECTED}")
    con.execute(f"""
    COPY (
        SELECT *
        FROM staged_trips
        WHERE dq_status != 'VALID'
    ) TO '{PROCESSED_REJECTED}' (FORMAT PARQUET, COMPRESSION SNAPPY);
    """)

    # Step 4: Print Data Observability Summary
    summary_df = con.execute("""
    SELECT 
        dq_status,
        COUNT(*) AS record_count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM staged_trips), 2) AS percentage
    FROM staged_trips
    GROUP BY dq_status
    ORDER BY record_count DESC;
    """).df()

    print("\n=== Data Quality Execution Audit ===")
    print(summary_df.to_string(index=False))
    print("\n✓ Milestone 4 transformation complete.")

if __name__ == "__main__":
    transform_trips()