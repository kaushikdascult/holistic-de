import sys
import time
from download_raw import download_file, TAXI_URL, TAXI_DEST, WEATHER_URL, WEATHER_DEST
from clean_trips import transform_trips
from enrich_and_aggregate import run_enrichment_and_gold
import duckdb
import os

BASE_DIR = os.path.expanduser("~/MyProject/holistic_de/taxi-weather")
WAREHOUSE_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")
GOLD_PARQUET = os.path.join(BASE_DIR, "data/gold/hourly_zone_metrics_2026-01.parquet")

def run_pipeline():
    start_time = time.time()
    print("==========================================================")
    print("🚀 Starting End-to-End Taxi-Weather Medallion Pipeline")
    print("==========================================================\n")

    # Stage 1: Ingestion
    print("--- [Stage 1/4] Checking Raw Ingestion ---")
    if not os.path.exists(TAXI_DEST):
        if not download_file(TAXI_URL, TAXI_DEST):
            sys.exit(1)
    else:
        print(f"✓ Taxi raw payload already present ({os.path.getsize(TAXI_DEST) // (1024*1024)} MB)")

    if not os.path.exists(WEATHER_DEST):
        if not download_file(WEATHER_URL, WEATHER_DEST):
            sys.exit(1)
    else:
        print(f"✓ Weather raw payload already present ({os.path.getsize(WEATHER_DEST) // 1024} KB)")

    # Stage 2: Data Quality & Silver Cleaning
    print("\n--- [Stage 2/4] Executing DQ Gating & Cleaning ---")
    transform_trips()

    # Stage 3: Feature Enrichment & Gold Mart Materialization
    print("\n--- [Stage 3/4] Executing Weather Join & Gold Aggregations ---")
    run_enrichment_and_gold()

    # Stage 4: Warehouse Catalog Registration
    print("\n--- [Stage 4/4] Updating Local Warehouse Catalog ---")
    con = duckdb.connect(WAREHOUSE_PATH)
    con.execute(f"""
        CREATE OR REPLACE VIEW hourly_zone_metrics AS
        SELECT * FROM read_parquet('{GOLD_PARQUET}');
    """)
    con.close()
    print("✓ Persistent views registered in warehouse.duckdb")

    total_elapsed = round(time.time() - start_time, 1)
    print("\n==========================================================")
    print(f"✅ Pipeline Completed Successfully in {total_elapsed}s")
    print("==========================================================")

if __name__ == "__main__":
    run_pipeline()