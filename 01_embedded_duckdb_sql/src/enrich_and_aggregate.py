import logging                                                                                                                    
import os                                                                                                                         
import sys                                                                                                                        
from pathlib import Path                                                                                                          
import duckdb                                                                                                                     
                                                                                                                                    
logging.basicConfig(                                                                                                              
    level=logging.INFO,                                                                                                           
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",                                                                   
    handlers=[logging.StreamHandler(sys.stdout)]                                                                                  
)                                                                                                                                 
logger = logging.getLogger("taxi_weather.enrich")                                                                                 
                                                                                                                                    
BASE_DIR = Path(__file__).resolve().parents[1]                                                                                    
PROCESSED_TRIPS = os.path.join(BASE_DIR, "data/processed/taxi_trips_2026-01.parquet")                                             
RAW_WEATHER = os.path.join(BASE_DIR, "data/raw/weather/weather_2026-01.json")                                                     
PROCESSED_ENRICHED = os.path.join(BASE_DIR, "data/processed/taxi_weather_2026-01.parquet")                                        
GOLD_METRICS = os.path.join(BASE_DIR, "data/gold/hourly_zone_metrics_2026-01.parquet")                                            
                                                                                                                                    
def run_enrichment_and_gold():                                                                                                    
    logger.info("Initializing DuckDB engine for Weather Normalization & Gold Aggregations...")                                    
    con = duckdb.connect()                                                                                                        
                                                                                                                                    
    logger.info("Normalizing weather JSON arrays...")                                                                             
    con.execute(f"""                                                                                                              
    CREATE OR REPLACE TEMP TABLE normalized_weather AS                                                                            
    SELECT                                                                                                                        
        CAST(unnest(hourly.time) AS TIMESTAMP) AS weather_hour,                                                                   
        unnest(hourly.temperature_2m) AS temperature_2m,                                                                          
        unnest(hourly.precipitation) AS precipitation,                                                                            
        unnest(hourly.snowfall) AS snowfall,                                                                                      
        unnest(hourly.wind_speed_10m) AS wind_speed_10m                                                                           
    FROM read_json_auto('{RAW_WEATHER}');                                                                                         
    """)                                                                                                                          
                                                                                                                                    
    logger.info(f"Materializing enriched trips to: {PROCESSED_ENRICHED}")                                                         
    con.execute(f"""                                                                                                              
    COPY (                                                                                                                        
        SELECT                                                                                                                    
            t.*,                                                                                                                  
            w.temperature_2m,                                                                                                     
            w.precipitation,                                                                                                      
            w.snowfall,                                                                                                           
            w.wind_speed_10m                                                                                                      
        FROM read_parquet('{PROCESSED_TRIPS}') t                                                                                  
        LEFT JOIN normalized_weather w                                                                                            
            ON date_trunc('hour', t.tpep_pickup_datetime) = w.weather_hour                                                        
    ) TO '{PROCESSED_ENRICHED}' (FORMAT PARQUET, COMPRESSION SNAPPY);                                                             
    """)                                                                                                                          
                                                                                                                                    
    logger.info(f"Materializing Gold analytics to: {GOLD_METRICS}")                                                               
    con.execute(f"""                                                                                                              
    COPY (                                                                                                                        
        SELECT                                                                                                                    
            date_trunc('hour', t.tpep_pickup_datetime) AS pickup_hour,                                                            
            t.PULocationID AS pickup_zone,                                                                                        
            COUNT(*) AS trip_count,                                                                                               
            ROUND(AVG(t.trip_duration_minutes), 2) AS avg_duration_minutes,                                                       
            ROUND(AVG(t.trip_distance), 2) AS avg_distance,                                                                       
            ROUND(SUM(t.fare_amount), 2) AS total_fare,                                                                           
            ROUND(AVG(w.temperature_2m), 1) AS avg_temp_c,                                                                        
            ROUND(AVG(w.precipitation), 2) AS avg_precipitation_mm,                                                               
            ROUND(AVG(w.wind_speed_10m), 1) AS avg_wind_speed_kmh                                                                 
        FROM read_parquet('{PROCESSED_TRIPS}') t                                                                                  
        LEFT JOIN normalized_weather w                                                                                            
            ON date_trunc('hour', t.tpep_pickup_datetime) = w.weather_hour                                                        
        GROUP BY 1, 2                                                                                                             
        ORDER BY pickup_hour ASC, trip_count DESC                                                                                 
    ) TO '{GOLD_METRICS}' (FORMAT PARQUET, COMPRESSION SNAPPY);                                                                   
    """)                                                                                                                          
                                                                                                                                    
    logger.info("Gold Table Inspection Sample (Top 5 Busiest Hourly Zones):")                                                     
    sample_df = con.execute(f"""                                                                                                  
        SELECT                                                                                                                    
            pickup_hour, pickup_zone, trip_count,                                                                                 
            avg_duration_minutes, avg_distance,                                                                                   
            total_fare, avg_temp_c                                                                                                
        FROM read_parquet('{GOLD_METRICS}')                                                                                       
        ORDER BY trip_count DESC                                                                                                  
        LIMIT 5;                                                                                                                  
    """).df()                                                                                                                     
    logger.info("\n" + sample_df.to_string(index=False))                                                                          
    logger.info("Milestone 5 complete: Enriched Silver and Gold Parquet materialized.")                                           
                                                                                                                                    
if __name__ == "__main__":                                                                                                        
    run_enrichment_and_gold()                                                                                                     
