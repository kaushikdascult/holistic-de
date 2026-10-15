import logging                                                                                                                    
import os                                                                                                                         
import sys                                                                                                                        
import time                                                                                                                       
from pathlib import Path                                                                                                          
import duckdb                                                                                                                     
                                                                                                                                    
from download_raw import (                                                                                                        
    create_resilient_session,                                                                                                     
    download_binary,                                                                                                              
    download_weather_json,                                                                                                        
    TAXI_URL,                                                                                                                     
    TAXI_DEST,                                                                                                                    
    WEATHER_URL,                                                                                                                  
    WEATHER_DEST                                                                                                                  
)                                                                                                                                 
from clean_trips import transform_trips                                                                                           
from enrich_and_aggregate import run_enrichment_and_gold                                                                          
                                                                                                                                    
logging.basicConfig(                                                                                                              
    level=logging.INFO,                                                                                                           
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",                                                                   
    handlers=[logging.StreamHandler(sys.stdout)]                                                                                  
)                                                                                                                                 
logger = logging.getLogger("taxi_weather.pipeline")                                                                               
                                                                                                                                    
BASE_DIR = Path(__file__).resolve().parents[1]                                                                                    
WAREHOUSE_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")                                                                       
GOLD_PARQUET = os.path.join(BASE_DIR, "data/gold/hourly_zone_metrics_2026-01.parquet")                                            
                                                                                                                                    
def run_pipeline():                                                                                                               
    start_time = time.time()                                                                                                      
    logger.info("==========================================================")                                                     
    logger.info("Starting End-to-End Taxi-Weather Medallion Pipeline")                                                            
    logger.info("==========================================================")                                                     
                                                                                                                                    
    # Stage 1: Ingestion                                                                                                          
    logger.info("--- [Stage 1/4] Checking Raw Ingestion ---")                                                                     
    session = create_resilient_session()                                                                                          
                                                                                                                                    
    if not os.path.exists(TAXI_DEST):                                                                                             
        try:                                                                                                                      
            download_binary(session, TAXI_URL, TAXI_DEST)                                                                         
        except Exception as e:                                                                                                    
            logger.error(f"Taxi download aborted: {e}")                                                                           
            sys.exit(2)                                                                                                           
    else:                                                                                                                         
        logger.info(f"Taxi raw payload already present ({os.path.getsize(TAXI_DEST) // (1024*1024)} MB)")                         
                                                                                                                                    
    if not os.path.exists(WEATHER_DEST):                                                                                          
        try:                                                                                                                      
            download_weather_json(session, WEATHER_URL, WEATHER_DEST)                                                             
        except Exception as e:                                                                                                    
            logger.error(f"Weather download aborted: {e}")                                                                        
            sys.exit(2)                                                                                                           
    else:                                                                                                                         
        logger.info(f"Weather raw payload already present ({os.path.getsize(WEATHER_DEST) // 1024} KB)")                          
                                                                                                                                    
    # Stage 2: Data Quality & Silver Cleaning                                                                                     
    logger.info("--- [Stage 2/4] Executing DQ Gating & Cleaning ---")                                                             
    transform_trips()                                                                                                             
                                                                                                                                    
    # Stage 3: Feature Enrichment & Gold Mart Materialization                                                                     
    logger.info("--- [Stage 3/4] Executing Weather Join & Gold Aggregations ---")                                                 
    run_enrichment_and_gold()                                                                                                     
                                                                                                                                    
    # Stage 4: Warehouse Catalog Registration                                                                                     
    logger.info("--- [Stage 4/4] Updating Local Warehouse Catalog ---")                                                           
    con = duckdb.connect(WAREHOUSE_PATH)                                                                                          
    con.execute(f"""                                                                                                              
        CREATE OR REPLACE VIEW hourly_zone_metrics AS                                                                             
        SELECT * FROM read_parquet('{GOLD_PARQUET}');                                                                             
    """)                                                                                                                          
    con.close()                                                                                                                   
    logger.info("Persistent views registered in warehouse.duckdb")                                                                
                                                                                                                                    
    total_elapsed = round(time.time() - start_time, 1)                                                                            
    logger.info("==========================================================")                                                     
    logger.info(f"Pipeline Completed Successfully in {total_elapsed}s")                                                           
    logger.info("==========================================================")                                                     
                                                                                                                                    
if __name__ == "__main__":                                                                                                        
    run_pipeline()                                                                                                                
