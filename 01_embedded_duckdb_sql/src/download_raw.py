import json                                                                                                                       
import logging                                                                                                                    
import os                                                                                                                         
import sys                                                                                                                        
from datetime import datetime, timezone                                                                                           
from pathlib import Path                                                                                                          
                                                                                                                                    
import requests                                                                                                                   
from requests.adapters import HTTPAdapter                                                                                         
from urllib3.util import Retry                                                                                                    
                                                                                                                                    
# Configure structured logging                                                                                                    
logging.basicConfig(                                                                                                              
    level=logging.INFO,                                                                                                           
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",                                                                   
    handlers=[logging.StreamHandler(sys.stdout)]                                                                                  
)                                                                                                                                 
logger = logging.getLogger("taxi_weather.ingest")                                                                                 
                                                                                                                                    
TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"                                      
WEATHER_URL = (                                                                                                                   
    "https://archive-api.open-meteo.com/v1/archive"                                                                               
    "?latitude=40.7128&longitude=-74.0060"                                                                                        
    "&start_date=2026-01-01&end_date=2026-01-31"                                                                                  
    "&hourly=temperature_2m,precipitation,snowfall,wind_speed_10m"                                                                
    "&timezone=America%2FNew_York"                                                                                                
)                                                                                                                                 
                                                                                                                                    
# Dynamic root resolution (independent of folder moves)                                                                           
BASE_DIR = Path(__file__).resolve().parents[1]                                                                                    
TAXI_DEST = os.path.join(BASE_DIR, "data/raw/taxi/yellow_tripdata_2026-01.parquet")                                               
WEATHER_DEST = os.path.join(BASE_DIR, "data/raw/weather/weather_2026-01.json")                                                    
                                                                                                                                    
def create_resilient_session(retries: int = 3, backoff_factor: float = 1.0) -> requests.Session:                                  
    """Configures a requests Session with automated exponential backoff on transient HTTP errors."""                              
    session = requests.Session()                                                                                                  
    retry_strategy = Retry(                                                                                                       
        total=retries,                                                                                                            
        backoff_factor=backoff_factor,                                                                                            
        status_forcelist=[429, 500, 502, 503, 504],                                                                               
        allowed_methods=["GET"]                                                                                                   
    )                                                                                                                             
    adapter = HTTPAdapter(max_retries=retry_strategy)                                                                             
    session.mount("https://", adapter)                                                                                            
    session.mount("http://", adapter)                                                                                             
    return session                                                                                                                
                                                                                                                                    
def download_binary(session: requests.Session, url: str, dest_path: str, timeout: int = 60) -> None:                              
    """Downloads a remote binary file atomically with exception propagation."""                                                   
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)                                                                        
    logger.info(f"Initiating binary download: {url}")                                                                             
    tmp_path = dest_path + ".tmp"                                                                                                 
    try:                                                                                                                          
        response = session.get(url, timeout=timeout)                                                                              
        response.raise_for_status()                                                                                               
        with open(tmp_path, "wb") as f:                                                                                           
            f.write(response.content)                                                                                             
        os.replace(tmp_path, dest_path)                                                                                           
        size_mb = os.path.getsize(dest_path) / (1024 * 1024)                                                                      
        logger.info(f"Binary ingestion complete: {dest_path} ({size_mb:.1f} MB)")                                                 
    except Exception:                                                                                                             
        if os.path.exists(tmp_path):                                                                                              
            os.remove(tmp_path)                                                                                                   
        raise                                                                                                                     
                                                                                                                                    
def download_weather_json(session: requests.Session, url: str, dest_path: str, timeout: int = 30) -> None:                        
    """Downloads weather JSON, stamps ingestion audit metadata, and writes atomically."""                                         
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)                                                                        
    logger.info(f"Initiating weather JSON extraction: {url}")                                                                     
    tmp_path = dest_path + ".tmp"                                                                                                 
    try:                                                                                                                          
        response = session.get(url, timeout=timeout)                                                                              
        response.raise_for_status()                                                                                               
        payload = response.json()                                                                                                 
                                                                                                                                    
        # Ingestion audit metadata                                                                                                
        payload["_ingested_at_utc"] = datetime.now(timezone.utc).isoformat()                                                      
        payload["_source"] = "open_meteo_archive_api"                                                                             
        payload["_source_url"] = url                                                                                              
                                                                                                                                    
        with open(tmp_path, "w", encoding="utf-8") as f:                                                                          
            json.dump(payload, f, indent=2)                                                                                       
        os.replace(tmp_path, dest_path)                                                                                           
        logger.info(f"Weather JSON ingestion complete: {dest_path}")                                                              
    except Exception:                                                                                                             
        if os.path.exists(tmp_path):                                                                                              
            os.remove(tmp_path)                                                                                                   
        raise                                                                                                                     
                                                                                                                                    
if __name__ == "__main__":                                                                                                        
    session = create_resilient_session()                                                                                          
                                                                                                                                    
    try:                                                                                                                          
        download_binary(session, TAXI_URL, TAXI_DEST)                                                                             
    except requests.exceptions.Timeout:                                                                                           
        logger.error("Taxi download timed out.")                                                                                  
        sys.exit(1)                                                                                                               
    except requests.exceptions.HTTPError as e:                                                                                    
        logger.error(f"Taxi download HTTP error: {e}")                                                                            
        sys.exit(2)                                                                                                               
    except OSError as e:                                                                                                          
        logger.error(f"Taxi download filesystem I/O error: {e}")                                                                  
        sys.exit(3)                                                                                                               
                                                                                                                                    
    try:                                                                                                                          
        download_weather_json(session, WEATHER_URL, WEATHER_DEST)                                                                 
    except requests.exceptions.Timeout:                                                                                           
        logger.error("Weather download timed out.")                                                                               
        sys.exit(1)                                                                                                               
    except requests.exceptions.HTTPError as e:                                                                                    
        logger.error(f"Weather download HTTP error: {e}")                                                                         
        sys.exit(2)                                                                                                               
    except (json.JSONDecodeError, KeyError) as e:                                                                                 
        logger.error(f"Weather JSON schema/deserialization error: {e}")                                                           
        sys.exit(4)                                                                                                               
    except OSError as e:                                                                                                          
        logger.error(f"Weather download filesystem I/O error: {e}")                                                               
        sys.exit(3)                                                                                                               
                                                                                                                                    
    logger.info("All raw ingestions completed successfully.")                                                                     
