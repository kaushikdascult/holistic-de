import os
import sys
import urllib.request
import urllib.error

TAXI_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet"
WEATHER_URL = (
    "https://archive-api.open-meteo.com/v1/archive"
    "?latitude=40.7128&longitude=-74.0060"
    "&start_date=2026-01-01&end_date=2026-01-31"
    "&hourly=temperature_2m,precipitation,snowfall,wind_speed_10m"
    "&timezone=America%2FNew_York"
)

BASE_DIR = os.path.expanduser("~/MyProject/holistic_de/taxi-weather")
TAXI_DEST = os.path.join(BASE_DIR, "data/raw/taxi/yellow_tripdata_2026-01.parquet")
WEATHER_DEST = os.path.join(BASE_DIR, "data/raw/weather/weather_2026-01.json")

def download_file(url: str, dest_path: str, timeout: int = 60) -> bool:
    """
    Downloads a remote file with exception handling and corrupt payload purging.
    Returns True on success, False on failure.
    """
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)
    print(f"Connecting to: {url}")
    print(f"Target destination: {dest_path}")

    try:
        # Stream remote content with socket timeout
        with urllib.request.urlopen(url, timeout=timeout) as response:
            with open(dest_path, "wb") as out_file:
                out_file.write(response.read())

        file_size_mb = os.path.getsize(dest_path) / (1024 * 1024)
        print(f"✓ Ingestion successful ({file_size_mb:.1f} MB)\n")
        return True

    except urllib.error.HTTPError as e:
        print(f"✗ HTTP Error {e.code}: {e.reason} for URL: {url}", file=sys.stderr)
    except urllib.error.URLError as e:
        print(f"✗ Network/URL Error: {e.reason} for URL: {url}", file=sys.stderr)
    except TimeoutError:
        print(f"✗ Socket Timeout: Request exceeded {timeout}s threshold for URL: {url}", file=sys.stderr)
    except OSError as e:
        print(f"✗ Filesystem I/O Error: {e} writing to: {dest_path}", file=sys.stderr)
    except Exception as e:
        print(f"✗ Unexpected Error: {e}", file=sys.stderr)

    # Clean up partial / corrupted file on failure
    if os.path.exists(dest_path):
        os.remove(dest_path)
        print(f"  Purged incomplete artifact: {dest_path}", file=sys.stderr)

    print()
    return False

if __name__ == "__main__":
    taxi_success = download_file(TAXI_URL, TAXI_DEST)
    weather_success = download_file(WEATHER_URL, WEATHER_DEST)

    if not (taxi_success and weather_success):
        print("Pipeline aborted: One or more raw ingestions failed.", file=sys.stderr)
        sys.exit(1)
    
    print("All raw ingestions completed successfully.")
