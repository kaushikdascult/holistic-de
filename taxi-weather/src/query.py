import sys
import duckdb
import os

BASE_DIR = os.path.expanduser("~/MyProject/holistic_de/taxi-weather")
WAREHOUSE_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")

if len(sys.argv) < 2:
    print("Usage: python3 src/query.py \"<SQL_QUERY>\"")
    print("Example: python3 src/query.py \"SELECT * FROM hourly_zone_metrics LIMIT 5;\"")
    sys.exit(1)

query = sys.argv[1]
con = duckdb.connect(WAREHOUSE_PATH)
try:
    print(con.sql(query))
finally:
    con.close()
