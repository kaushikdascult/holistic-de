import os                                                                                                                         
import sys                                                                                                                        
from pathlib import Path                                                                                                          
import duckdb                                                                                                                     
                                                                                                                                    
BASE_DIR = Path(__file__).resolve().parents[1]                                                                                    
WAREHOUSE_PATH = os.path.join(BASE_DIR, "warehouse.duckdb")                                                                       
                                                                                                                                    
def execute_query(sql_query: str):                                                                                                
    if not os.path.exists(WAREHOUSE_PATH):                                                                                        
        print(f"Error: Database {WAREHOUSE_PATH} does not exist. Run pipeline.py first.", file=sys.stderr)                        
        sys.exit(1)                                                                                                               
                                                                                                                                    
    con = duckdb.connect(WAREHOUSE_PATH, read_only=True)                                                                          
    try:                                                                                                                          
        print(f"\nExecuting Query:\n{sql_query}\n" + "-"*50)                                                                      
        result_df = con.execute(sql_query).df()                                                                                   
        print(result_df.to_string(index=False))                                                                                   
        print("-" * 50)                                                                                                           
        print(f"Query returned {len(result_df)} rows.\n")                                                                         
    except Exception as e:                                                                                                        
        print(f"Query Execution Error: {e}", file=sys.stderr)                                                                     
        sys.exit(2)                                                                                                               
    finally:                                                                                                                      
        con.close()                                                                                                               
                                                                                                                                    
if __name__ == "__main__":                                                                                                        
    if len(sys.argv) < 2:                                                                                                         
        print("Usage: python query.py \"<SQL_QUERY>\"", file=sys.stderr)                                                          
        sys.exit(1)                                                                                                               
                                                                                                                                    
    query = " ".join(sys.argv[1:])                                                                                                
    execute_query(query)                                                                                                          
