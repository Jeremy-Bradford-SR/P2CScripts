import pyodbc
import os
import re

def get_db_connection():
    """Establishes a database connection using settings from .env-db."""
    # Try looking in common locations
    candidates = [
        os.path.join(os.getcwd(), '.env-db'),
        os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env-db'),
    ]
    
    env_path = None
    for c in candidates:
        if os.path.exists(c):
            env_path = c
            break
            
    conn_str_raw = ""
    if env_path:
        with open(env_path, 'r') as f:
            for line in f:
                if line.startswith("ConnectionStrings__Default="):
                    conn_str_raw = line.strip().split("=", 1)[1]
                    break
    
    if not conn_str_raw:
        print("Could not find ConnectionStrings__Default in .env-db")
        return None

    # Parse connection string
    parts = [p for p in conn_str_raw.split(';') if p]
    params = {}
    for p in parts:
        if '=' in p:
            k, v = p.split('=', 1)
            params[k.strip()] = v.strip()

    driver = "{ODBC Driver 18 for SQL Server}"
    server = params.get('Server')
    database = params.get('Database')
    uid = params.get('User Id')
    pwd = params.get('Password')
    trust_cert = params.get('TrustServerCertificate', 'no')
    if trust_cert.lower() == 'true':
        trust_cert = 'yes'
    
    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};TrustServerCertificate={trust_cert};"
    return pyodbc.connect(conn_str, autocommit=True)

def apply_sql_file(file_path):
    print(f"Applying {file_path}...")
    conn = get_db_connection()
    if not conn:
        print("Failed to connect.")
        return

    try:
        with open(file_path, 'r') as f:
            script = f.read()

        # Split by GO (case insensitive, on its own line)
        # Regex looks for newline, optional whitespace, GO, optional whitespace, newline or end of string
        batches = re.split(r'(?i)^\s*GO\s*$', script, flags=re.MULTILINE)
        
        cursor = conn.cursor()
        for batch in batches:
            if batch.strip():
                print(f"Executing batch ({len(batch)} chars)...")
                try:
                    cursor.execute(batch)
                except Exception as e:
                    print(f"Error executing batch: {e}")
                    # Continue or abort? Abort seems safer for view creation if dependencies fail
                    raise e
        
        print("Successfully applied SQL script.")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        apply_sql_file(sys.argv[1])
    else:
        print("Usage: python apply_sql.py <path_to_sql_file>")
