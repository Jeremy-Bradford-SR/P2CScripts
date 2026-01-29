import pyodbc
import os

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
    return pyodbc.connect(conn_str)

def check_view(view_name):
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
        print(f"--- Checking {view_name} ---")
        
        # Check count
        cursor.execute(f"SELECT SourceType, COUNT(*) as Total, COUNT(lat) as WithGeo FROM {view_name} GROUP BY SourceType")
        print("Counts by SourceType:")
        for row in cursor.fetchall():
            print(f"  {row[0]}: Total={row[1]}, WithGeo={row[2]}")
        
        # Check sample
        print("Sample data (first 3 rows):")
        cursor.execute(f"SELECT TOP 3 * FROM {view_name}")
        columns = [column[0] for column in cursor.description]
        print(columns)
        for row in cursor.fetchall():
            print(row)
            
        # Check for geocoded data
        cursor.execute(f"SELECT COUNT(*) FROM {view_name} WHERE lat IS NOT NULL AND lat != 0")
        geo_count = cursor.fetchval()
        print(f"Rows with geodata: {geo_count}")

    except Exception as e:
        print(f"Error checking {view_name}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_view('vw_AllEvents')
