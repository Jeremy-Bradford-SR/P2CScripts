import pyodbc
import os

def get_db_connection():
    """Establishes a database connection using settings from .env-db."""
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env-db')
    
    conn_str_raw = ""
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                if line.startswith("ConnectionStrings__Default="):
                    conn_str_raw = line.strip().split("=", 1)[1]
                    break
    
    if not conn_str_raw:
        # Fallback for direct execution if .env-db isn't found where expected
        print(f"Warning: .env-db not found at {env_path}")
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

def check_table(table_name):
    conn = get_db_connection()
    if not conn:
        return
    
    cursor = conn.cursor()
    try:
        print(f"--- Schema for {table_name} ---")
        cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{table_name}'")
        rows = cursor.fetchall()
        if not rows:
            print(f"Table {table_name} not found.")
        else:
            for row in rows:
                print(f"{row[0]} ({row[1]})")
    except Exception as e:
        print(f"Error checking {table_name}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_table('cadHandler')
    check_table('DispatchCalls')
    check_table('DailyBulletinArrests')
