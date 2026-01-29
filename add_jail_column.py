
import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

MSSQL_SERVER   = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DRIVER   = "{ODBC Driver 18 for SQL Server}"

def get_db_connection():
    conn_str = f"DRIVER={MSSQL_DRIVER};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD};TrustServerCertificate=yes;"
    return pyodbc.connect(conn_str)

def add_column():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # Check if column exists
        cursor.execute("SELECT COL_LENGTH('jail_inmates', 'next_court_date')")
        if cursor.fetchone()[0] is not None:
            print("Column 'next_court_date' already exists.")
        else:
            print("Adding column 'next_court_date'...")
            cursor.execute("ALTER TABLE jail_inmates ADD next_court_date DATETIME NULL")
            conn.commit()
            print("Column added successfully.")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    add_column()
