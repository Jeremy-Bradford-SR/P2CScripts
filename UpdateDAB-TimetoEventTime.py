import pyodbc
import sys
# Reuse your standard connection string
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=192.168.0.43;"  # Replace with your actual server name
    "DATABASE=p2cdubuque;"  # Replace with your actual database name
    "UID=sa;"  # Replace with your actual username
    "PWD=Thugitout09!;"  # Replace with your actual password
    "TrustServerCertificate=yes;"
)

# Clean and convert 'time' field to datetime
update_query = """
UPDATE dbo.DailyBulletinArrests
SET event_time = TRY_CONVERT(datetime, 
    REPLACE(REPLACE(LTRIM(RTRIM(time)), 'on ', ''), '.', '')
)
WHERE event_time IS NULL
  AND time IS NOT NULL
  AND TRY_CONVERT(datetime, 
    REPLACE(REPLACE(LTRIM(RTRIM(time)), 'on ', ''), '.', '')
) IS NOT NULL;
"""

def update_event_time():
    try:
        with pyodbc.connect(conn_str, timeout=5) as conn:
            with conn.cursor() as cursor:
                cursor.execute(update_query)
                affected = cursor.rowcount
                conn.commit()
                print(f"[OK] Updated {affected} rows in DailyBulletinArrests.")
    except pyodbc.Error as db_err:
        print(f"[ERROR] SQL error: {db_err}")
    except Exception as ex:
        print(f"[ERROR] Unexpected error: {ex}")

if __name__ == "__main__":
    update_event_time()