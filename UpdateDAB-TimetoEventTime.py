import pyodbc
import sys
import re
from datetime import datetime

# Reuse your standard connection string
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=192.168.0.211;"  # Reverted to the correct server IP
    "DATABASE=p2cdubuque;"  # Replace with your actual database name
    "UID=sa;"  # Replace with your actual username
    "PWD=Thugitout09!;"  # Replace with your actual password
    "TrustServerCertificate=yes;"
)

def parse_time_with_regex(time_str):
    """
    Uses regex to find the timestamp after 'Reported: '.
    Returns a datetime object if successful, otherwise tries other patterns.
    """
    if not time_str:
        return None
    
    # Rule 1: Prioritize 'Reported:' time.
    reported_match = re.search(r"Reported:\s*(.+?)\.", time_str)
    if reported_match:
        date_string = reported_match.group(1).strip()
        try:
            return datetime.strptime(date_string, '%H:%M, %m/%d/%Y')
        except ValueError:
            pass # Fall through to the next rule if parsing fails

    # Rule 2: Handle 'between...and...' format, taking the second time.
    between_match = re.search(r"and\s+(.+?)\.", time_str)
    if between_match:
        date_string = between_match.group(1).strip()
        try:
            return datetime.strptime(date_string, '%H:%M, %m/%d/%Y')
        except ValueError:
            pass

    # Rule 3: Handle simple 'on...' format.
    on_match = re.search(r"on\s+(.+?)\.", time_str)
    if on_match:
        date_string = on_match.group(1).strip()
        try:
            return datetime.strptime(date_string, '%H:%M, %m/%d/%Y')
        except ValueError:
            pass

    return None # Return None if no patterns match

def update_event_time():
    updated_count = 0
    failed_count = 0
    try:
        with pyodbc.connect(conn_str, timeout=30) as conn: # Increased timeout to 30 seconds
            read_cursor = conn.cursor()
            # Step 1: Fetch all rows that need processing
            read_cursor.execute("SELECT id, time FROM dbo.DailyBulletinArrests WHERE event_time IS NULL OR event_time = '1900-01-01'")
            rows_to_process = read_cursor.fetchall()
            read_cursor.close()
            
            print(f"[INFO] Found {len(rows_to_process)} rows with NULL event_time to process.")

            write_cursor = conn.cursor()
            # Step 2: Process each row in Python
            for row in rows_to_process:
                event_time = parse_time_with_regex(row.time)
                if event_time:
                    # Step 3: If parsing is successful, execute a targeted UPDATE
                    write_cursor.execute("UPDATE dbo.DailyBulletinArrests SET event_time = ? WHERE id = ?", event_time, row.id)
                    updated_count += 1
                else:
                    failed_count += 1
            
            if updated_count > 0:
                conn.commit()
            write_cursor.close()

            print(f"\n[SUCCESS] Script finished.")
            print(f"  - Rows successfully updated: {updated_count}")
            print(f"  - Rows that could not be parsed: {failed_count}")

    except pyodbc.Error as db_err:
        print(f"[ERROR] SQL error: {db_err}")
    except Exception as ex:
        print(f"[ERROR] Unexpected error: {ex}")

if __name__ == "__main__":
    update_event_time()