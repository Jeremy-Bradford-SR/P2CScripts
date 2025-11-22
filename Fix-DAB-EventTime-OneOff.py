import pyodbc
import sys
import re
from datetime import datetime

# --- CONFIGURATION ---
# This script uses the same connection details as your other scripts.
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=192.168.0.211;"
    "DATABASE=p2cdubuque;"
    "UID=sa;"
    "PWD=Thugitout09!;"
    "TrustServerCertificate=yes;"
)

def parse_time_logic(time_str):
    """
    Applies a series of parsing rules to extract a valid datetime object.
    This logic is more comprehensive to handle multiple formats.
    """
    if not time_str:
        return None

    # Rule 1: Prioritize 'Reported:' time, as it's the most reliable.
    reported_match = re.search(r"Reported:\s*(.+?)\.", time_str)
    if reported_match:
        date_string = reported_match.group(1).strip()
        try:
            return datetime.strptime(date_string, '%H:%M, %m/%d/%Y')
        except ValueError:
            pass # If it fails, proceed to the next rule.

    # Rule 2: If no 'Reported:' time, look for a time after '... and ...'
    between_match = re.search(r"and\s+(.+?)\.", time_str)
    if between_match:
        date_string = between_match.group(1).strip()
        try:
            return datetime.strptime(date_string, '%H:%M, %m/%d/%Y')
        except ValueError:
            pass

    # Rule 3: If neither of the above, look for a simple 'on ...' time.
    on_match = re.search(r"on\s+(.+?)\.", time_str)
    if on_match:
        date_string = on_match.group(1).strip()
        try:
            return datetime.strptime(date_string, '%H:%M, %m/%d/%Y')
        except ValueError:
            pass

    return None # Return None if no rules match.

def fix_incorrect_event_times():
    """
    Fetches records with the incorrect '1900-01-01' date and attempts to fix them.
    """
    updated_count = 0
    failed_count = 0
    try:
        with pyodbc.connect(conn_str, timeout=30) as conn:
            read_cursor = conn.cursor()
            # Fetch only the records that were incorrectly set.
            read_cursor.execute("SELECT id, time FROM dbo.DailyBulletinArrests WHERE event_time = '1900-01-01'")
            rows_to_fix = read_cursor.fetchall()
            read_cursor.close()
            
            print(f"[INFO] Found {len(rows_to_fix)} records with incorrect '1900-01-01' event_time to fix.")

            write_cursor = conn.cursor()
            for row in rows_to_fix:
                event_time = parse_time_logic(row.time)
                if event_time:
                    write_cursor.execute("UPDATE dbo.DailyBulletinArrests SET event_time = ? WHERE id = ?", event_time, row.id)
                    updated_count += 1
            
            if updated_count > 0:
                conn.commit()
            write_cursor.close()

            print(f"\n[SUCCESS] One-off fix script finished.")
            print(f"  - Rows successfully fixed: {updated_count}")

    except pyodbc.Error as db_err:
        print(f"[ERROR] SQL error: {db_err}")
    except Exception as ex:
        print(f"[ERROR] Unexpected error: {ex}")

if __name__ == "__main__":
    fix_incorrect_event_times()