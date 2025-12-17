import pyodbc
import sys
import re
from datetime import datetime
import os
import dotenv

dotenv.load_dotenv()

MSSQL_SERVER = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")

# Reuse your standard connection string
conn_str = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={MSSQL_SERVER};"
    f"DATABASE={MSSQL_DATABASE};"
    f"UID={MSSQL_USERNAME};"
    f"PWD={MSSQL_PASSWORD};"
    "TrustServerCertificate=yes;"
)

def try_parse_formats(date_string, raw_time_str, rule_name):
    formats = [
        '%H:%M, %m/%d/%Y',       # 14:30, 11/20/2025
        '%m/%d/%Y %I:%M:%S %p',  # 11/20/2025 2:57:00 PM
        '%m/%d/%Y %I:%M %p',     # 11/20/2025 2:57 PM
        '%m/%d/%Y %H:%M',        # 11/22/2025 23:42
        '%m/%d/%Y'               # 11/23/2025
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(date_string, fmt)
            # print(f"MATCH {rule_name}: '{date_string}' (fmt: {fmt}) -> {dt}")
            return dt
        except ValueError:
            continue
    
    # print(f"FAIL {rule_name}: '{date_string}' - No format matched")
    return None

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
        dt = try_parse_formats(date_string, time_str, "Rule 1 (Reported)")
        if dt: return dt

    # Rule 2: Handle 'between...and...' format, taking the second time.
    between_match = re.search(r"and\s+(.+?)\.", time_str)
    if between_match:
        date_string = between_match.group(1).strip()
        dt = try_parse_formats(date_string, time_str, "Rule 2 (Between)")
        if dt: return dt

    # Rule 3: Handle simple 'on...' format.
    on_match = re.search(r"on\s+(.+?)\.", time_str)
    if on_match:
        date_string = on_match.group(1).strip()
        dt = try_parse_formats(date_string, time_str, "Rule 3 (On)")
        if dt: return dt

    # Rule 4: Handle 'On ... at ...' format (e.g. "On 11/15/2025 at 11:00")
    on_at_match = re.search(r"On\s+(.+?)\s+at\s+(.+?)(?:\.|$)", time_str, re.IGNORECASE)
    if on_at_match:
        date_part = on_at_match.group(1).strip()
        time_part = on_at_match.group(2).strip()
        combined = f"{date_part} {time_part}"
        dt = try_parse_formats(combined, time_str, "Rule 4 (On .. at)")
        if dt: return dt

    return None # Return None if no patterns match

def update_event_time(target_ids=None):
    updated_count = 0
    failed_count = 0
    try:
        # print(f"[INFO] Connecting to {MSSQL_SERVER} / {MSSQL_DATABASE}")
        with pyodbc.connect(conn_str, timeout=30) as conn: 
            read_cursor = conn.cursor()
            
            # Diagnostic counts (only if standard run)
            if not target_ids:
                read_cursor.execute("SELECT COUNT(*) FROM dbo.DailyBulletinArrests")
                total_rows = read_cursor.fetchone()[0]
                # print(f"[INFO] Total rows in table: {total_rows}")

                read_cursor.execute("SELECT COUNT(*) FROM dbo.DailyBulletinArrests WHERE event_time IS NULL")
                null_rows = read_cursor.fetchone()[0]
                # print(f"[INFO] Rows with event_time IS NULL: {null_rows}")

            # Step 1: Fetch rows
            base_sql = "SELECT id, time FROM dbo.DailyBulletinArrests WHERE (event_time IS NULL OR event_time = '1900-01-01')"
            params = []
            
            if target_ids:
                # Targeted mode
                placeholders = ', '.join(['?'] * len(target_ids))
                base_sql += f" AND id IN ({placeholders})"
                params.extend(target_ids)
            
            read_cursor.execute(base_sql, params)
            rows_to_process = read_cursor.fetchall()
            read_cursor.close()
            
            if not target_ids:
                print(f"[INFO] Found {len(rows_to_process)} rows with NULL or 1900-01-01 event_time to process.")
            
            if not rows_to_process and target_ids:
                return

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
                    # print(f"FAILED to parse row ID {row.id}: '{row.time}'")
            
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