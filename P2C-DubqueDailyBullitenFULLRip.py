import requests
import random
import time
import pyodbc
import sys
import concurrent.futures
import threading
import uuid # Import the UUID library
from datetime import datetime, timedelta

# --- CONFIGURATION ---
MSSQL_SERVER   = "192.168.0.43"
MSSQL_DATABASE = "p2cdubuque"
MSSQL_USERNAME = "sa"
MSSQL_PASSWORD = "Thugitout09!" # ⚠️ For security, use environment variables in production
MSSQL_DRIVER   = "{ODBC Driver 18 for SQL Server}"

DATA_URL = "http://p2c.cityofdubuque.org/jqHandler.ashx?op=s"
DAILY_BULLETIN_URL = "http://p2c.cityofdubuque.org/dailybulletin.aspx"

# --- DATE & CONCURRENCY CONFIGURATION ---
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime.now()
MAX_WORKERS = 5 # Number of threads to run in parallel
CHUNK_SIZE = 5  # Number of days to process in each batch

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
]

# --- Global variables for thread-safe operations ---
total_inserted = 0
total_skipped = 0
data_lock = threading.Lock()

def status(step, message):
    """Prints a timestamped status message."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{step}] {message}")

def get_session(user_agent):
    """Acquires a new session ID from the website."""
    headers = {"User-Agent": user_agent}
    try:
        resp = requests.get(DAILY_BULLETIN_URL, headers=headers, timeout=10)
        resp.raise_for_status()
        if "ASP.NET_SessionId" in resp.cookies:
            return resp.cookies.get("ASP.NET_SessionId")
    except requests.RequestException as e:
        print(f"[WARN] Could not get a session ID: {e}")
    return None

def daterange(start_date, end_date):
    """Generator for iterating through a range of dates."""
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def generate_unique_id(existing_ids):
    """Generates a unique, string-based ID to prevent collisions."""
    while True:
        # Generate a unique string ID, e.g., "gen_a1b2c3d4-..."
        new_id = f"gen_{uuid.uuid4()}"
        with data_lock:
            if new_id not in existing_ids:
                existing_ids.add(new_id)
                return new_id

# --- WORKER FUNCTION (This is what each thread runs) ---
def process_day(current_date, existing_ids):
    """
    Scrapes all pages for a single day and inserts new records into the database.
    """
    global total_inserted, total_skipped

    date_str = current_date.strftime("%m/%d/%Y")
    thread_name = threading.current_thread().name
    status(thread_name, f"Starting date: {date_str}")

    conn_str = f"DRIVER={MSSQL_DRIVER};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD};TrustServerCertificate=yes;"
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
    except pyodbc.Error as e:
        status(thread_name, f"FATAL: Could not connect to DB. Aborting thread. Error: {e}")
        return

    current_user_agent = random.choice(USER_AGENTS)
    session_id = get_session(current_user_agent)
    if not session_id:
        status(thread_name, f"Could not get a session ID for {date_str}. Skipping this day.")
        conn.close()
        return

    page_num = 1
    daily_inserted, daily_skipped = 0, 0

    while True:
        payload = { "t": "db", "d": date_str, "_search": "false", "nd": int(time.time() * 1000), "rows": 100, "page": page_num, "sidx": "case", "sord": "asc" }
        headers = { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "Origin": "http://p2c.cityofdubuque.org", "Referer": "http://p2c.cityofdubuque.org/dailybulletin.aspx", "X-Requested-With": "XMLHttpRequest", "User-Agent": current_user_agent }
        cookies = {"ASP.NET_SessionId": session_id}

        try:
            r = requests.post(DATA_URL, data=payload, headers=headers, cookies=cookies, timeout=15)
            r.raise_for_status()
            rows = r.json().get("rows", [])
        except requests.RequestException as e:
            status(thread_name, f"Request failed for {date_str} page {page_num}: {e}. Stopping work on this day.")
            break

        if not rows:
            break

        for record in rows:
            # --- MODIFIED LOGIC FOR STRING ID ---
            # Get the ID as a string and clean it
            original_id_str = str(record.get('id', '')).strip()

            if not original_id_str or original_id_str == '&nbsp;':
                # Original ID is missing or invalid, generate a new one
                rec_id = generate_unique_id(existing_ids)
                status(thread_name, f"Replaced invalid ID '{original_id_str}' with new ID {rec_id}")
            else:
                # Use the original string ID
                rec_id = original_id_str
            # --- END OF MODIFIED LOGIC ---

            with data_lock:
                if rec_id in existing_ids:
                    daily_skipped += 1
                    continue
                existing_ids.add(rec_id)
            
            try:
                insert_sql = "INSERT INTO dbo.Arrests (invid, [key], location, id, name, crime, [time], property, officer, [case], description, race, sex, lastname, firstname, charge, middlename) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                cursor.execute(insert_sql, (record.get("invid"), record.get("key"), record.get("location"), rec_id, record.get("name"), record.get("crime"), record.get("time"), record.get("property"), record.get("officer"), record.get("case"), record.get("description"), record.get("race"), record.get("sex"), record.get("lastname"), record.get("firstname"), record.get("charge"), record.get("middlename")))
                daily_inserted += 1
            except pyodbc.Error as db_err:
                status(thread_name, f"[WARN] DB insert failed for ID {rec_id}. Error: {db_err}")
                daily_skipped += 1 
                continue

        conn.commit()
        page_num += 1
        time.sleep(0.5)

    with data_lock:
        total_inserted += daily_inserted
        total_skipped += daily_skipped

    status(thread_name, f"Finished {date_str}. Inserted: {daily_inserted}, Skipped: {daily_skipped}")
    conn.close()

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    status("Main", "Connecting to DB to fetch all existing source IDs...")
    conn_str = f"DRIVER={MSSQL_DRIVER};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD};TrustServerCertificate=yes;"
    try:
        main_conn = pyodbc.connect(conn_str)
        cursor = main_conn.cursor()
        cursor.execute("SELECT id FROM dbo.Arrests")
        # existing_ids will now be a set of strings, which is perfectly fine.
        existing_ids = {row.id for row in cursor.fetchall()}
        cursor.close()
        main_conn.close()
        status("Main", f"Loaded {len(existing_ids)} existing source IDs into memory.")
    except Exception as e:
        status("Main", f"FATAL: Could not fetch initial IDs from database: {e}")
        sys.exit(1)

    dates_to_process = list(daterange(START_DATE, END_DATE))
    status("Main", f"Beginning scrape for {len(dates_to_process)} days in batches of {CHUNK_SIZE}.")

    for i in range(0, len(dates_to_process), CHUNK_SIZE):
        date_chunk = dates_to_process[i:i + CHUNK_SIZE]
        start_date_str = date_chunk[0].strftime('%Y-%m-%d')
        end_date_str = date_chunk[-1].strftime('%Y-%m-%d')
        status("Main", f"--- Processing batch: {start_date_str} to {end_date_str} ---")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_day, date, existing_ids) for date in date_chunk]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    status("Main", f"A worker thread generated an exception: {exc}")

        status("Main", f"--- Finished batch. Pausing before next one... ---")
        time.sleep(random.uniform(5, 10))

    status("Complete", "Historical data import has finished.")
    print("\n" + "="*25)
    print("   SCRAPING SUMMARY   ")
    print("="*25)
    print(f"  Total New Records Inserted: {total_inserted}")
    print(f"  Total Duplicate Records Skipped: {total_skipped}")
    print("="*25 + "\n")
