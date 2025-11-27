import requests
import random
import time
from bs4 import BeautifulSoup
from urllib.parse import quote_plus
import pyodbc
import sys
import concurrent.futures
import threading
from datetime import datetime, timedelta
import argparse
import os
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
MSSQL_SERVER   = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DRIVER   = "{ODBC Driver 18 for SQL Server}"

DATA_URL = "http://p2c.cityofdubuque.org/jqHandler.ashx?op=s"
SESSION_INIT_URL = "http://p2c.cityofdubuque.org/main.aspx" # Use main.aspx to establish the session
DAILY_BULLETIN_URL = "http://p2c.cityofdubuque.org/dailybulletin.aspx"
PROXY_LIST_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"

# --- DATE & CONCURRENCY CONFIGURATION ---
# Default values (can be overridden by command line arguments)
DEFAULT_DAYS_TO_SCRAPE = 7
DEFAULT_MAX_WORKERS = 7 # Number of threads to run in parallel
DEFAULT_CHUNK_SIZE = 7 # Process 7 days at a time
MAX_RETRIES_PER_DAY = 5 # Number of times to retry a day with a fresh proxy if it fails

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 Edg/125.0.2535.67",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/125.0.6422.80 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 14) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.6422.90 Mobile Safari/537.36",
]

# --- Global variables for thread-safe operations ---
total_inserted = 0
total_skipped = 0
data_lock = threading.Lock()

def status(step, message):
    """Prints a timestamped status message."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{step}] {message}")

def check_proxy(proxy):
    """Tests a single proxy against a reliable target."""
    test_url = "http://example.com"
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    try:
        requests.get(test_url, proxies=proxies_dict, timeout=3)
        return proxy
    except:
        return None

def validate_proxies(proxies_list, batch_size=100):
    """Validates a list of proxies in parallel to find working ones."""
    random.shuffle(proxies_list)
    valid_proxies = []
    for i in range(0, len(proxies_list), batch_size):
        batch = proxies_list[i:i + batch_size]
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            results = list(executor.map(check_proxy, batch))
        valid_batch = [proxy for proxy in results if proxy]
        valid_proxies.extend(valid_batch)
        if len(valid_proxies) >= 1000: # Stop if we have a decent number
            break
    return valid_proxies

def get_fresh_session(user_agent, proxy_pool):
    """
    Acquires a new, fresh requests.Session object with a valid ASP.NET_SessionId.
    It will only use proxies. If all proxies fail, it returns None.
    """
    headers = {"User-Agent": user_agent}
    
    # Create a copy of the pool to try each proxy once without affecting other threads
    local_proxy_pool = list(proxy_pool)
    random.shuffle(local_proxy_pool)

    for proxy in local_proxy_pool:
        proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
        try:
            session = requests.Session()
            # Set a longer timeout for the initial, critical session request
            resp = session.get(SESSION_INIT_URL, headers=headers, proxies=proxies_dict, timeout=20)
            resp.raise_for_status()
            if "ASP.NET_SessionId" in session.cookies:
                return session, proxy # Return the session and the working proxy
        except requests.RequestException:
            continue # Try next proxy
    # If the loop completes without success, return None
    return None, None

def daterange(start_date, end_date):
    """Generator for iterating through a range of dates."""
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

# --- WORKER FUNCTION (This is what each thread runs) ---
def process_day(current_date, existing_ids, valid_proxies):
    """
    Scrapes all pages for a single day and inserts new records into the database.
    Retries with a fresh proxy if a critical failure occurs.
    """
    global total_inserted, total_skipped

    date_str = current_date.strftime("%m/%d/%Y")
    thread_name = threading.current_thread().name
    
    conn_str = f"DRIVER={MSSQL_DRIVER};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD};TrustServerCertificate=yes;"

    for attempt in range(1, MAX_RETRIES_PER_DAY + 1):
        status(thread_name, f"Starting date: {date_str} (Attempt {attempt}/{MAX_RETRIES_PER_DAY})")
        
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
        except pyodbc.Error as e:
            status(thread_name, f"FATAL: Could not connect to DB. Aborting thread. Error: {e}")
            return # DB error is likely fatal for all retries, or we could retry, but usually it's config

        current_user_agent = random.choice(USER_AGENTS)
        session, proxy_in_use = get_fresh_session(current_user_agent, valid_proxies)

        if not session:
            status(thread_name, f"FATAL: Could not get a session for {date_str}. Retrying...")
            conn.close()
            continue # Retry loop
        
        if proxy_in_use:
            status(thread_name, f"Acquired fresh session for {date_str} using proxy {proxy_in_use}")
        else:
            status(thread_name, f"Acquired fresh session for {date_str} using a direct connection.")

        # Define headers once, to be used for all subsequent requests in this thread.
        headers = { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "Origin": "http://p2c.cityofdubuque.org", "Referer": "http://p2c.cityofdubuque.org/dailybulletin.aspx", "X-Requested-With": "XMLHttpRequest", "User-Agent": current_user_agent }

        # --- CORRECTED SESSION INITIALIZATION FLOW ---
        try:
            # Step 1: GET the bulletin page to scrape hidden form fields
            get_headers = {'User-Agent': current_user_agent, 'Referer': SESSION_INIT_URL}
            proxies_dict = {"http": proxy_in_use, "https": proxy_in_use} if proxy_in_use else None
            
            form_page_resp = session.get(DAILY_BULLETIN_URL, headers=get_headers, proxies=proxies_dict, timeout=15)
            form_page_resp.raise_for_status()
            soup = BeautifulSoup(form_page_resp.text, 'html.parser')

            # Step 2: Manually build the form data as a raw string to prevent URL encoding of the date.
            # This is the definitive fix for the server ignoring the date change.
            viewstate = quote_plus(soup.find('input', {'name': '__VIEWSTATE'}).get('value', ''))
            viewstategen = quote_plus(soup.find('input', {'name': '__VIEWSTATEGENERATOR'}).get('value', ''))
            eventvalidation = quote_plus(soup.find('input', {'name': '__EVENTVALIDATION'}).get('value', ''))

            raw_form_data = (
                f"__EVENTTARGET=MasterPage%24mainContent%24lbUpdate&__VIEWSTATE={viewstate}&__VIEWSTATEGENERATOR={viewstategen}"
                f"&__EVENTVALIDATION={eventvalidation}&MasterPage%24mainContent%24ddlType2=AL&MasterPage%24mainContent%24txtDate2={quote_plus(date_str)}"
            )

            # Step 3: POST back to the bulletin page to set the date on the server
            post_headers = {'User-Agent': current_user_agent, 'Referer': DAILY_BULLETIN_URL, 'Content-Type': 'application/x-www-form-urlencoded'}
            set_date_resp = session.post(DAILY_BULLETIN_URL, data=raw_form_data, headers=post_headers, proxies=proxies_dict, timeout=20)
            set_date_resp.raise_for_status()

        except requests.RequestException as e:
            status(thread_name, f"Failed to initialize form for {date_str}: {e}. Retrying...")
            conn.close()
            continue # Retry loop

        page_num = 1
        daily_inserted, daily_skipped = 0, 0
        day_success = True

        while True:
            payload = { "t": "db", "d": date_str, "_search": "false", "nd": int(time.time() * 1000), "rows": 10000, "page": page_num, "sidx": "case", "sord": "asc" } # Request max rows
            proxies_dict = {"http": proxy_in_use, "https": proxy_in_use} if proxy_in_use else None
            
            page_data = None
            try:
                # Use the SAME session object that was used to set the date.
                r = session.post(DATA_URL, data=payload, headers=headers, proxies=proxies_dict, timeout=20)
                r.raise_for_status()
                page_data = r.json()
            except requests.RequestException as e:
                status(thread_name, f"Data request failed for {date_str} page {page_num}: {e}. Retrying day...")
                day_success = False
                break # Break inner loop to retry the day

            rows = page_data.get("rows", [])

            if not rows:
                break # No more rows on this page, so we're done with this date.

            for record in rows:
                try:
                    # Treat the ID as a string to prevent overflow errors.
                    rec_id = str(record.get('id', '') or '').strip()
                except (ValueError, TypeError):
                    daily_skipped += 1
                    continue # Skip records with invalid IDs

                with data_lock:
                    if rec_id in existing_ids:
                        daily_skipped += 1
                        continue
                    existing_ids.add(rec_id)
                
                try:
                    insert_sql = "INSERT INTO dbo.DailyBulletinArrests (invid, [key], location, id, name, crime, [time], property, officer, [case], description, race, sex, lastname, firstname, charge, middlename) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                    cursor.execute(insert_sql, (record.get("invid"), record.get("key"), record.get("location"), rec_id, record.get("name"), record.get("crime"), record.get("time"), record.get("property"), record.get("officer"), record.get("case"), record.get("description"), record.get("race"), record.get("sex"), record.get("lastname"), record.get("firstname"), record.get("charge"), record.get("middlename")))
                    daily_inserted += 1
                except pyodbc.Error as db_err:
                    status(thread_name, f"[WARN] DB insert failed for ID {rec_id}. Error: {db_err}")
                    daily_skipped += 1 
                    continue

            conn.commit()
            page_num += 1
            time.sleep(0.5) # Be polite between page requests

        if day_success:
            with data_lock:
                total_inserted += daily_inserted
                total_skipped += daily_skipped

            status(thread_name, f"Finished {date_str}. Inserted: {daily_inserted}, Skipped: {daily_skipped}")
            conn.close()
            return # Success, exit function
        else:
            conn.close()
            # Loop continues to next attempt

    status(thread_name, f"FATAL: Failed to process {date_str} after {MAX_RETRIES_PER_DAY} attempts.")

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="P2C Scraper")
    parser.add_argument("--DAYS_TO_SCRAPE", type=int, default=DEFAULT_DAYS_TO_SCRAPE, help="Number of days to scrape")
    parser.add_argument("--MAX_WORKERS", type=int, default=DEFAULT_MAX_WORKERS, help="Number of worker threads")
    parser.add_argument("--CHUNK_SIZE", type=int, default=DEFAULT_CHUNK_SIZE, help="Number of days per batch")
    args = parser.parse_args()

    DAYS_TO_SCRAPE = args.DAYS_TO_SCRAPE
    MAX_WORKERS = args.MAX_WORKERS
    CHUNK_SIZE = args.CHUNK_SIZE

    status("Main", f"Configuration: Days={DAYS_TO_SCRAPE}, Workers={MAX_WORKERS}, ChunkSize={CHUNK_SIZE}")
    status("Main", "Fetching and validating proxy list...")
    try:
        proxy_resp = requests.get(PROXY_LIST_URL, timeout=10)
        proxy_resp.raise_for_status()
        proxies_list = [line.split("://")[-1].strip() for line in proxy_resp.text.splitlines() if line.strip()]
        status("Main", f"Retrieved {len(proxies_list)} raw proxies.")
        valid_proxies = validate_proxies(proxies_list)
        status("Main", f"Found {len(valid_proxies)} working proxies to use in the pool.")
    except Exception as e:
        status("Main", f"[WARN] Could not fetch or validate proxy list: {e}. Proceeding with direct connections only.")
        valid_proxies = []

    if not valid_proxies:
        status("Main", "No working proxies found. The script will rely on direct connections.")


    status("Main", "Connecting to DB to fetch all existing record IDs...")
    conn_str = f"DRIVER={MSSQL_DRIVER};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD};TrustServerCertificate=yes;"
    try:
        main_conn = pyodbc.connect(conn_str)
        cursor = main_conn.cursor()
        cursor.execute("SELECT id FROM dbo.DailyBulletinArrests")
        # Ensure all existing IDs are treated as strings for consistent comparison.
        existing_ids = {str(row.id) for row in cursor.fetchall()}
        cursor.close()
        main_conn.close()
        status("Main", f"Loaded {len(existing_ids)} existing record IDs into memory.")
    except Exception as e:
        status("Main", f"FATAL: Could not fetch initial IDs from database: {e}")
        sys.exit(1)

    # Calculate the date range for the last 7 days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_TO_SCRAPE - 1)
    dates_to_process = list(daterange(start_date, end_date))
    
    start_date_str = dates_to_process[0].strftime('%Y-%m-%d')
    end_date_str = dates_to_process[-1].strftime('%Y-%m-%d')
    status("Main", f"Beginning scrape for date range: {start_date_str} to {end_date_str}")

    # Process dates in clean batches to ensure stability
    for i in range(0, len(dates_to_process), CHUNK_SIZE):
        date_chunk = dates_to_process[i:i + CHUNK_SIZE]
        batch_start_str = date_chunk[0].strftime('%Y-%m-%d')
        batch_end_str = date_chunk[-1].strftime('%Y-%m-%d')
        status("Main", f"--- Processing Batch: {batch_start_str} to {batch_end_str} ---")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Assign each day in the batch to a worker thread
            futures = [executor.submit(process_day, date, existing_ids, valid_proxies) for date in date_chunk]
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as exc:
                    status("Main", f"A worker thread generated an exception: {exc}")
        status("Main", f"--- Finished Batch. Pausing before next... ---")
        time.sleep(5) # Brief pause between batches

    status("Complete", "Scraping job has finished.")
    print("\n" + "="*30)
    print("      SCRAPING SUMMARY")
    print("="*30)
    print(f"  Total New Records Inserted: {total_inserted}")
    print(f"  Total Duplicate Records Skipped: {total_skipped}")
    print("="*30 + "\n")

    # --- ADDED: Date Count Summary ---
    status("Analysis", "Fetching record counts for the scraped date range...")
    try:
        analysis_conn = pyodbc.connect(conn_str)
        analysis_cursor = analysis_conn.cursor()

        # Query to count records per day within the scraped date range.
        # This assumes another script populates event_time from the raw 'time' field.
        query_end_date = end_date + timedelta(days=1)
        date_count_query = """
            SELECT CAST(event_time AS DATE) as EventDate, COUNT(*) as RecordCount
            FROM dbo.DailyBulletinArrests
            WHERE event_time >= ? AND event_time < ?
            GROUP BY CAST(event_time AS DATE)
            ORDER BY RecordCount DESC;
        """
        analysis_cursor.execute(date_count_query, start_date, query_end_date)
        date_counts = analysis_cursor.fetchall()
        analysis_conn.close()

        print("\n" + "="*30)
        print("  TOP 7 DAYS BY RECORD COUNT")
        print("="*30)
        for i, row in enumerate(date_counts[:7]):
            print(f"  {i+1}. {row.EventDate}: {row.RecordCount} records")
        print("="*30 + "\n")

    except Exception as e:
        status("Analysis", f"[WARN] Could not generate date count summary: {e}")
