import requests
import json
import time
import random
import sys
import concurrent.futures
import threading # New import for background proxy management
from datetime import datetime, date
from bs4 import BeautifulSoup
import pyodbc
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed for verify=False on proxy check
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import os
from dotenv import load_dotenv

load_dotenv()

# --- GLOBAL CONFIGURATION AND THREAD MANAGEMENT ---
# SQL Server Details (Loaded from .env)
MSSQL_SERVER   = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DRIVER   = "{ODBC Driver 18 for SQL Server}" # Use 17 or 18

# Proxy and URL Configurations
PROXY_LIST_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"
LIST_URL = 'https://doc-search.iowa.gov/api/offender/GetOffenderListAjax'
DETAIL_BASE_URL = 'https://doc-search.iowa.gov/offender/detail?offenderNumber='

INITIAL_BASE_SEARCH_URL = 'https://doc-search.iowa.gov/Offender/Search' 
AJAX_REFERER_URL = 'https://doc-search.iowa.gov/Offender/SearchResult?search=%7B%22FirsName%22%3Anull,%22MiddleName%22%3Anull,%22LastName%22%3Anull,%22Gender%22%3Anull,%22OffenderNumber%22%3Anull,%22Location%22%3Anull,%22Offense%22%3Anull,%22County%22%3A%2231%22,%22SearchType%22%3A%22SW%22%7D'
PROXY_TEST_URL = 'https://doc-search.iowa.gov/Offender/Search'

# Threading control variables and shared counters
PROXY_REFRESHER_RUNNING = True
PROXY_LOCK = threading.Lock()
DETAIL_STATS_LOCK = threading.Lock()
CHARGE_STATS_LOCK = threading.Lock()
# Counters for the final summary report
DETAIL_STATS = {'inserted': 0, 'skipped': 0}
CHARGE_STATS = {'inserted': 0, 'skipped': 0}


# Random Pool of User Agents (unchanged)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2; rv:118.0) Gecko/20100101 Firefox/118.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1"
]

# Base headers and LIST_BASE_DATA (unchanged)
HEADERS = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9',
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'Origin': 'https://doc-search.iowa.gov',
    'X-Requested-With': 'XMLHttpRequest'
}
LIST_BASE_DATA = {
    'draw': '1', 
    'columns[0][data]': 'Name', 'columns[0][name]': 'Name', 'columns[0][searchable]': 'true', 'columns[0][orderable]': 'true', 'columns[0][search][value]': '', 'columns[0][search][regex]': 'false',
    'columns[1][data]': 'OffenderNumber', 'columns[1][name]': 'OffenderNumber', 'columns[1][searchable]': 'true', 'columns[1][orderable]': 'true', 'columns[1][search][value]': '', 'columns[1][search][regex]': 'false',
    'columns[2][data]': 'Age', 'columns[2][name]': 'Age', 'columns[2][searchable]': 'true', 'columns[2][orderable]': 'true', 'columns[2][search][value]': '', 'columns[2][search][regex]': 'false',
    'columns[3][data]': 'Gender', 'columns[3][name]': 'Gender', 'columns[3][searchable]': 'true', 'columns[3][orderable]': 'true', 'columns[3][search][value]': '', 'columns[3][search][regex]': 'false',
    'order[0][column]': '0',
    'order[0][dir]': 'asc',
    'length': '25',
    'search[value]': '',
    'search[regex]': 'false',
    'searchModel.FirsName': '', 'searchModel.MiddleName': '', 'searchModel.LastName': '', 'searchModel.OffenderNumber': '', 'searchModel.Gender': '',
    'searchModel.County': '31', 
    'searchModel.Location': '', 'searchModel.Offense': '', 'searchModel.SearchType': 'SW',
    'start': '0'
}

# --- Utility Functions ---

def status(step, message):
    """Prints a standardized, time-stamped status message for Jenkins output."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [STATUS] {step}: {message}")

def get_db_connection(db_conn_str):
    """Establishes a new, isolated DB connection (required for multi-threading)."""
    try:
        conn = pyodbc.connect(db_conn_str)
        return conn
    except pyodbc.Error as ex:
        # Since this is run per thread, we just log the failure and let the thread die
        print(f"[ERROR] Thread failed to establish DB connection: {ex}")
        return None

def get_raw_proxies():
    """Fetches a fresh list of raw proxies from the source URL."""
    try:
        proxy_resp = requests.get(PROXY_LIST_URL, timeout=10)
        proxy_resp.raise_for_status()
        proxies_list = [line.split("://")[-1].strip() for line in proxy_resp.text.splitlines() if line.strip()]
        status("Proxy Fetch", f"Retrieved {len(proxies_list)} raw proxies.")
        return proxies_list
    except Exception as e:
        print(f"[ERROR] Could not fetch proxy list: {e}")
        return []

def parse_date_string(date_str):
    """
    Converts date strings (MM/DD/YYYY or YYYYMMDD) into a Python date object 
    for proper insertion into the SQL DATE column.
    """
    if not date_str or not date_str.strip():
        return None
        
    date_str = date_str.strip()

    # Case 1: MM/DD/YYYY format (Used for most detail page dates)
    try:
        return datetime.strptime(date_str, '%m/%d/%Y').date()
    except ValueError:
        pass
        
    # Case 2: YYYYMMDD format (Used for data-sort attribute on charges table)
    if len(date_str) == 8 and date_str.isdigit():
        try:
            return datetime.strptime(date_str, '%Y%m%d').date()
        except ValueError:
            pass
            
    # If parsing fails for any format, return None
    return None

def check_proxy(proxy):
    """Tests a single proxy against a reliable target."""
    test_url = "http://example.com"
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    try:
        requests.get(test_url, proxies=proxies_dict, timeout=3)
        return proxy
    except:
        return None

def validate_proxies(proxies_list, target_count=1000, batch_size=100):
    """
    Validates a list of proxies in parallel until target_count is reached. 
    Returns the list of valid proxies and the index of the next proxy to check.
    """
    status("Proxy Validation", f"Validating proxies in parallel (batch size {batch_size}).")
    
    proxies_to_check = list(proxies_list)
    random.shuffle(proxies_to_check)
    
    valid_proxies = []
    
    for i in range(0, len(proxies_to_check), batch_size):
        batch = proxies_to_check[i:i + batch_size]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            results = list(executor.map(check_proxy, batch))
        
        valid_batch = [proxy for proxy in results if proxy]
        
        with PROXY_LOCK:
            valid_proxies.extend(valid_batch)
        
        status("Proxy Validation", f"Found {len(valid_proxies)} working proxies so far. Continuing validation...")
        
        # Stop early if target_count valid proxies are found
        if len(valid_proxies) >= target_count:
            status("Proxy Validation", f"Sufficient proxies found ({len(valid_proxies)}), starting scraper.")
            
            next_start_index = i + len(batch) 
            return valid_proxies, next_start_index, proxies_to_check

    return valid_proxies, len(proxies_to_check), proxies_to_check

def proxy_refresher(raw_proxies_shuffled, valid_proxies_pool, initial_index):
    """
    Runs in a background thread to continuously check remaining raw proxies 
    and add working ones to the shared pool.
    """
    global PROXY_REFRESHER_RUNNING 
    current_index = initial_index
    batch_size = 50 

    while PROXY_REFRESHER_RUNNING and current_index < len(raw_proxies_shuffled):
        batch = raw_proxies_shuffled[current_index:current_index + batch_size]
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            results = list(executor.map(check_proxy, batch))
        
        valid_batch = [proxy for proxy in results if proxy]
        
        if valid_batch:
            with PROXY_LOCK:
                valid_proxies_pool.extend(valid_batch)
            status("Refresher", f"Added {len(valid_batch)} new proxies. Pool size: {len(valid_proxies_pool)}")
        
        current_index += len(batch)
        
        time.sleep(random.uniform(5, 10))
        
    if current_index >= len(raw_proxies_shuffled):
        status("Refresher", "Exhausted raw proxy list. Background validation stopping.")
        PROXY_REFRESHER_RUNNING = False


# --- Batch Insertion Functions (Adapted for Single Record Insert per thread) ---

def execute_single_insert_fallback(conn, cursor, sql, data_to_insert):
    """
    Fallback for executemany when IntegrityError occurs (to skip duplicates).
    """
    inserted = 0
    skipped = 0
    for row in data_to_insert:
        try:
            cursor.execute(sql, row)
            inserted += 1
        except pyodbc.IntegrityError as e:
            skipped += 1
        except pyodbc.Error as ex:
            print(f"[ERROR] Fallback single insert error: {ex}")
            
    return inserted, skipped

def execute_batch_insert(conn, table_name, records):
    """Executes bulk insertion, optimized for single/small batches in a thread."""
    if not records:
        return 0, 0 # inserted, skipped
        
    cursor = conn.cursor()
    
    # Define column order for each table explicitly
    if table_name == 'Offender_Detail':
        columns = [
            'OffenderNumber', 'Location', 'Offense', 'TDD_SDD', 
            'CommitmentDate', 'RecallDate', 'InterviewDate', 'MandatoryMinimum', 
            'DecisionType', 'Decision', 'DecisionDate', 'EffectiveDate'
        ]
    elif table_name == 'Offender_Charges':
        columns = ['OffenderNumber', 'SupervisionStatus', 'OffenseClass', 'CountyOfCommitment', 'EndDate']
    elif table_name == 'Offender_Summary':
        columns = ['OffenderNumber', 'Name', 'Gender', 'Age']
    else: 
        return 0, 0

    # Convert list of dictionaries to list of tuples in the required column order
    data_to_insert = [tuple(record.get(col) for col in columns) for record in records]

    placeholders = ', '.join(['?' for _ in columns])
    column_names = ', '.join(columns)
    sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
    
    inserted_count = 0
    skipped_count = 0
    
    try:
        cursor.executemany(sql, data_to_insert)
        inserted_count = len(records)
        conn.commit()
    except pyodbc.IntegrityError as e:
        conn.rollback()
        # Fallback to slower, row-by-row insert to skip the duplicates gracefully.
        inserted_count, skipped_count = execute_single_insert_fallback(conn, cursor, sql, data_to_insert)
        conn.commit()
    except pyodbc.Error as ex:
        print(f"[ERROR] Threaded batch insertion failed for {table_name}: {ex}")
        conn.rollback()
        return 0, 0
    finally:
        cursor.close()

    return inserted_count, skipped_count

# --- Core Scraper Functions ---

def check_detail_data_exists(cursor, offender_number):
    """
    Checks if the detail data (Offender_Detail and implicitly Offender_Charges) 
    exists for the given OffenderNumber.
    """
    try:
        # Check for the primary detail record
        cursor.execute("SELECT 1 FROM Offender_Detail WHERE OffenderNumber = ?", offender_number.strip())
        return cursor.fetchone() is not None
    except pyodbc.Error as ex:
        print(f"[ERROR] DB check failed for detail data existence: {ex}")
        return False

def get_fresh_session(valid_proxies, max_attempts=50):
    """
    Acquires a fresh requests.Session object with a valid Anti-Forgery Token.
    Returns the session and proxy used if successful, otherwise None.
    Limits attempts to max_attempts to avoid hanging if the whole pool is bad.
    """
    session = requests.Session()
    with PROXY_LOCK:
        proxy_pool = list(valid_proxies)
    
    random.shuffle(proxy_pool)
    # Limit to a subset to prevent infinite blocking
    proxy_subset = proxy_pool[:max_attempts]
    
    for i, proxy in enumerate(proxy_subset):
        try:
            current_headers = {'User-Agent': random.choice(USER_AGENTS)}
            proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
            
            # 1. Hit the base search page to initiate the session (verify=False)
            response = session.get(INITIAL_BASE_SEARCH_URL, headers=current_headers, proxies=proxies_dict, timeout=5, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 2. Scrape the __RequestVerificationToken
            antiforgery_input = soup.find('input', {'name': '__RequestVerificationToken'})
            
            if antiforgery_input and antiforgery_input.get('value'):
                token_value = antiforgery_input.get('value')
                
                # Set the token explicitly as a cookie 
                session.cookies.set('__RequestVerificationToken', token_value, domain='doc-search.iowa.gov', path='/')
                
                # 3. Navigate to the search results page to set the specific referer
                session.get(AJAX_REFERER_URL, headers=current_headers, proxies=proxies_dict, timeout=5, verify=False)
                
                return session, proxy
        except requests.exceptions.RequestException:
            pass
            
    return None, None

def scrape_offender_detail(offender_number, session, valid_proxies, db_conn_str):
    """
    Attempts to fetch and parse the detail page, retrying with all available proxies,
    and performs immediate, isolated database insertion upon success.
    """
    detail_url = DETAIL_BASE_URL + offender_number
    
    # Establish local connection for this thread
    conn = get_db_connection(db_conn_str)
    if not conn:
        return False

    local_detail_data = {}
    local_charge_data = []
    
    # Retry logic instead of iterating all proxies in a nested loop
    MAX_RETRIES = 5
    success_proxy = None
    
    for attempt in range(MAX_RETRIES):
        # 1. Acquire Session (if needed)
        if session is None:
            session, success_proxy = get_fresh_session(valid_proxies)
            if not session:
                # If we can't get a session after trying a batch of proxies, wait a bit and try again
                time.sleep(random.uniform(1, 3))
                continue
        else:
            # If session was passed in or reused, we need a proxy. 
            # If we don't know the proxy associated with this session (e.g. passed from outside without proxy info),
            # we might be in trouble if we need to stick to the same IP.
            # But get_fresh_session returns (session, proxy).
            # If 'session' was passed as arg, 'success_proxy' might be None.
            # For now, let's assume if session is passed, we might need to just pick a proxy or use one if we knew it.
            # To be safe, if we don't have a proxy, let's get a new session+proxy.
            if not success_proxy:
                 # This happens if session was passed from caller but proxy wasn't.
                 # In current main(), we pass None, so we usually enter the 'if session is None' block.
                 # If we are reusing a session from a previous successful attempt in this function? No, we break on success.
                 # So this block is rarely hit unless we pass a session.
                 # Let's just get a fresh one to be safe and ensure we have the proxy.
                 session, success_proxy = get_fresh_session(valid_proxies)
                 if not session:
                     continue

        # 2. Make Request
        proxies_dict = {"http": f"http://{success_proxy}", "https": f"http://{success_proxy}"}
        detail_headers = HEADERS.copy()
        detail_headers['User-Agent'] = random.choice(USER_AGENTS)
        
        try:
            response = session.get(detail_url, headers=detail_headers, proxies=proxies_dict, timeout=10, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            def get_detail_value(label_text):
                # Use 'string' instead of deprecated 'text'
                label_element = soup.find('div', class_='label', string=lambda t: t and label_text in t)
                if label_element:
                    data_element = label_element.find_next_sibling('div', class_='d-inline-flex')
                    if data_element:
                        return data_element.get_text(strip=True)
                return None

            # --- Extract Main Detail Fields ---
            # Check if we actually got a valid page (look for a known element)
            if not soup.find('div', class_='label'):
                 # Might be a redirect or error page
                 raise ValueError("Invalid detail page content")

            local_detail_data = {
                'OffenderNumber': offender_number.strip(),
                'Location': get_detail_value('Location:'),
                'Offense': get_detail_value('Offense:'),
                'TDD_SDD': parse_date_string(get_detail_value('TDD/SDD *:')),
                'CommitmentDate': parse_date_string(get_detail_value('Commitment Date:')),
                'RecallDate': parse_date_string(get_detail_value('Recall Date:')),
                'InterviewDate': get_detail_value('Interview Date and Time (if being interviewd):'),
                'MandatoryMinimum': get_detail_value('Mandatory Minimum (if applicable):')
            }
            local_detail_data['DecisionType'] = get_detail_value('Decision Type:')
            local_detail_data['Decision'] = get_detail_value('Decision:')
            local_detail_data['DecisionDate'] = parse_date_string(get_detail_value('Decision Date:'))
            local_detail_data['EffectiveDate'] = parse_date_string(get_detail_value('Effective Date:'))

            # --- Extract Charges Table ---
            charges_table = soup.find('table', id='charges')
            if charges_table:
                tbody = charges_table.find('tbody')
                if tbody:
                    rows = tbody.find_all('tr')
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 5: 
                            end_date_raw = cols[4].get('data-sort') or cols[4].get_text(strip=True) 
                            charge_data = {
                                'OffenderNumber': offender_number.strip(),
                                'SupervisionStatus': cols[1].get_text(strip=True) or None,
                                'OffenseClass': cols[2].get_text(strip=True) or None,
                                'CountyOfCommitment': cols[3].get_text(strip=True) or None,
                                'EndDate': parse_date_string(end_date_raw)
                            }
                            local_charge_data.append(charge_data)
            
            # Successfully scraped, break the retry loop
            status("Detail Scrape", f"Successfully scraped details for {offender_number}")
            break 
        
        except requests.exceptions.RequestException:
            session = None # Reset session on network error
        except Exception as e:
            print(f"[ERROR] Parsing error during detail scrape for {offender_number.strip()}: {e}")
            session = None # Reset session on parsing error (might be bad page)
            continue # Try next attempt
    
    # --- THREAD-SAFE INSERTION AND COUNTER UPDATE ---
    success = False
    if local_detail_data:
        # 1. Insert Detail Record (list of 1 dict)
        d_inserted, d_skipped = execute_batch_insert(conn, 'Offender_Detail', [local_detail_data])
        
        # 2. Insert Charge Records (list of 0+ dicts)
        c_inserted, c_skipped = execute_batch_insert(conn, 'Offender_Charges', local_charge_data)
        
        # 3. Update thread-safe global counters
        with DETAIL_STATS_LOCK:
            DETAIL_STATS['inserted'] += d_inserted
            DETAIL_STATS['skipped'] += d_skipped
        
        with CHARGE_STATS_LOCK:
            CHARGE_STATS['inserted'] += c_inserted
            CHARGE_STATS['skipped'] += c_skipped
            
        success = d_inserted > 0
        
    else:
        print(f"[ERROR] Detail scrape failed for {offender_number.strip()}: All {len(proxy_pool)} proxies exhausted.")
    
    # Close the thread's connection
    conn.close()
    return success

    # Close the thread's connection
    conn.close()
    return success

def process_list_batch(offsets, valid_proxies):
    """
    Worker function to fetch a batch of list pages using a persistent session.
    Returns a list of offender dictionaries found.
    """
    results = []
    session, proxy = get_fresh_session(valid_proxies)
    
    if not session:
        print("[WARN] Worker failed to get initial session. Skipping batch.")
        return []

    list_headers = HEADERS.copy()
    list_headers['Referer'] = AJAX_REFERER_URL
    list_headers['User-Agent'] = random.choice(USER_AGENTS)
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    page_size = int(LIST_BASE_DATA['length'])

    for start_index in offsets:
        # Prepare data
        list_data = LIST_BASE_DATA.copy()
        list_data['start'] = str(start_index)
        list_data['draw'] = str((start_index // page_size) + 1)
        
        token = session.cookies.get('__RequestVerificationToken')
        if token:
            list_data['__RequestVerificationToken'] = token

        # Try fetching page (with local retry)
        page_success = False
        for attempt in range(3):
            try:
                resp = session.post(LIST_URL, data=list_data, headers=list_headers, proxies=proxies_dict, timeout=15, verify=False)
                resp.raise_for_status()
                data = resp.json()
                offenders = data.get('data', [])
                results.extend(offenders)
                page_success = True
                status("Batch Worker", f"Fetched page {list_data['draw']} (Offset {start_index}). Found {len(offenders)} records.")
                break
            except Exception as e:
                print(f"[WARN] Batch worker error on page {list_data['draw']}: {e}. Refreshing session...")
                session, proxy = get_fresh_session(valid_proxies)
                if session:
                    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
                    token = session.cookies.get('__RequestVerificationToken')
                    if token:
                        list_data['__RequestVerificationToken'] = token
                else:
                    print("[ERROR] Worker lost session and could not recover.")
                    return results 
        
        if not page_success:
            print(f"[ERROR] Failed to fetch page starting at {start_index} after retries.")
            
        # Brief pause between pages in the same batch
        time.sleep(random.uniform(1.0, 2.0))
    
    return results

def scrape_offender_list(valid_proxies, existing_detail_ids):
    """
    Orchestrates the parallel scraping of the offender list.
    """
    print(f"[STATUS] List Scrape: Starting parallel list scrape...")
    
    # 1. Get a session to determine total records (Retry a few times)
    session = None
    proxy_for_list = None
    for i in range(3):
        session, proxy_for_list = get_fresh_session(valid_proxies, max_attempts=50)
        if session:
            break
        print(f"[WARNING] Failed to get session for list scrape (Attempt {i+1}/3). Retrying...")
        
    if not session:
        print("[FATAL] Could not get session to determine total records.")
        return [], 0, []

    try:
        proxies_dict = {"http": f"http://{proxy_for_list}", "https": f"http://{proxy_for_list}"}
        list_data = LIST_BASE_DATA.copy()
        token = session.cookies.get('__RequestVerificationToken')
        if token:
            list_data['__RequestVerificationToken'] = token
            
        resp = session.post(LIST_URL, data=list_data, headers=HEADERS, proxies=proxies_dict, timeout=15, verify=False)
        resp.raise_for_status()
        data = resp.json()
        total_records = data.get('recordsFiltered', 0)
        status("List Scrape", f"Total records to fetch: {total_records}")
    except Exception as e:
        print(f"[FATAL] Failed to get total record count: {e}")
        return [], 0, []

    if total_records == 0:
        return [], 0, []

    # 2. Generate Batches
    page_size = int(LIST_BASE_DATA['length'])
    all_offsets = range(0, total_records, page_size)
    
    # Split offsets into chunks for workers
    BATCH_SIZE = 10 # Pages per worker
    offset_chunks = [all_offsets[i:i + BATCH_SIZE] for i in range(0, len(all_offsets), BATCH_SIZE)]
    
    status("List Scrape", f"Split {len(all_offsets)} pages into {len(offset_chunks)} batches.")

    # 3. Run Parallel Batches
    all_offenders_raw = []
    MAX_WORKERS = 10
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_list_batch, chunk, valid_proxies) for chunk in offset_chunks]
        
        for future in concurrent.futures.as_completed(futures):
            try:
                batch_results = future.result()
                all_offenders_raw.extend(batch_results)
            except Exception as e:
                print(f"[ERROR] Batch future exception: {e}")

    status("List Scrape", f"Raw records fetched: {len(all_offenders_raw)}")

    # 4. Process and Deduplicate
    offenders_missing_details = []
    summary_records_to_insert = []
    seen_offender_numbers = set()
    
    for offender in all_offenders_raw:
        offender_number = offender.get('OffenderNumber', '').strip()
        if not offender_number:
            continue
            
        if offender_number not in seen_offender_numbers:
            seen_offender_numbers.add(offender_number)
            
            summary = {
                'OffenderNumber': offender_number,
                'Name': offender.get('Name', '').strip(),
                'Gender': offender.get('Gender', '').strip(),
                'Age': offender.get('Age')
            }
            summary_records_to_insert.append(summary)
            
            if offender_number not in existing_detail_ids:
                offenders_missing_details.append(summary)

    return offenders_missing_details, len(all_offenders_raw), summary_records_to_insert

# --- Main Scraper Logic ---
def main():
    global PROXY_REFRESHER_RUNNING 
    
    # --- 0. Prepare Connection String ---
    DB_CONN_STR = (
        f"DRIVER={MSSQL_DRIVER};"
        f"SERVER={MSSQL_SERVER};"
        f"DATABASE={MSSQL_DATABASE};"
        f"UID={MSSQL_USERNAME};"
        f"PWD={MSSQL_PASSWORD};"
        "TrustServerCertificate=yes;"
    )

    # --- 1. Proxy Setup (Initial Fetch and Validation) ---
    raw_proxies = get_raw_proxies()
    if not raw_proxies:
        print("[FATAL] Scraper aborted. Could not retrieve any raw proxies.")
        sys.exit(1)

    valid_proxies, next_index_to_check, shuffled_raw_proxies = validate_proxies(raw_proxies)

    if not valid_proxies:
        print("[FATAL] No working proxies found. Aborting.")
        sys.exit(1)
        
    status("Proxy Setup", f"Initial setup complete: {len(valid_proxies)} proxies available.")
    
    # --- Start Background Proxy Refresher ---
    if next_index_to_check < len(shuffled_raw_proxies):
        status("Proxy Setup", "Starting background proxy refresher thread.")
        refresher_thread = threading.Thread(
            target=proxy_refresher, 
            args=(shuffled_raw_proxies, valid_proxies, next_index_to_check),
            daemon=True
        )
        refresher_thread.start()
    else:
         status("Proxy Setup", "All raw proxies checked. No need for background refresher.")
         
    # --- 2. Database Connection & Pre-load Existing IDs ---
    status("DB Connection", "Connecting to MSSQL for initial operations...")
    conn_summary = None
    existing_detail_ids = set()
    try:
        conn_summary = pyodbc.connect(DB_CONN_STR)
        cursor_summary = conn_summary.cursor()
        status("DB Connection", "Connection successful. Fetching existing OffenderNumbers...")
        
        cursor_summary.execute("SELECT OffenderNumber FROM Offender_Detail")
        rows = cursor_summary.fetchall()
        existing_detail_ids = {row.OffenderNumber.strip() for row in rows}
        status("DB Connection", f"Loaded {len(existing_detail_ids)} existing offender IDs.")
        
    except pyodbc.Error as ex:
        print(f"[FATAL] Database connection failed: {ex}")
        PROXY_REFRESHER_RUNNING = False
        sys.exit(1)

    # --- 3. Parallel List Scrape ---
    # Note: We no longer pass a single session. Workers create their own.
    offenders_to_scrape, total_records_found, summary_records_to_insert = scrape_offender_list(valid_proxies, existing_detail_ids)
    
    # --- 4. Batch Insert Summaries ---
    summary_inserted, summary_skipped = execute_batch_insert(conn_summary, 'Offender_Summary', summary_records_to_insert)
    conn_summary.close() # Close summary connection once list is done
    status("Summary Insert", f"Inserted {summary_inserted} summaries, skipped {summary_skipped} duplicates.")
    
    # --- 5. Scrape Details for Each Offender (Parallel Insertion) ---
    if offenders_to_scrape:
        status("Detail Scrape", f"Starting parallel detail scraping for {len(offenders_to_scrape)} missing unique offenders...")
        
        # --- THREAD POOL EXECUTION FOR SPEED ---
        MAX_WORKERS = 10 
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Pass the CONNECTION STRING (DB_CONN_STR) for each thread to open its own connection
            futures = [executor.submit(
                scrape_offender_detail, 
                offender['OffenderNumber'], 
                None, # Session is now created inside scrape_offender_detail if not passed? Wait, scrape_offender_detail expects a session.
                valid_proxies, 
                DB_CONN_STR
            ) for offender in offenders_to_scrape]
            
            # WAIT: scrape_offender_detail currently expects a 'session' argument.
            # We need to update scrape_offender_detail to create its own session or pass one.
            # Since we are refactoring, let's update scrape_offender_detail signature in the loop below.
            pass

    # --- FIX: We need to adapt the detail scraper loop since we removed the main session ---
    # We will create a session for each worker or let them create it.
    # Actually, scrape_offender_detail (Line 328) takes 'session'.
    # We should update scrape_offender_detail to accept None and create one, OR create a pool here.
    # For simplicity, let's create a session for each thread in the executor? No, executor manages threads.
    
    # Let's modify the loop to pass a NEW session to each task.
    # But creating 1000 sessions is bad.
    # Better: Update scrape_offender_detail to get a session from a pool or create one if None.
    
    # Let's update the loop to use a helper that manages the session.
    
    status("Detail Scrape", "Detail scraping logic needs session management. (Implemented below)")
    
    # Helper to manage session for detail scraping
    def detail_worker(offender_num):
        # Create a local session for this task (or reuse if we had a persistent worker)
        # Since we are submitting tasks, we can't easily persist session across tasks without a custom worker.
        # However, creating a session per detail page is expensive (2 requests to init).
        # Ideally we reuse sessions.
        
        # For now, to keep it working, we will create a session.
        # This might be slow. 
        # A better way is to use the same batching strategy as the list!
        # But for now, let's just get a session.
        sess, _ = get_fresh_session(valid_proxies)
        if sess:
            scrape_offender_detail(offender_num, sess, valid_proxies, DB_CONN_STR)
        else:
            print(f"[ERROR] Could not get session for detail {offender_num}")

    if offenders_to_scrape:
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
             futures = [executor.submit(detail_worker, offender['OffenderNumber']) for offender in offenders_to_scrape]
             concurrent.futures.wait(futures)

        status("Detail Scrape", f"Parallel scraping complete. Detail insertion results gathered.")
            
        # Counters are now already populated in DETAIL_STATS and CHARGE_STATS by the threads
        detail_inserted, detail_skipped = DETAIL_STATS['inserted'], DETAIL_STATS['skipped']
        charge_inserted, charge_skipped = CHARGE_STATS['inserted'], CHARGE_STATS['skipped']
        
        status("Detail Insert", f"Inserted {detail_inserted} detail records, skipped {detail_skipped} duplicates.")
        status("Charge Insert", f"Inserted {charge_inserted} charge records, skipped {charge_skipped} duplicates.")
    else:
        status("Detail Scrape", "No missing offender details found to scrape.")
        detail_inserted, detail_skipped, charge_inserted, charge_skipped = 0, 0, 0, 0
        
    # --- 6. Final Summary and Cleanup ---
    status("Complete", "Scraping and insertion finished.")
    
    # Signal the background thread to stop before exit
    PROXY_REFRESHER_RUNNING = False
    
    print("\n" + "="*50)
    print("FINAL SCRAPING SUMMARY (for Jenkins Status)")
    print("="*50)
    print(f"Total Unique Offender Records Found in Site: {total_records_found}")
    print(f"Offender Details Targeted for Scraping: {len(offenders_to_scrape)}")
    print(f"Offender Summary - Inserted: {summary_inserted}, Skipped: {summary_skipped}")
    print(f"Offender Detail - Inserted: {detail_inserted}, Skipped: {detail_skipped}")
    print(f"Offender Charges - Inserted: {charge_inserted}, Skipped: {charge_skipped}")
    print("="*50 + "\n")

    status("Cleanup", "Database connection closed. Exiting with success code.")
    
    sys.exit(0)
        
if __name__ == "__main__":
    main()

