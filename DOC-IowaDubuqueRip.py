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

# --- GLOBAL CONFIGURATION AND THREAD MANAGEMENT ---
# SQL Server Details (UPDATE THESE WITH YOUR ACTUAL VALUES)
MSSQL_SERVER   = "192.168.0.43" # e.g., "192.168.0.43"
MSSQL_DATABASE = "p2cdubuque" # Database name updated to p2cdubque
MSSQL_USERNAME = "sa" # e.g., "sa"
MSSQL_PASSWORD = "Thugitout09!" # e.g., "Thugitout09!"
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
        proxies_list = [p.split("://")[-1].strip().split(":")[0].strip() + ":" + p.split(":")[-1].strip() for p in proxy_resp.text.splitlines() if p.strip()]
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
    """Tests a single proxy against an HTTPS URL to validate secure tunneling capability."""
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    try:
        requests.get(PROXY_TEST_URL, proxies=proxies_dict, timeout=3, verify=False) 
        return proxy
    except requests.exceptions.RequestException:
        return None

def validate_proxies(proxies_list, target_count=100, batch_size=250):
    """
    Validates proxies in parallel until target_count is reached. 
    Returns the list of valid proxies and the index of the next proxy to check.
    """
    status("Proxy Validation", f"Validating proxies in parallel (batch size {batch_size}) against HTTPS endpoint.")
    
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
        
        # Stop early if 100 valid proxies are found
        if len(valid_proxies) >= target_count:
            status("Proxy Validation", f"Sufficient proxies found ({len(valid_proxies)}), starting scraper and refreshing in background.")
            
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
        except pyodbc.IntegrityError:
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
    else: # Offender_Summary is handled outside the thread pool
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
    except pyodbc.IntegrityError:
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

def get_fresh_session(valid_proxies):
    """
    Acquires fresh cookies and scrapes the Anti-Forgery Token from the initial base search page.
    Returns the session and proxy used if successful, otherwise None.
    """
    session = requests.Session()
    with PROXY_LOCK:
        proxy_pool = list(valid_proxies)
    
    random.shuffle(proxy_pool)
    
    for proxy in proxy_pool:
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

    with PROXY_LOCK:
        proxy_pool = list(valid_proxies)
    
    random.shuffle(proxy_pool)
    
    local_detail_data = None
    local_charge_data = []

    for proxy in proxy_pool:
        proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
        detail_headers = HEADERS.copy()
        detail_headers['User-Agent'] = random.choice(USER_AGENTS)
        
        try:
            response = session.get(detail_url, headers=detail_headers, proxies=proxies_dict, timeout=10, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            def get_detail_value(label_text):
                label_element = soup.find('div', class_='label', text=lambda t: t and label_text in t)
                if label_element:
                    data_element = label_element.find_next_sibling('div', class_='d-inline-flex')
                    if data_element:
                        return data_element.get_text(strip=True)
                return None

            # --- Extract Main Detail Fields ---
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
                rows = charges_table.find('tbody').find_all('tr')
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
            
            # Successfully scraped, break the proxy loop
            break 
        
        except requests.exceptions.RequestException:
            pass
        except Exception as e:
            print(f"[ERROR] Parsing error during detail scrape for {offender_number.strip()}: {e}")
            break
    
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

def scrape_offender_list(session, valid_proxies, cursor):
    """
    Handles the pagination loop to retrieve all offender summaries and identifies those missing details.
    """
    global PROXY_REFRESHER_RUNNING
    page_size = int(LIST_BASE_DATA['length'])
    start_index = 0
    total_records = float('inf')
    offenders_missing_details = []
    summary_records_to_insert = []
    scraped_offender_numbers = set()
    total_records_received = 0
    
    list_headers = HEADERS.copy()
    list_headers['Referer'] = AJAX_REFERER_URL 
    
    # Outer loop for comprehensive recovery (re-scraping the proxy list)
    for proxy_list_attempt in range(1, 3): 
        
        status("Proxy List Cycle", f"Starting list scrape using current proxy pool (Attempt {proxy_list_attempt})")
        
        while start_index < total_records:
            
            list_data = LIST_BASE_DATA.copy()
            list_data['start'] = str(start_index)
            list_data['draw'] = str((start_index // page_size) + 1)
            
            page_success = False
            
            with PROXY_LOCK:
                proxy_pool = list(valid_proxies)
            random.shuffle(proxy_pool)
            
            for proxy in proxy_pool:
                
                antiforgery_token_input = session.cookies.get('__RequestVerificationToken')
                if antiforgery_token_input:
                    list_data['__RequestVerificationToken'] = antiforgery_token_input
                
                proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
                list_headers['User-Agent'] = random.choice(USER_AGENTS)

                try:
                    response = session.post(LIST_URL, data=list_data, headers=list_headers, proxies=proxies_dict, timeout=10, verify=False)
                    response.raise_for_status()
                    
                    data = response.json()
                    total_records = data.get('recordsFiltered', 0)
                    offenders = data.get('data', [])
                    
                    total_records_received += len(offenders)
                    
                    if not offenders:
                        status("List Fetch", "No more offenders returned.")
                        total_records = start_index 
                        page_success = True
                        break
                        
                    for offender in offenders:
                        offender_number_stripped = offender['OffenderNumber'].strip()
                        
                        # --- Summary and Detail Preparation Logic ---
                        if offender_number_stripped not in scraped_offender_numbers:
                            
                            offender_summary = {
                                'OffenderNumber': offender_number_stripped,
                                'Name': offender['Name'].strip(),
                                'Gender': offender['Gender'].strip(),
                                'Age': offender['Age']
                            }
                            # 1. Collect all unique summaries for insertion
                            summary_records_to_insert.append(offender_summary)
                            
                            # 2. Check if Detail record ALREADY EXISTS in DB
                            if not check_detail_data_exists(cursor, offender_number_stripped):
                                # If details are MISSING, add to the target list
                                offenders_missing_details.append(offender_summary)
                                
                            scraped_offender_numbers.add(offender_number_stripped)
                    
                    status("Progress", f"Page {list_data['draw']} successful. Total unique found: {len(scraped_offender_numbers)}. Missing details to scrape: {len(offenders_missing_details)}")
                    page_success = True
                    break # Break inner proxy loop on success
                
                except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
                    if page_success == False:
                         print(f"[ERROR] List request failed on page {list_data['draw']} via proxy: {e.__class__.__name__}. Trying next proxy...")
                    
                    try:
                        new_session, new_proxy_used = get_fresh_session(valid_proxies) 
                        if new_session:
                             session = new_session 
                             status("Recovery", "Session successfully refreshed. Trying next proxy in pool.")
                    except Exception:
                        pass 

            # End of Inner (Proxy) Loop
            
            if not page_success:
                print(f"[ERROR] All {len(proxy_pool)} proxies failed for page {list_data['draw']}. Triggering full recovery.")
                start_index -= page_size
                break 

            start_index += page_size
            time.sleep(random.uniform(2.0, 4.0)) 

        # End of Middle (Pagination) Loop

        if start_index >= total_records:
            status("List Fetch", "Pagination complete.")
            break 
        
        # --- Full Recovery Logic (Phase 2: Refresh Proxy List) ---
        if proxy_list_attempt == 1:
            status("Full Recovery", "Exhausted current proxy pool. Fetching and validating a NEW proxy list.")
            new_raw_proxies = get_raw_proxies()
            if new_raw_proxies:
                new_valid_proxies, _, _ = validate_proxies(new_raw_proxies, target_count=50, batch_size=250)
                if new_valid_proxies:
                    with PROXY_LOCK:
                        valid_proxies.extend(new_valid_proxies)
                    status("Full Recovery", f"New proxy pool acquired ({len(new_valid_proxies)} proxies added). Restarting pagination from page {list_data['draw']}.")
                else:
                    status("FATAL Recovery", "Failed to acquire sufficient working proxies from new list.")
                    break
            else:
                status("FATAL Recovery", "Failed to fetch a new proxy list.")
                break
        else:
            status("FATAL Recovery", "Exhausted all recovery attempts (original and new proxy list).")
            break

    status("Summary Complete", f"Total unique offender numbers found: {len(scraped_offender_numbers)}. Details to scrape: {len(offenders_missing_details)}")
    return offenders_missing_details, total_records_received, summary_records_to_insert

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
         
    # --- 2. Database Connection for Summary Insert/Check ---
    status("DB Connection", "Connecting to MSSQL for initial summary operations...")
    conn_summary = None
    try:
        conn_summary = pyodbc.connect(DB_CONN_STR)
        cursor_summary = conn_summary.cursor()
        status("DB Connection", "Connection successful.")
    except pyodbc.Error as ex:
        print(f"[FATAL] Database connection failed: {ex}")
        PROXY_REFRESHER_RUNNING = False
        sys.exit(1)

    # --- 3. Session Setup & Cookie Acquisition ---
    session, session_proxy = get_fresh_session(valid_proxies)
    
    if not session_proxy:
        print("[FATAL] Scraper aborted due to failure in acquiring a valid session/token.")
        conn_summary.close()
        PROXY_REFRESHER_RUNNING = False
        sys.exit(1)

    # --- 4. Pagination Loop (Summary Collection) ---
    # offenders_to_scrape: list of summaries whose details are MISSING
    # summary_records_to_insert: list of all unique summaries found for batch insert
    offenders_to_scrape, total_records_found, summary_records_to_insert = scrape_offender_list(session, valid_proxies, cursor_summary)
    
    # --- 5. Batch Insert Summaries ---
    summary_inserted, summary_skipped = execute_batch_insert(conn_summary, 'Offender_Summary', summary_records_to_insert)
    conn_summary.close() # Close summary connection once list is done
    status("Summary Insert", f"Inserted {summary_inserted} summaries, skipped {summary_skipped} duplicates.")
    
    # --- 6. Scrape Details for Each Offender (Parallel Insertion) ---
    if offenders_to_scrape:
        status("Detail Scrape", f"Starting parallel detail scraping for {len(offenders_to_scrape)} missing unique offenders...")
        
        # --- THREAD POOL EXECUTION FOR SPEED ---
        MAX_WORKERS = 10 
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Pass the CONNECTION STRING (DB_CONN_STR) for each thread to open its own connection
            futures = [executor.submit(
                scrape_offender_detail, 
                offender['OffenderNumber'], 
                session, 
                valid_proxies, 
                DB_CONN_STR
            ) for offender in offenders_to_scrape]

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
        
    # --- 7. Final Summary and Cleanup ---
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

