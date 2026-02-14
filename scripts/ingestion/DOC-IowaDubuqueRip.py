import requests
import json
import time
import random
import sys
import concurrent.futures
import threading 
from datetime import datetime
from bs4 import BeautifulSoup
import pyodbc
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single warning from urllib3 needed for verify=False on proxy check
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import shared_utils
import shared_utils
from shared_utils import status, parse_date, APIClient

# --- GLOBAL CONFIGURATION AND THREAD MANAGEMENT ---
LIST_URL = 'https://doc-search.iowa.gov/api/offender/GetOffenderListAjax'
DETAIL_BASE_URL = 'https://doc-search.iowa.gov/offender/detail?offenderNumber='

INITIAL_BASE_SEARCH_URL = 'https://doc-search.iowa.gov/Offender/Search' 
AJAX_REFERER_URL = 'https://doc-search.iowa.gov/Offender/SearchResult?search=%7B%22FirsName%22%3Anull,%22MiddleName%22%3Anull,%22LastName%22%3Anull,%22Gender%22%3Anull,%22OffenderNumber%22%3Anull,%22Location%22%3Anull,%22Offense%22%3Anull,%22County%22%3A%2231%22,%22SearchType%22%3A%22SW%22%7D'

# Threading control variables and shared counters
PROXY_REFRESHER_RUNNING = True
PROXY_LOCK = threading.Lock()
DETAIL_STATS_LOCK = threading.Lock()
CHARGE_STATS_LOCK = threading.Lock()
# Counters for the final summary report
DETAIL_STATS = {'inserted': 0, 'skipped': 0}
CHARGE_STATS = {'inserted': 0, 'skipped': 0}

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
    'columns[0][data]': 'Name', 'columns[0][name]': 'Name', 'columns[0][searchable]': 'true', 'columns[0][search][value]': '', 'columns[0][search][regex]': 'false',
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

import argparse

# --- ARGS ---
def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="{}", help="JSON config string")
    return parser.parse_args()

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
            results = list(executor.map(shared_utils.check_proxy, batch))
        
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

def execute_batch_insert_api(table_name, records):
    """Executes bulk insertion via API"""
    if not records:
        return 0, 0 
    
    api = APIClient()
    try:
        if table_name == 'Offender_Summary':
            api.post_ingestion("doc/batch-summary", records)
        # Note: Detail insertion is more complex as it involves children. 
        # The script calls this function for Detail AND Charges separately.
        # But our API endpoint `doc/batch-details` expects a structured object.
        # We need to adapt the caller `process_detail_batch` to construct the full DTO.
        return len(records), 0
    except Exception as e:
        import logging
        logging.error(f"API Error in execute_batch_insert_api: {e}")
        return 0, len(records)

# --- Core Scraper Functions ---


@shared_utils.get_retry_decorator(max_attempts=200, wait_seconds=0.1)
def get_authenticated_session(proxy_pool):
    """
    Acquires a fresh session and performs the CSRF token dance.
    Returns (session, proxy) or raises Exception (which retry decorator handles).
    
    This version cycles through ALL available proxies until one works.
    """
    if not proxy_pool:
        raise Exception("No proxies available")
    
    # Shuffle to randomize order
    import random
    shuffled_proxies = list(proxy_pool)
    random.shuffle(shuffled_proxies)
    
    # Try EVERY proxy in the pool
    for proxy in shuffled_proxies:
        try:
            session = requests.Session()
            session.headers.update({'User-Agent': random.choice(shared_utils.USER_AGENTS)})
            
            proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
            
            # 1. Hit the base search page
            response = session.get(INITIAL_BASE_SEARCH_URL, headers={'User-Agent': session.headers['User-Agent']}, 
                                 proxies=proxies_dict, timeout=10, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 2. Scrape the __RequestVerificationToken
            antiforgery_input = soup.find('input', {'name': '__RequestVerificationToken'})
            
            if antiforgery_input and antiforgery_input.get('value'):
                token_value = antiforgery_input.get('value')
                
                # Set the token explicitly as a cookie 
                session.cookies.set('__RequestVerificationToken', token_value, domain='doc-search.iowa.gov', path='/')
                
                # 3. Navigate to the search results page to set the specific referer
                session.get(AJAX_REFERER_URL, headers={'User-Agent': session.headers['User-Agent']}, 
                          proxies=proxies_dict, timeout=10, verify=False)
                
                return session, proxy
        except Exception:
            # Try next proxy
            continue
    
    # If we get here, all proxies failed
    raise Exception(f"All {len(shuffled_proxies)} proxies failed for authentication handshake")

def create_session_pool(proxy_pool, pool_size=30):
    """
    Pre-create authenticated sessions to avoid re-auth on every detail fetch.
    Returns: list of (session, proxy) tuples
    """
    status("Session Pool", f"Creating pool of {pool_size} authenticated sessions...")
    session_pool = []
    
    import random
    shuffled = list(proxy_pool)
    random.shuffle(shuffled)
    
    attempted = 0
    for proxy in shuffled:
        if len(session_pool) >= pool_size:
            break
        attempted += 1
        try:
            session = requests.Session()
            session.headers.update({'User-Agent': random.choice(shared_utils.USER_AGENTS)})
            
            proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
            
            response = session.get(INITIAL_BASE_SEARCH_URL, headers={'User-Agent': session.headers['User-Agent']}, 
                                 proxies=proxies_dict, timeout=10, verify=False)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            antiforgery_input = soup.find('input', {'name': '__RequestVerificationToken'})
            
            if antiforgery_input and antiforgery_input.get('value'):
                token_value = antiforgery_input.get('value')
                session.cookies.set('__RequestVerificationToken', token_value, domain='doc-search.iowa.gov', path='/')
                session.get(AJAX_REFERER_URL, headers={'User-Agent': session.headers['User-Agent']}, 
                          proxies=proxies_dict, timeout=10, verify=False)
                
                session_pool.append((session, proxy))
        except Exception:
            continue
    
    status("Session Pool", f"Created {len(session_pool)} working sessions from {attempted} attempts")
    return session_pool

def process_detail_batch(offender_batch, session_with_proxy):
    """
    Worker function to process offender details using a pre-authenticated session.
    session_with_proxy: tuple of (session, proxy_string)
    """
    if not offender_batch:
        return

    batch_dtos = []
    session, proxy = session_with_proxy
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    
    for offender in offender_batch:
        offender_number = offender['OffenderNumber']
        try:
            # Fetch detail with simple retry
            url = DETAIL_BASE_URL + offender_number
            headers = HEADERS.copy()
            headers['User-Agent'] = session.headers['User-Agent']
            
            html = None
            for attempt in range(3):
                try:
                    resp = session.get(url, headers=headers, proxies=proxies_dict, timeout=15, verify=False)
                    resp.raise_for_status()
                    html = resp.text
                    break
                except Exception:
                    if attempt == 2:
                        raise
                    time.sleep(0.1)
            
            soup = BeautifulSoup(html, 'html.parser')
            
            if not soup.find('div', class_='label'):
                raise ValueError("Invalid content")

            def get_detail_value(label_text):
                label_element = soup.find('div', class_='label', string=lambda t: t and label_text in t)
                if label_element:
                    data_element = label_element.find_next_sibling('div', class_='d-inline-flex')
                    if data_element:
                        return data_element.get_text(strip=True)
                return None

            def to_iso(dt):
                return dt.isoformat() if dt else None

            # Extract Detail
            local_detail_data = {
                'OffenderNumber': offender_number.strip(),
                'Location': get_detail_value('Location:'),
                'Offense': get_detail_value('Offense:'),
                'TDD_SDD': to_iso(parse_date(get_detail_value('TDD/SDD *:'))),
                'CommitmentDate': to_iso(parse_date(get_detail_value('Commitment Date:'))),
                'RecallDate': to_iso(parse_date(get_detail_value('Recall Date:'))),
                'InterviewDate': get_detail_value('Interview Date and Time (if being interviewd):'),
                'MandatoryMinimum': get_detail_value('Mandatory Minimum (if applicable):'),
                'DecisionType': get_detail_value('Decision Type:'),
                'Decision': get_detail_value('Decision:'),
                'DecisionDate': to_iso(parse_date(get_detail_value('Decision Date:'))),
                'EffectiveDate': to_iso(parse_date(get_detail_value('Effective Date:'))),
                'Charges': []
            }
            
            # Extract Charges
            charges_table = soup.find('table', id='charges')
            if charges_table:
                tbody = charges_table.find('tbody')
                if tbody:
                    for row in tbody.find_all('tr'):
                        cols = row.find_all('td')
                        if len(cols) >= 5:
                            end_date_raw = cols[4].get('data-sort') or cols[4].get_text(strip=True)
                            charge_data = {
                                'SupervisionStatus': (cols[1].get_text(strip=True) or None),
                                'OffenseClass': (cols[2].get_text(strip=True) or None),
                                'CountyOfCommitment': (cols[3].get_text(strip=True) or None),
                                'EndDate': to_iso(parse_date(end_date_raw))
                            }
                            local_detail_data['Charges'].append(charge_data)

            batch_dtos.append(local_detail_data)
            status("Detail Scrape", f"Processed {offender_number}")

        except Exception as e:
            import logging
            logging.error(f"Failed to scrape {offender_number}: {e}")

    # Send Batch to API
    if batch_dtos:
        try:
            api = APIClient()
            api.post_ingestion("doc/batch-details", batch_dtos)
            
            with DETAIL_STATS_LOCK:
                DETAIL_STATS['inserted'] += len(batch_dtos)
            with CHARGE_STATS_LOCK:
                c_count = sum(len(d['Charges']) for d in batch_dtos)
                CHARGE_STATS['inserted'] += c_count
                  
        except Exception as e:
            import logging
            logging.error(f"Batch API Insert Failed: {e}")

def process_list_batch(offsets, valid_proxies):
    """
    Worker function to fetch a batch of list pages using a persistent session.
    """
    results = []
    try:
        session, proxy = get_authenticated_session(valid_proxies)
    except Exception:
        session = None

    if not session:
        return []

    # get_authenticated_session sets cookies, but we need to pass headers.
    list_headers = HEADERS.copy()
    list_headers['Referer'] = AJAX_REFERER_URL
    list_headers['User-Agent'] = session.headers['User-Agent']
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    page_size = int(LIST_BASE_DATA['length'])

    for start_index in offsets:
        list_data = LIST_BASE_DATA.copy()
        list_data['start'] = str(start_index)
        list_data['draw'] = str((start_index // page_size) + 1)
        
        token = session.cookies.get('__RequestVerificationToken')
        if token:
            list_data['__RequestVerificationToken'] = token

        page_success = False
        while True:
            try:
                resp = session.post(LIST_URL, data=list_data, headers=list_headers, proxies=proxies_dict, timeout=15, verify=False)
                resp.raise_for_status()
                data = resp.json()
                offenders = data.get('data', [])
                results.extend(offenders)
                page_success = True
                status("Batch Worker", f"Fetched page {list_data['draw']}. Found {len(offenders)} records.")
                break
            except Exception as e:
                # Retry loop for scraping
                for attempt in range(3): 
                    if not session:
                        try:
                            session, proxy = get_authenticated_session(valid_proxies)
                        except Exception:
                            session = None
                            
                        if not session:
                            time.sleep(1)
                            continue
                    
                    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
                    list_headers['User-Agent'] = session.headers['User-Agent']
                    token = session.cookies.get('__RequestVerificationToken')
                    if token:
                        list_data['__RequestVerificationToken'] = token
                    
                    # If we got a session, try again
                    if session:
                        break # Break from attempt loop to retry the main request
                else: # This else belongs to the for loop, executed if no break occurred
                    # All attempts failed to get a session or update details
                    time.sleep(2) # Original sleep if no session could be acquired after retries
                    break # Break from while True, giving up on this page
        
        time.sleep(random.uniform(1.0, 2.0))
    
    return results

def scrape_offender_list(valid_proxies, existing_detail_ids):
    """
    Orchestrates the parallel scraping of the offender list.
    """
    status("List Scrape", "Starting parallel list scrape...")
    
    session = None
    proxy_for_list = None
    
    # Get authenticated session (retry decorator handles all attempts)
    try:
        session, proxy_for_list = get_authenticated_session(valid_proxies)
    except Exception as e:
        import logging
        logging.error(f"[FATAL] Could not get session after trying all proxies: {e}")
        sys.exit(1)

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
        # Log as ERROR
        import logging
        logging.error(f"[FATAL] Failed to get total record count: {e}")
        return [], 0, []

    if total_records == 0:
        return [], 0, []

    page_size = int(LIST_BASE_DATA['length'])
    all_offsets = range(0, total_records, page_size)
    
    BATCH_SIZE = 10 # Pages per worker
    offset_chunks = [all_offsets[i:i + BATCH_SIZE] for i in range(0, len(all_offsets), BATCH_SIZE)]
    
    status("List Scrape", f"Split {len(all_offsets)} pages into {len(offset_chunks)} batches.")

    all_offenders_raw = []
    MAX_WORKERS = 10
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_list_batch, chunk, valid_proxies) for chunk in offset_chunks]
        for future in concurrent.futures.as_completed(futures):
            try:
                batch_results = future.result()
                all_offenders_raw.extend(batch_results)
            except Exception as e:
                # Log as ERROR
                logging.error(f"[ERROR] Batch future exception: {e}")

    # Deduplicate and Filter
    offenders_missing_details = []
    summary_records_to_insert = []
    seen_offender_numbers = set()
    
    for offender in all_offenders_raw:
        offender_number = offender.get('OffenderNumber', '').strip()
        if not offender_number: continue
            
        if offender_number not in seen_offender_numbers:
            seen_offender_numbers.add(offender_number)
            
            summary = {
                'OffenderNumber': offender_number,
                'Name': offender.get('Name', '').strip(),
                'Gender': offender.get('Gender', '').strip(),
                'Age': str(offender.get('Age') or '')
            }
            summary_records_to_insert.append(summary)
            
            if offender_number not in existing_detail_ids:
                offenders_missing_details.append(summary)

    return offenders_missing_details, len(all_offenders_raw), summary_records_to_insert

# --- Main Scraper Logic ---
def main():
    parser = argparse.ArgumentParser(description="DOC Iowa Scraper")
    parser.add_argument("--LOG_LEVEL", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging verbosity level")
    parser.add_argument("--config", type=str, default="{}", help="JSON config string")
    args = parser.parse_args()

    # 0. Logging
    shared_utils.setup_logging(args.LOG_LEVEL)

    # 1. Proxy Setup
    config = shared_utils.get_config()
    if args.config:
        try:
             import json
             config.update(json.loads(args.config))
        except: pass
        
    GLOBAL_MAX_WORKERS = int(config.get("workers", 10))
    status("Main", f"Max Workers: {GLOBAL_MAX_WORKERS}")

    raw_proxies = shared_utils.get_proxies_from_source(config=config)
    if not raw_proxies:
        sys.exit(1)

    # Validate enough proxies to start
    valid_proxies = shared_utils.validate_proxies(raw_proxies, target_count=50) 
    if not valid_proxies:
        status("Main", "[FATAL] No working proxies found.")
        sys.exit(1)
        
    status("Proxy Setup", f"Initial setup complete: {len(valid_proxies)} proxies available.")
    
    # We removed the separate thread. If we needed to refresh, we'd do it in loops, 
    # but this script runs in phases. We can adding logic to reuse refresh_proxy_pool if needed.
    
    existing_detail_ids = set()
    status("Pre-Check", "Skipping existing ID check (assuming full scrape).")

    # 3. List Scrape
    offenders_to_scrape, total_records_found, summary_records_to_insert = scrape_offender_list(valid_proxies, existing_detail_ids)
    
    # 4. Summary Insert
    execute_batch_insert_api('Offender_Summary', summary_records_to_insert)
    summary_inserted = len(summary_records_to_insert)
    summary_skipped = 0
    status("Summary Insert", f"Inserted {summary_inserted} summaries.")
    
    # 5. Detail Scrape (Session Pooling)
    if offenders_to_scrape:
        status("Detail Scrape", f"Starting scrape for {len(offenders_to_scrape)} missing details...")
        
        # Create pool of authenticated sessions
        SESSION_POOL_SIZE = 30
        session_pool = create_session_pool(valid_proxies, SESSION_POOL_SIZE)
        
        if not session_pool:
            import logging
            logging.error("[FATAL] Could not create any working sessions for detail scraping")
            sys.exit(1)
        
        BATCH_SIZE = 50  # Records per worker
        offender_chunks = [offenders_to_scrape[i:i + BATCH_SIZE] for i in range(0, len(offenders_to_scrape), BATCH_SIZE)]
        
        # Use as many workers as we have sessions (up to chunk count)
        MAX_WORKERS = min(len(session_pool), len(offender_chunks))
        status("Detail Scrape", f"Using {MAX_WORKERS} workers with {len(session_pool)} pre-authenticated sessions")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Distribute sessions to workers via round-robin
            futures = []
            for i, chunk in enumerate(offender_chunks):
                session_idx = i % len(session_pool)
                futures.append(executor.submit(process_detail_batch, chunk, session_pool[session_idx]))
            concurrent.futures.wait(futures)


        detail_inserted, detail_skipped = DETAIL_STATS['inserted'], DETAIL_STATS['skipped']
        charge_inserted, charge_skipped = CHARGE_STATS['inserted'], CHARGE_STATS['skipped']
        
        status("Detail Insert", f"Inserted {detail_inserted} details, skipped {detail_skipped}.")
        status("Charge Insert", f"Inserted {charge_inserted} charges, skipped {charge_skipped}.")
    else:
        status("Detail Scrape", "No missing details to scrape.")
        detail_inserted, detail_skipped, charge_inserted, charge_skipped = 0, 0, 0, 0
        
    # 6. Final Summary
    print("\n" + "="*50)
    print("FINAL SCRAPING SUMMARY")
    print(f"Total Unique Offender Records Found: {total_records_found}")
    print(f"Offender Details Targeted: {len(offenders_to_scrape)}")
    print(f"Summary - Inserted: {summary_inserted}, Skipped: {summary_skipped}")
    print(f"Detail  - Inserted: {detail_inserted}, Skipped: {detail_skipped}")
    print(f"Charges - Inserted: {charge_inserted}, Skipped: {charge_skipped}")
    print("="*50 + "\n")

    # Failure Check
    failed = False
    if total_records_found > 0 and summary_inserted == 0:
        failed = True
    if len(offenders_to_scrape) > 0 and detail_inserted == 0:
        failed = True
        
    if failed:
        print("[ERROR] Job failed to insert records despite finding candidates.")
        sys.exit(1)

    sys.exit(0)
        
if __name__ == "__main__":
    main()
