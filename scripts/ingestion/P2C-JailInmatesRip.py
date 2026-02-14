import requests
import random
import time
import json
import pyodbc
import sys
import concurrent.futures
import threading
import argparse
import json
import os
from datetime import datetime
from bs4 import BeautifulSoup
from dotenv import load_dotenv

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import shared_utils
from shared_utils import status, parse_date, APIClient
import base64

load_dotenv()

# --- CONFIGURATION ---
BASE_URL = "http://p2c.cityofdubuque.org"
JAIL_PAGE_URL = f"{BASE_URL}/jailinmates.aspx"
DATA_URL = f"{BASE_URL}/jqHandler.ashx?op=s"
MUG_URL_TEMPLATE = f"{BASE_URL}/Mug.aspx?Type=4&ImageID={{}}&ss=1"

# --- GLOBAL STATS ---
total_processed = 0
total_inserted = 0
total_updated = 0
total_released = 0
total_errors = 0
stats_lock = threading.Lock()

# --- DETAIL FETCHING LOGIC ---
def get_detail_url(session, record_index, viewstate, viewstategen, eventvalidation):
    data = {
        '__VIEWSTATE': viewstate,
        '__VIEWSTATEGENERATOR': viewstategen,
        '__EVENTVALIDATION': eventvalidation,
        'ctl00$MasterPage$mainContent$CenterColumnContent$hfRecordIndex': str(record_index),
        'ctl00$MasterPage$mainContent$CenterColumnContent$btnInmateDetail': ''
    }
    
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Referer': JAIL_PAGE_URL,
        'Origin': BASE_URL
    }
    
    try:
        # allow_redirects=False to capture the 302
        resp = session.post(JAIL_PAGE_URL, data=data, headers=headers, allow_redirects=False, timeout=15)
        if resp.status_code == 302:
            return resp.headers.get('Location')
    except Exception as e:
        pass
    return None

def fetch_inmate_details(session, record_index, viewstate, viewstategen, eventvalidation):
    # 1. Get URL
    location = get_detail_url(session, record_index, viewstate, viewstategen, eventvalidation)
    if not location:
        return None, [], None, None, None
        
    full_url = f"{BASE_URL}/{location}"
    
    try:
        resp = session.get(full_url, timeout=15)
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        # 2. Extract Name for Verification
        detail_name = None
        name_span = soup.find('span', id='mainContent_CenterColumnContent_lblName')
        if name_span:
            detail_name = name_span.get_text(strip=True)

        # 3. Extract Total Bond
        total_bond = None
        bond_span = soup.find('span', id='mainContent_CenterColumnContent_lblTotalBoundAmount')
        if bond_span:
            total_bond = bond_span.get_text(strip=True)
            if "NO BOND" in total_bond.upper() or "N/A" in total_bond.upper():
                total_bond = "0.00"

        # 4. Extract Next Court Date
        next_court_date = None
        court_span = soup.find('span', id='mainContent_CenterColumnContent_lblNextCourtDate')
        if court_span:
            court_str = court_span.get_text(strip=True)
            if court_str:
                next_court_date = parse_date(court_str)

        # 3. Extract Mugshot URL
        mug_url = None
        mug_img = soup.find('img', id='mainContent_CenterColumnContent_imgPhoto')
        if mug_img and mug_img.get('src'):
            mug_url = mug_img.get('src')
            
        # 4. Extract Charges
        charges = []
        tables = soup.find_all('table')
        for table in tables:
            if table.get('id') in ['classicmenu', 'superfishtb', 'Table1']:
                continue
                
            rows = table.find_all('tr', recursive=False)
            for r_idx, row in enumerate(rows):
                cols = [c.get_text(strip=True) for c in row.find_all(['td', 'th'], recursive=False)]
                if not cols: continue
                if len(cols) < 4: continue
                
                cols_lower = [c.lower() for c in cols]
                if "charge" in cols_lower and "status" in cols_lower and "bond amount" in cols_lower:
                    for data_row in rows[r_idx+1:]:
                        d_cols = [c.get_text(strip=True) for c in data_row.find_all('td', recursive=False)]
                        if len(d_cols) < 4: continue
                        if "charge" in d_cols[0].lower(): continue
                        
                        val0 = d_cols[0].lower()
                        if val0.startswith("name") or val0.startswith("age") or val0.startswith("race"): continue
                        
                        charge_obj = {
                            'charge': d_cols[0],
                            'status': d_cols[1],
                            'docket': d_cols[2],
                            'bond': d_cols[3]
                        }
                        charges.append(charge_obj)
                    break 
            
            if charges:
                break 
                
        return total_bond, charges, mug_url, detail_name, next_court_date
        
    except Exception as e:
        return None, [], None, None, None


# --- WORKER: Process Batch ---
def process_batch(batch, valid_proxies):
    """
    Worker function to process a list of inmate records (dicts) in a separate session.
    """
    global total_processed, total_inserted, total_updated, total_errors, total_released

    # 1. Setup Session & Init
    initialized = False
    session = None
    proxy = None
    
    # Try up to 3 times to get a working session that can complete the init sequence
    for _ in range(3):
        # We use main.aspx as the test url for basic connectivity
        session, proxy = shared_utils.get_resilient_session(
            user_agent=None,
            proxy_pool=valid_proxies,
            test_url=f"{BASE_URL}/main.aspx"
        )
        
        if not session:
            continue

        try:
            # Init sequence (Main -> Jail -> Search) to establish state
            # session.get(main) already done by test_url check implicitly? 
            # No, get_resilient_session just checks it. We might need to keep cookies.
            # Safe to redo.
            
            resp = session.get(JAIL_PAGE_URL, timeout=15)
            soup = BeautifulSoup(resp.text, 'html.parser')
            
            vs_input = soup.find('input', {'name': '__VIEWSTATE'})
            if not vs_input:
                continue
                
            viewstate = vs_input.get('value', '')
            viewstategen_input = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})
            viewstategen = viewstategen_input.get('value', '') if viewstategen_input else ''
            eventvalidation_input = soup.find('input', {'name': '__EVENTVALIDATION'})
            eventvalidation = eventvalidation_input.get('value', '') if eventvalidation_input else ''

            # Perform Search (Load Grid)
            jq_payload = {
                "t": "ii", "_search": "false", "nd": int(time.time() * 1000), "rows": 10000, "page": 1, "sidx": "disp_name", "sord": "asc"
            }
            jq_headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "Origin": BASE_URL, "Referer": JAIL_PAGE_URL, "X-Requested-With": "XMLHttpRequest"
            }
            session.post(DATA_URL, data=jq_payload, headers=jq_headers, timeout=30)
            
            initialized = True
            break
            
        except Exception as e:
            time.sleep(1)
            pass
            
    if not initialized:
        status("Worker", "Failed to initialize session after retries. Skipping batch.")
        with stats_lock: total_errors += len(batch)
        return

    # 2. Process Records - Build Batch for API
    inmates_payload = []
    
    for record in batch:
        book_id = record.get('book_id')
        record_index = record.get('my_num')

        total_bond, charges, mug_src, detail_name, next_court_date = None, [], None, None, None
        
        if record_index is not None:
            try:
                # Re-fetch Page for strict ViewState sync
                resp = session.get(JAIL_PAGE_URL, timeout=10)
                soup_page = BeautifulSoup(resp.text, 'html.parser')
                vs_input = soup_page.find('input', {'name': '__VIEWSTATE'})
                if vs_input:
                    viewstate = vs_input.get('value', '')
                    viewstategen = soup_page.find('input', {'name': '__VIEWSTATEGENERATOR'}).get('value', '')
                    eventvalidation = soup_page.find('input', {'name': '__EVENTVALIDATION'}).get('value', '')
                    
                    total_bond, charges, mug_src, detail_name, next_court_date = fetch_inmate_details(session, record_index, viewstate, viewstategen, eventvalidation)
            except Exception as e:
                pass
        
        # Download Photo
        photo_b64 = None
        if mug_src:
            pk = f"{BASE_URL}/{mug_src}" if not mug_src.startswith("http") else mug_src
            try:
                p_resp = session.get(pk, headers={"Referer": f"{BASE_URL}/InmateDetail.aspx"}, timeout=10)
                if p_resp.status_code == 200 and len(p_resp.content) != 1981:
                    photo_b64 = base64.b64encode(p_resp.content).decode('utf-8')
            except: pass

        # Construct DTO
        charges_dto = []
        if charges:
            for c in charges:
                 charges_dto.append({
                     "charge_description": c['charge'],
                     "status": c['status'],
                     "docket_number": c['docket'],
                     "bond_amount": c['bond'],
                     "disp_charge": record.get('disp_charge')
                 })
        else:
             charges_dto.append({
                 "charge_description": record.get('chrgdesc'),
                 "disp_charge": record.get('disp_charge')
             })

        # Helper for safe isoformat
        def to_iso(dt):
            return dt.isoformat() if dt else None

        inmate_dto = {
            "book_id": book_id,
            "invid": record.get("invid"),
            "firstname": record.get("firstname"),
            "lastname": record.get("lastname"),
            "middlename": record.get("middlename"),
            "disp_name": record.get("disp_name"),
            "age": record.get("age"),
            "dob": to_iso(parse_date(record.get("dob"))), 
            "sex": record.get("sex"),
            "race": record.get("race"),
            "arrest_date": to_iso(parse_date(record.get("date_arr"))),
            "agency": record.get("agency"),
            "disp_agency": record.get("disp_agency"),
            "total_bond_amount": total_bond if total_bond else None,
            "next_court_date": next_court_date.isoformat() if next_court_date else None,
            "photo_data": photo_b64,
            "charges": charges_dto
        }
        inmates_payload.append(inmate_dto)

    # API Sync
    if inmates_payload:
        try:
             client = APIClient()
             # Endpoint expects { inmates: [...] }
             payload = { "inmates": inmates_payload }
             res = client.post_ingestion("jail/sync", payload)
             with stats_lock:
                 total_inserted += res.get('inserted', 0)
                 total_updated += res.get('updated', 0)
                 total_released += res.get('released', 0)
                 total_processed += len(inmates_payload)
        except Exception as e:
             status("Worker", f"Batch sync failed: {e}")
             with stats_lock: total_errors += 1


# --- MAIN ORCHESTRATOR ---
def main():
    global total_released
    
    # 0. Args
    parser = argparse.ArgumentParser(description="P2C Jail Inmates Scraper")
    parser.add_argument("--LOG_LEVEL", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging verbosity level")
    args = parser.parse_args()

    # 1. Logging
    shared_utils.setup_logging(args.LOG_LEVEL)

    # 2. Config
    config = shared_utils.get_config()
    
    # 3. Proxies
    raw_proxies = shared_utils.get_proxies_from_source(config=config)
    valid_proxies = shared_utils.validate_proxies(raw_proxies, target_count=30, test_url=BASE_URL)
    
    if not valid_proxies:
        status("Main", "WARNING: No valid proxies found. Proceeding with direct connections.")
        valid_proxies = []

    # 4. Initial List Scrape (Single Thread)
    status("Main", "Fetching Inmate List...")
    all_rows = []
    
    list_success = False
    
    # We try up to 3 resilient sessions (each having up to 9 attempts)
    for _ in range(3):
        start_session, _ = shared_utils.get_resilient_session(
            user_agent=None,
            proxy_pool=valid_proxies,
            test_url=f"{BASE_URL}/main.aspx"
        )
        
        if not start_session:
            # Refresh if needed
            valid_proxies = shared_utils.refresh_proxy_pool(valid_proxies)
            continue
        
        try:
            start_session.get(JAIL_PAGE_URL, timeout=15)
            
            jq_payload = {
                "t": "ii", "_search": "false", "nd": int(time.time() * 1000), "rows": 10000, "page": 1, "sidx": "disp_name", "sord": "asc"
            }
            jq_headers = {
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "Origin": BASE_URL, "Referer": JAIL_PAGE_URL, "X-Requested-With": "XMLHttpRequest"
            }
            resp = start_session.post(DATA_URL, data=jq_payload, headers=jq_headers, timeout=30)
            resp.raise_for_status()
            
            data = resp.json()
            all_rows = data.get('rows', [])
            status("Main", f"Found {len(all_rows)} records. Starting batch processing...")
            list_success = True
            break
            
        except Exception as e:
            status("Main", f"List fetch attempt failed: {e}. Retrying...")
            time.sleep(2)
            pass
            
    if not list_success:
        status("Main", "FATAL: Could not fetch inmate list after retries.")
        sys.exit(1)

    # 5. Parallel Batch Processing
    BATCH_SIZE = 10 
    chunks = [all_rows[i:i + BATCH_SIZE] for i in range(0, len(all_rows), BATCH_SIZE)]
    
    workers = int(config.get('workers', 10))
    status("Main", f"Processing {len(chunks)} batches with {workers} threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_batch, chunk, valid_proxies) for chunk in chunks]
        concurrent.futures.wait(futures)

    print("\n" + "="*30)
    print("      JAIL SCRAPE SUMMARY")
    print("="*30)
    print(f"  Total Processed: {total_processed}")
    print(f"  Total Inserted:  {total_inserted}")
    print(f"  Total Updated:   {total_updated}")
    print(f"  Total Released:  {total_released}")
    print(f"  Total Errors:    {total_errors}")
    print("="*30 + "\n")
    
    if total_errors > 0:
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()

