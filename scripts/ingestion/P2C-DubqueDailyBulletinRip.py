import requests
import random
import time
import logging
import sys
import os
import argparse
import hashlib
import concurrent.futures
import threading
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
from urllib.parse import quote_plus

# Ensure module path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import shared_utils
from shared_utils import APIClient

# --- CONFIGURATION CONSTANTS ---
DATA_URL = "http://p2c.cityofdubuque.org/jqHandler.ashx?op=s"
SESSION_INIT_URL = "http://p2c.cityofdubuque.org/main.aspx"
DAILY_BULLETIN_URL = "http://p2c.cityofdubuque.org/dailybulletin.aspx"

DEFAULT_DAYS_TO_SCRAPE = 7
DEFAULT_MAX_WORKERS = 7
DEFAULT_CHUNK_SIZE = 7

# --- Global Statistics ---
stats_lock = threading.Lock()
total_inserted = 0
total_skipped = 0
total_inserted_ids = []

# Audit Log: {id: {date: str, status: str, details: str}}
audit_log = {}

def update_audit(record_id, date, status, details=""):
    with stats_lock:
        audit_log[record_id] = {"date": date, "status": status, "details": details}

def verify_database_state(start_date, end_date):
    """
    Queries the database for all IDs in the date range and compares with audit_log.
    """
    logging.info("--- Starting Database Verification ---")
    
    # Format dates for SQL / API (ISO 8601)
    s_str = start_date.strftime("%Y-%m-%dT00:00:00")
    e_str = end_date.strftime("%Y-%m-%dT23:59:59")
    
    try:
        api = APIClient()
        resp = api.post("tools/daily-bulletin/ids", {"Start": s_str, "End": e_str})
        
        db_ids = set()
        if isinstance(resp, list):
            for r in resp:
                db_ids.add(str(r)) # Response is list of strings
        elif isinstance(resp, dict) and 'data' in resp: # unexpected wrapper?
             for r in resp['data']:
                db_ids.add(str(r))
        else:
            # Fallback if API returns list of objects but Dapper returns strings?
            # conn.QueryAsync<string> returns IEnumerable<string>. API Controller returns Ok(res).
            # So JSON should be ["id1", "id2", ...]
            # But let's be safe.
            logging.info(f"API Response Type: {type(resp)}")
            if isinstance(resp, list):
                 for r in resp: db_ids.add(str(r))

        logging.info(f"Database contains {len(db_ids)} records for this range.")
        
        # Comparison
        downloaded_ids = set(audit_log.keys())
        
        missing_in_db = downloaded_ids - db_ids
        unexpected_in_db = db_ids - downloaded_ids
        
        logging.info(f"Audit Comparison:")
        logging.info(f"  - Total Downloaded: {len(downloaded_ids)}")
        logging.info(f"  - Total in DB:      {len(db_ids)}")
        logging.info(f"  - Missing in DB:    {len(missing_in_db)}")
        logging.info(f"  - Unexpected in DB: {len(unexpected_in_db)} (Likely old/other data)")
        
        if missing_in_db:
            logging.warning("!!! CRITICAL: The following IDs were downloaded but are MISSING from DB:")
            for mid in list(missing_in_db)[:50]: # Cap at 50
                info = audit_log[mid]
                logging.warning(f"    MISSING: {mid} (Date: {info['date']}, Status: {info['status']}, Details: {info['details']})")
            if len(missing_in_db) > 50:
                logging.warning(f"    ... and {len(missing_in_db) - 50} more.")

    except Exception as e:
        logging.error(f"Verification Failed: {e}")


def get_fresh_session(user_agent, proxy_pool):
    """
    Acquires a new, fresh requests.Session object with a valid ASP.NET_SessionId.
    Implements the 10x3 retry strategy: 
    - Try 3 times with the same proxy.
    - If fails, switch proxy and try 3 times.
    - Repeat up to 10 distinct proxies (Total 30 attempts).
    """
    headers = {"User-Agent": user_agent}

    # Direct Mode (No Proxies)
    if not proxy_pool:
        for attempt in range(3):
            try:
                session = requests.Session()
                resp = session.get(SESSION_INIT_URL, headers=headers, timeout=20)
                resp.raise_for_status()
                if "ASP.NET_SessionId" in session.cookies:
                    return session, None
            except Exception as e:
                logging.warning(f"Direct connection attempt {attempt+1}/3 failed: {e}")
                time.sleep(1)
        return None, None

    # Proxy Mode
    local_proxy_pool = list(proxy_pool)
    random.shuffle(local_proxy_pool)
    
    # Try up to 10 distinct proxies
    for proxy_idx, proxy in enumerate(local_proxy_pool[:10]):
        proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
        
        # Try 3 times per proxy
        for attempt in range(3):
            try:
                session = requests.Session()
                resp = session.get(SESSION_INIT_URL, headers=headers, proxies=proxies_dict, timeout=20)
                resp.raise_for_status()
                if "ASP.NET_SessionId" in session.cookies:
                    return session, proxy 
            except requests.RequestException as e:
                logging.warning(f"Proxy {proxy} attempt {attempt+1}/3 failed: {e}")
                time.sleep(1)
        
        logging.warning(f"Proxy {proxy} failed 3 times. Switching...")

    return None, None

def init_session_form(session, current_date, report_type, user_agent, proxy_in_use):
    """Initializes the ASP.NET form state for a specific date and type."""
    date_str = current_date.strftime("%m/%d/%Y")
    
    try:
        # Step 1: GET page for ViewState
        get_headers = {'User-Agent': user_agent, 'Referer': SESSION_INIT_URL}
        proxies_dict = {"http": f"http://{proxy_in_use}", "https": f"http://{proxy_in_use}"} if proxy_in_use else None
        
        form_page_resp = session.get(DAILY_BULLETIN_URL, headers=get_headers, proxies=proxies_dict, timeout=15)
        form_page_resp.raise_for_status()
        soup = BeautifulSoup(form_page_resp.text, 'html.parser')

        # Step 2: Extract hidden fields
        viewstate = quote_plus(soup.find('input', {'name': '__VIEWSTATE'}).get('value', ''))
        viewstategen = quote_plus(soup.find('input', {'name': '__VIEWSTATEGENERATOR'}).get('value', ''))
        eventvalidation = quote_plus(soup.find('input', {'name': '__EVENTVALIDATION'}).get('value', ''))

        # Step 3: POST to set date/type
        raw_form_data = (
            f"__EVENTTARGET=MasterPage%24mainContent%24lbUpdate&__VIEWSTATE={viewstate}&__VIEWSTATEGENERATOR={viewstategen}"
            f"&__EVENTVALIDATION={eventvalidation}&MasterPage%24mainContent%24ddlType2={report_type}&MasterPage%24mainContent%24txtDate2={quote_plus(date_str)}"
        )
        post_headers = {'User-Agent': user_agent, 'Referer': DAILY_BULLETIN_URL, 'Content-Type': 'application/x-www-form-urlencoded'}
        
        set_date_resp = session.post(DAILY_BULLETIN_URL, data=raw_form_data, headers=post_headers, proxies=proxies_dict, timeout=20)
        set_date_resp.raise_for_status()
        return True
    
    except Exception as e:
        logging.warning(f"Form initialization failed for {date_str}: {e}")
        return False

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + timedelta(n)

def process_day(current_date, valid_proxies):
    global total_inserted, total_skipped
    date_str = current_date.strftime("%m/%d/%Y")
    
    # Try multiple attempts to handle "All proxies dead" scenario
    MAX_DAY_RETRIES = 5
    for day_attempt in range(MAX_DAY_RETRIES):
        logging.info(f"Starting processing for {date_str} (Attempt {day_attempt+1}/{MAX_DAY_RETRIES})")
        
        current_user_agent = random.choice(shared_utils.USER_AGENTS)
        session, proxy_in_use = get_fresh_session(current_user_agent, valid_proxies)

        # -- DYNAMIC PROXY REFRESH --
        if not session and valid_proxies:
            logging.warning(f"All proxies failed for {date_str}. Attempting to refresh proxy pool...")
            new_proxies = shared_utils.refresh_proxies()
            if new_proxies:
                valid_proxies[:] = new_proxies # Update in place for this thread? 
                # Note: valid_proxies is passed by reference (list), so modification affects this scope.
                # However, thread safety might be an issue if we modify the shared list directly?
                # Actually, main passes `valid_proxies` which is a list.
                # We should be careful. But reading is fine.
                # Here we are just using it locally in `get_fresh_session`.
                # Let's verify compatibility. Yes, replace content.
                logging.info(f"Refreshed proxy pool with {len(new_proxies)} proxies. Retrying...")
                time.sleep(2)
                continue # Retry day loop immediately with new proxies
            else:
                 logging.error("Proxy refresh failed or returned empty. Sleeping and retrying...")
                 time.sleep(10)
                 continue

        if not session:
            logging.error(f"FATAL: Could not acquire session for {date_str} after all retries and refresh attempts.")
            return # Skip day

        logging.info(f"Acquired session for {date_str} via {proxy_in_use or 'Direct'}")
        
        # Headers for data requests
        headers = { 
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", 
            "Origin": "http://p2c.cityofdubuque.org", 
            "Referer": "http://p2c.cityofdubuque.org/dailybulletin.aspx", 
            "X-Requested-With": "XMLHttpRequest", 
            "User-Agent": current_user_agent 
        }

        # Process Types (Empty string = ALL)
        REPORT_TYPES = [''] 
        day_success = True
        daily_inserted = 0
        daily_skipped = 0
        daily_ids = []

        for report_type in REPORT_TYPES:
            if not init_session_form(session, current_date, report_type, current_user_agent, proxy_in_use):
                day_success = False
                break
            
            page_num = 1
            while True:
                payload = { "t": "db", "d": date_str, "_search": "false", "nd": int(time.time() * 1000), "rows": 10000, "page": page_num, "sidx": "case", "sord": "asc" }
                proxies_dict = {"http": f"http://{proxy_in_use}", "https": f"http://{proxy_in_use}"} if proxy_in_use else None
                
                try:
                    r = session.post(DATA_URL, data=payload, headers=headers, proxies=proxies_dict, timeout=20)
                    r.raise_for_status()
                    page_data = r.json()
                    rows = page_data.get("rows", [])
                except Exception as e:
                    logging.warning(f"Data fetch failed for {date_str} pg {page_num}: {e}")
                    day_success = False
                    break

                if not rows:
                    break

                # Transform & Load
                batch_dto = []
                for record in rows:
                    try:
                        raw_id = str(record.get('id', '') or '').strip()
                        rec_key = str(record.get('key', '') or '').strip()
                        
                        # Generate a robust, deterministic composite ID.
                        # Aggressively normalize string inputs (strip spaces, upper case, remove HTML artifacts) to ensure 
                        # that superficial changes in the underlying ASP.NET API JSON do not mutate the MD5 hash across cyclical scrapes.
                        def normalize_field(val):
                            s = str(val or '').strip().upper()
                            s = s.replace('<BR>', ' ').replace('<BR/>', ' ').replace('&NBSP;', ' ')
                            # Replace multiple spaces with a single space to handle arbitrary padding changes
                            import re
                            return re.sub(r'\s+', ' ', s).strip()

                        norm_name = normalize_field(record.get('name'))
                        norm_time = normalize_field(record.get('time'))
                        norm_charge = normalize_field(record.get('charge'))
                        norm_location = normalize_field(record.get('location'))
                        
                        unique_blob = f"{raw_id}_{norm_name}_{norm_time}_{norm_charge}_{norm_location}"
                        rec_id = hashlib.md5(unique_blob.encode('utf-8')).hexdigest()

                        dto = {
                            "invid": record.get("invid"),
                            "key": rec_key,
                            "location": record.get("location"),
                            "id": rec_id,
                            "site_id": raw_id,
                            "name": record.get("name"),
                            "crime": record.get("crime"),
                            "time": record.get("time"),
                            "property": record.get("property"),
                            "officer": record.get("officer"),
                            "case": record.get("case"),
                            "description": record.get("description"),
                            "race": record.get("race"),
                            "sex": record.get("sex"),
                            "lastname": record.get("lastname"),
                            "firstname": record.get("firstname"),
                            "charge": record.get("charge"),
                            "middlename": record.get("middlename")
                        }
                        batch_dto.append(dto)
                        
                        # AUDIT LOGGING
                        update_audit(rec_id, date_str, "Downloaded", f"Name: {record.get('name')}, Charge: {record.get('charge')}")
                        
                    except Exception as e:
                        logging.warning(f"Error parsing record: {e}")
                        daily_skipped += 1

                if batch_dto:
                    try:
                        api = APIClient()
                        res = api.post_ingestion("daily-bulletin/batch", batch_dto)
                        daily_inserted += res.get('inserted', 0)
                        daily_skipped += res.get('skipped', 0)
                        daily_ids.extend(res.get('insertedIds', []))
                    except Exception as e:
                        logging.error(f"API Batch Upload Failed: {e}")
                        day_success = False # Fail day on API error
                        break
                
                page_num += 1
                time.sleep(0.5)
            
            if not day_success: break
        
        if day_success:
            with stats_lock:
                total_inserted += daily_inserted
                total_skipped += daily_skipped
                total_inserted_ids.extend(daily_ids)
            logging.info(f"Finished {date_str}. Inserted: {daily_inserted}")
            
            # --- INLINE POST-PROCESSING ---
            if daily_ids:
                try:
                    import os, sys
                    etl_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'ETL')
                    if etl_path not in sys.path: sys.path.append(etl_path)
                    
                    import UpdateDAB_TimetoEventTime
                    import backfill_geocoding
                    
                    logging.info(f"Triggering Target ETL for {len(daily_ids)} rows...")
                    UpdateDAB_TimetoEventTime.update_event_time(target_ids=daily_ids)
                    backfill_geocoding.geocode_and_update('DailyBulletinArrests', 'id', 'location', 'event_time', target_ids=daily_ids)
                except Exception as e:
                    logging.error(f"Inline ETL Failed for {date_str}: {e}")
            # -----------------------------
            return
        
        logging.warning(f"Failed processing {date_str}. Retrying...")
        time.sleep(5)

    logging.error(f"Given up on {date_str} after {MAX_DAY_RETRIES} attempts.")

if __name__ == "__main__":
    # 0. Load Config & Args
    config = shared_utils.get_config()
    
    parser = argparse.ArgumentParser(description="P2C Daily Bulletin Scraper")
    parser.add_argument("--DAYS_TO_SCRAPE", type=int, default=DEFAULT_DAYS_TO_SCRAPE, help="Number of days to scrape backwards from today")
    parser.add_argument("--MAX_WORKERS", type=int, default=DEFAULT_MAX_WORKERS, help="Number of concurrent worker threads")
    parser.add_argument("--CHUNK_SIZE", type=int, default=DEFAULT_CHUNK_SIZE, help="Number of days per processing batch")
    parser.add_argument("--LOG_LEVEL", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging verbosity level")
    parser.add_argument("--config", type=str, default="{}", help="JSON config string override")
    args = parser.parse_args()

    # 1. Setup Logging
    shared_utils.setup_logging(args.LOG_LEVEL)

    # 2. Apply Config
    DAYS_TO_SCRAPE = int(config.get("days", args.DAYS_TO_SCRAPE))
    MAX_WORKERS = int(config.get("workers", args.MAX_WORKERS))
    CHUNK_SIZE = int(config.get("chunk_size", args.CHUNK_SIZE))

    logging.info(f"Configuration: Days={DAYS_TO_SCRAPE}, Workers={MAX_WORKERS}, Chunk={CHUNK_SIZE}, Level={args.LOG_LEVEL}")

    # 3. Proxies
    raw_proxies = shared_utils.get_proxies_from_source(config=config)
    valid_proxies = shared_utils.validate_proxies(raw_proxies, target_count=300)
    if not valid_proxies:
        logging.warning("No working proxies found. Proceeding with DIRECT connection (high block risk).")

    # 4. Processing Loop
    end_date = datetime.now()
    start_date = end_date - timedelta(days=DAYS_TO_SCRAPE - 1)
    dates = list(daterange(start_date, end_date))
    
    logging.info(f"Scraping range: {dates[0].strftime('%Y-%m-%d')} to {dates[-1].strftime('%Y-%m-%d')}")

    for i in range(0, len(dates), CHUNK_SIZE):
        chunk = dates[i:i + CHUNK_SIZE]
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(process_day, d, valid_proxies) for d in chunk]
            concurrent.futures.wait(futures)
        logging.info("Batch complete. Pausing...")
        time.sleep(2)

    # 5. Summary
    logging.info("="*30)
    logging.info(f"SUMMARY: New={total_inserted}, Skipped={total_skipped}")
    logging.info("="*30)

    # Note: ETL Post-Processing is now executed inline inside process_day() on strictly daily batches.
    verify_database_state(dates[0], dates[-1])

    sys.exit(0)
