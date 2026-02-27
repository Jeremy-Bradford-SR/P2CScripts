import requests
import random
import time
import json
import pyodbc
import sys
import concurrent.futures
import threading
import argparse
import os
from datetime import datetime
from dotenv import load_dotenv

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import shared_utils
import shared_utils
from shared_utils import status, parse_date, APIClient
import base64
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

load_dotenv()

# --- CONFIGURATION ---
SEARCH_BASE_URL = "https://www.iowasexoffender.gov/api/search/results.json"
DETAIL_BASE_URL = "https://www.iowasexoffender.gov/api/registrant/"

# --- GLOBAL STATS ---
total_inserted = 0
total_skipped = 0
total_errors = 0
stats_lock = threading.Lock()

def construct_dto(reg, photo_data=None):
    registrant_id = reg.get('registrant')
    if not registrant_id:
        registrant_id = reg.get('registrant_id') or reg.get('id')
    registrant_id = str(registrant_id)
    
    # Map Children
    conviction_list = []
    for c in reg.get('convictions', []):
        victims_dto = []
        for v in c.get('victims', []):
            victims_dto.append({
                "gender": v.get('gender'),
                "age_group": v.get('age')
            })
        conviction_list.append({
            "conviction_text": c.get('conviction'),
            "registrant_age": str(c.get('registrant_age') or "0"), # DB requires Int-compatible string
            "victims": victims_dto
        })
        
    alias_list = []
    for a in reg.get('aliases', []):
        alias_list.append({
            "last_name": a.get('last_name'),
            "first_name": a.get('first_name'),
            "middle_name": a.get('middle_name')
        })
        
    markings = []
    for m in reg.get('skin_markings', []):
        val = m if isinstance(m, str) else m.get('marking_value')
        markings.append(val)
        
    photo_b64 = "" # Default to empty string instead of None
    if photo_data:
        photo_b64 = base64.b64encode(photo_data).decode('utf-8')
        
    dto = {
        "registrant_id": registrant_id,
        "oci": str(reg.get('oci')) if reg.get('oci') else None,
        "last_name": reg.get('last_name'),
        "first_name": reg.get('first_name'),
        "middle_name": reg.get('middle_name'),
        "gender": reg.get('gender'),
        "tier": reg.get('tier'),
        "race": reg.get('race'),
        "hair_color": reg.get('hair_color'),
        "eye_color": reg.get('eye_color'),
        "height_inches": str(reg.get('height_inches')) if reg.get('height_inches') else None,
        "weight_pounds": str(reg.get('weight_pounds')) if reg.get('weight_pounds') else None,
        "address_line_1": reg.get('line_1'),
        "address_line_2": reg.get('line_2'),
        "city": reg.get('city'),
        "state": reg.get('state'),
        "postal_code": reg.get('postal_code'),
        "county": reg.get('county'),
        "lat": reg.get('lat'),
        "lon": reg.get('lon'),
        "lat": reg.get('lat'),
        "lon": reg.get('lon'),
        "birthdate": parse_date(reg.get('birthdate')).isoformat() if reg.get('birthdate') else None,
        "victim_minors": reg.get('victim_minors'),
        "victim_adults": reg.get('victim_adults'),
        "victim_unknown": reg.get('victim_unknown'),
        "registrant_cluster": reg.get('registrant_cluster'),
        "photo_url": reg.get('photo'),
        "distance": float(reg.get('distance')) if reg.get('distance') and str(reg.get('distance')).lower() != "not available" else None,
        "last_changed": parse_date(reg.get('last_changed')).isoformat() if reg.get('last_changed') else None,
        "photo_data": photo_b64,
        "conviction_list": conviction_list,
        "alias_list": alias_list,
        "markings": markings
    }
    return dto

# --- WORKER FUNCTIONS ---
@shared_utils.get_retry_decorator(max_attempts=6, wait_seconds=1)
def fetch_registrant_data_with_retry(url, proxy_pool):
    """
    Fetches registrar data with automatic retries, rotating proxies each attempt.
    """
    session, proxy = shared_utils.get_resilient_session(
        user_agent=None,
        proxy_pool=proxy_pool,
        verify=False
    )
    if not session:
        raise Exception("Failed to acquire session.")
        
    try:
        resp = session.get(url, timeout=20, verify=False)
        # Check 404 explicitly to stop retrying
        if resp.status_code == 404:
             return None
        resp.raise_for_status()
        return resp.json(), session
    except requests.exceptions.RequestException as e:
        import logging
        logging.warning(f"[ProxyManager] {proxy} timed out on detail fetch. Discarding and retrying...")
        raise e

def fetch_and_process_registrant(registrant_id, proxy_pool):
    global total_inserted, total_skipped, total_errors
    
    url = f"{DETAIL_BASE_URL}{registrant_id}.json"

    try:
        result = fetch_registrant_data_with_retry(url, proxy_pool)
        
        if result is None: # 404
             with stats_lock: total_skipped += 1
             return

        data_raw, session = result
        
        # Data Parsing
        try:
             # Handle list wrapping if any
            if isinstance(data_raw, list):
                if not data_raw:
                    with stats_lock: total_skipped += 1
                    return
                if isinstance(data_raw[0], str): # Bad format
                    with stats_lock: total_errors += 1
                    return
                data = data_raw[0]
            else:
                data = data_raw

            if not isinstance(data, dict):
                 with stats_lock: total_errors += 1
                 return

            # Download Photo if URL exists (Best Effort)
            photo_data = None
            if data.get('photo'):
                try:
                    p_resp = session.get(data['photo'], timeout=10, verify=False)
                    if p_resp.status_code == 200:
                        photo_data = p_resp.content
                except: pass

            # Construct DTO
            dto = construct_dto(data, photo_data)
            
            api = APIClient()
            payload = { "registrants": [dto] }
            api.post_ingestion("sex-offenders/batch", payload)
            
            with stats_lock:
                total_inserted += 1
                
        except json.JSONDecodeError:
            status("Worker", f"Failed to decode JSON for {registrant_id}.")
            with stats_lock: total_errors += 1
        except Exception as e:
            import logging
            logging.error(f"Worker Processing Error for {registrant_id}: {e}")
            with stats_lock: total_errors += 1
            
    except Exception as e:
        # If retry failed finally after 6 proxy rotations, mark as skipped so it doesn't fail the job
        import logging
        logging.warning(f"Worker completely exhausted retries for {registrant_id}: {e}. Skipping.")
        with stats_lock: total_skipped += 1

# --- MAIN ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Iowa Sex Offender Scraper")
    parser.add_argument("--update", action="store_true", help="Fetch only updated records (updated=yesterday)")
    parser.add_argument("--max_workers", type=int, default=10, help="Number of concurrent workers")
    parser.add_argument("--LOG_LEVEL", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging verbosity level")
    parser.add_argument("--config", type=str, default="{}", help="JSON config string override")
    args = parser.parse_args()
    
    # 0. Logging
    shared_utils.setup_logging(args.LOG_LEVEL)

    # 1. Config
    config = shared_utils.get_config()
    
    # Merge JSON config into config dict
    if args.config:
        try:
            json_config = json.loads(args.config)
            config.update(json_config)
        except: pass

    # Apply Config Overrides
    if config.get("update"):
        args.update = True
    if config.get("max_workers"):
        args.max_workers = int(config.get("max_workers"))

    # 2. Get Proxies
    # 2. Get Proxies
    # Standardize fetching (Shared utils handles API -> Config -> URL)
    raw_proxies = shared_utils.get_proxies_from_source(config=config)
    
    # If using API, they are already validated. If strict validation is needed for non-API, validate them.
    # Note: validate_proxies handles the "skip if already validated" check via env var.
    valid_proxies = shared_utils.validate_proxies(raw_proxies, target_count=50, test_url="https://www.iowasexoffender.gov") 
    
    if not valid_proxies:
        status("Main", "[FATAL] No valid proxies found. Direct connections are disabled.")
        sys.exit(1)

    # 3. Search Loop
    all_registrant_ids = set()
    page = 1
    
    status("Main", "Starting Search Loop...")
    
    while True:
        # Get resilient session for search page
        session, proxy = shared_utils.get_resilient_session(
            user_agent=None,
            proxy_pool=valid_proxies,
            verify=False,
            test_url=None
        )
        
        if not session:
            status("Main", "Failed to get session for search page. Retrying/Refreshing...")
            valid_proxies = shared_utils.refresh_proxy_pool(valid_proxies)
            time.sleep(5)
            continue

        try:
            params = {
                "countyname": "Dubuque",
                "per_page": 100,
                "page": page
            }
            if args.update:
                params["updated"] = "yesterday"
                
            query_str = "&".join([f"{k}={v}" for k, v in params.items()])
            search_url = f"{SEARCH_BASE_URL}?{query_str}"
            
            resp = session.get(search_url, timeout=20, verify=False)
            resp.raise_for_status()
            data = resp.json()
            
            records = []
            if isinstance(data, dict):
                records = data.get('records', [])
            elif isinstance(data, list):
                records = data
            
            if not records:
                status("Search", "No more records found. Stopping search.")
                break
                
            count_new = 0
            for rec in records:
                reg_id = rec.get('registrant')
                if reg_id:
                    all_registrant_ids.add(str(reg_id))
                    count_new += 1
            
            status("Search", f"Page {page}: Found {count_new} IDs.")
            
            if count_new == 0:
                break 
                
            page += 1
            time.sleep(1) 
            
        except Exception as e:
            status("Search", f"Error fetching page {page}: {e}")
            time.sleep(2)
            # Loop continues, getting new session next time
            continue

    status("Main", f"Total unique Registrant IDs found: {len(all_registrant_ids)}")

    # 4. Detail Fetch Loop
    status("Main", "Starting Detail Fetch & Insert...")
    
    registrant_list = list(all_registrant_ids)
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [executor.submit(fetch_and_process_registrant, rid, valid_proxies) for rid in registrant_list]
        concurrent.futures.wait(futures)

    status("Main", "Job Complete.")
    print("\n" + "="*30)
    print("      SCRAPING SUMMARY")
    print("="*30)
    print(f"  Total Records Inserted/Updated: {total_inserted}")
    print(f"  Total Skipped (404/Empty):      {total_skipped}")
    print(f"  Total Errors:                   {total_errors}")
    print("="*30 + "\n")
    
    if total_errors > 0:
        sys.exit(1)
    sys.exit(0)

