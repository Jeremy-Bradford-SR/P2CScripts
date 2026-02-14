import requests
import random
import time
import pyodbc
import sys
import concurrent.futures
from datetime import datetime
import os
import argparse
import json
import sys
import os

# Add parent directory (root P2CScripts) to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from dotenv import load_dotenv

import shared_utils
from shared_utils import status, APIClient

# Add scripts.ETL to path (via root) or import directly if package
# Since we added root, we can do:
try:
    from scripts.ETL import backfill_geocoding
except ImportError:
    # If standard import fails, try relative append (for direct execution without package)
    pass

# --- CONFIG ---
CAD_URL = "http://p2c.cityofdubuque.org/cad/cadHandler.ashx?op=s"


def main():
    # 0. Parse Args
    parser = argparse.ArgumentParser(description="P2C CAD Scraper")
    parser.add_argument("--LOG_LEVEL", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"], help="Logging verbosity level")
    args = parser.parse_args()

    # 1. Setup Logging
    shared_utils.setup_logging(args.LOG_LEVEL)
    
    # 2. Load Config
    config = shared_utils.get_config()
    
    # --- Start ---
    status("Start", "Beginning CAD import process")

    # --- Step 3: Get proxy list ---
    status("Proxy Fetch", "Fetching proxy list")
    raw_proxies = shared_utils.get_proxies_from_source(config=config)
    
    # --- Step 4: Validate proxies ---
    status("Proxy Validation", "Validating proxies in parallel")
    valid_proxies = shared_utils.validate_proxies(raw_proxies, target_count=30, test_url="http://p2c.cityofdubuque.org")
    status("Proxy Validation", f"{len(valid_proxies)} proxies passed validation")

    # --- Step 5: Payload ---
    nd_value = str(int(time.time() * 1000) + random.randint(100, 999))
    payload = {
        "t": "css",
        "_search": "false",
        "nd": nd_value,
        "rows": 200,
        "page": 1,
        "sidx": "starttime",
        "sord": "desc"
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "http://p2c.cityofdubuque.org",
        "Referer": "http://p2c.cityofdubuque.org/cad/callsnapshot.aspx",
        "X-Requested-With": "XMLHttpRequest"
    }

    # --- Step 6: Try CAD endpoint ---
    status("CAD Request", "Attempting CAD endpoint with resilient session")
    resp = None
    
    while True:
        # Get resilient session
        # We use the base URL as test_url to ensure connectivity to the target site
        session, proxy = shared_utils.get_resilient_session(
            user_agent=None, 
            proxy_pool=valid_proxies, 
            test_url="http://p2c.cityofdubuque.org"
        )

        if not session:
            status("CAD Request", "Failed to acquire a working session/proxy. Refreshing pool...")
            valid_proxies = shared_utils.refresh_proxy_pool(valid_proxies)
            if not valid_proxies:
                status("CAD Request", "No proxies available after refresh. Sleeping 30s...")
                time.sleep(30)
            continue
        
        try:
            status("CAD Request", f"Posting to CAD endpoint with proxy: {proxy or 'Direct'}")
            resp = session.post(CAD_URL, data=payload, headers=headers, timeout=20)
            resp.raise_for_status()
            status("CAD Request", "Success!")
            break
        except Exception as e:
            status("CAD Request", f"POST failed with proxy {proxy}: {e}")
            # Loop will retry with new session
            time.sleep(2)

    # --- Step 7: Process data ---
    try:
        data = resp.json()
        rows = data.get("rows", [])
        status("Data Processing", f"Retrieved {len(rows)} CAD rows")
    except Exception as e:
        status("Data Processing", f"Failed to parse JSON: {e}")
        sys.exit(1)

    if not rows:
        status("Data Processing", "[ERROR] No records retrieved. Exiting with failure code.")
        sys.exit(1)

    # --- Step 8: Send to API ---
    status("API Sync", "Sending records to API")
    
    api_payload = []
    for r in rows:
        # Parse Dates to ISO 8601
        st = r.get("starttime")
        ct = r.get("closetime")
        try:
            if st: st = shared_utils.parse_date(st).isoformat()
        except: pass
        try:
            if ct: ct = shared_utils.parse_date(ct).isoformat()
        except: pass

        dto = {
            "id": r.get("id"),
            "invid": r.get("invid"),
            "starttime": st,
            "closetime": ct,
            "agency": r.get("agency"),
            "service": r.get("service"),
            "nature": r.get("nature"),
            "address": r.get("address"),
            "geox": float(r.get("geox")) if r.get("geox") else None,
            "geoy": float(r.get("geoy")) if r.get("geoy") else None,
            "marker_details_xml": r.get("marker_details_xml"),
            "rec_key": r.get("rec_key"),
            "icon_url": r.get("icon_url"),
            "icon": r.get("icon")
        }
        api_payload.append(dto)

    inserted_ids = []
    
    if api_payload:
        try:
            client = APIClient()
            # Wrap in object for API
            payload = { "calls": api_payload }
            result = client.post_ingestion("recent-calls/batch", payload)
            
            inserted_count = result.get('inserted', 0)
            skipped_count = result.get('skipped', 0)
            inserted_ids_raw = result.get('insertedIds', [])
            
            inserted_ids = [int(x) for x in inserted_ids_raw]
            
            status("API Sync", f"Inserted {inserted_count} new records")
            status("API Sync", f"Skipped {skipped_count} duplicates")
            
        except Exception as e:
            status("API Sync", f"Batch ingestion failed: {e}")
            sys.exit(1)
            
    # --- Step 9: Geocoding (ETL) ---
    if inserted_ids:
        status("Geocoding", f"Running targeted geocoding for {len(inserted_ids)} new records...")
        try:
            from scripts.ETL import backfill_geocoding
            backfill_geocoding.geocode_and_update('cadHandler', 'id', 'address', 'starttime', target_ids=inserted_ids)
            status("Geocoding", "Geocoding complete.")
        except Exception as e:
            status("Geocoding", f"Geocoding failed: {e}")
            sys.exit(1)
    else:
        status("Geocoding", "No new records to geocode.")
        
    sys.exit(0)

if __name__ == "__main__":
    main()

