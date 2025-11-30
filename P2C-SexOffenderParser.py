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

load_dotenv()

# --- CONFIGURATION ---
MSSQL_SERVER   = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DRIVER   = "{ODBC Driver 18 for SQL Server}"

PROXY_LIST_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"
SEARCH_BASE_URL = "https://www.iowasexoffender.gov/api/search/results.json"
DETAIL_BASE_URL = "https://www.iowasexoffender.gov/api/registrant/"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
]

# --- GLOBAL STATS ---
total_inserted = 0
total_skipped = 0
total_errors = 0
stats_lock = threading.Lock()

def status(step, message):
    """Prints a timestamped status message."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{step}] {message}")

# --- PROXY LOGIC ---
def check_proxy(proxy):
    """Tests a single proxy against a reliable target."""
    test_url = "https://www.google.com" # Using google as a reliable test for internet connectivity
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    try:
        requests.get(test_url, proxies=proxies_dict, timeout=5)
        return proxy
    except:
        return None

def validate_proxies(proxies_list, batch_size=100):
    """Validates a list of proxies in parallel to find working ones."""
    random.shuffle(proxies_list)
    valid_proxies = []
    status("ProxyManager", f"Validating {len(proxies_list)} proxies...")
    for i in range(0, len(proxies_list), batch_size):
        batch = proxies_list[i:i + batch_size]
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            results = list(executor.map(check_proxy, batch))
        valid_batch = [proxy for proxy in results if proxy]
        valid_proxies.extend(valid_batch)
        status("ProxyManager", f"Batch {i//batch_size + 1}: Found {len(valid_batch)} working proxies. Total valid: {len(valid_proxies)}")
        if len(valid_proxies) >= 200: # Stop if we have enough
            break
    return valid_proxies

def get_session(proxy_pool):
    """Returns a session with a random proxy from the pool."""
    session = requests.Session()
    session.headers.update({"User-Agent": random.choice(USER_AGENTS)})
    
    proxy = None
    if proxy_pool:
        proxy = random.choice(proxy_pool)
        session.proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    
    return session, proxy

# --- DATABASE LOGIC ---
def get_db_connection():
    conn_str = f"DRIVER={MSSQL_DRIVER};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD};TrustServerCertificate=yes;"
    return pyodbc.connect(conn_str)

def parse_date(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
    except ValueError:
        try:
             return datetime.strptime(date_str, '%m/%d/%Y')
        except ValueError:
            return None

def upsert_registrant(cursor, reg, photo_blob=None):
    """Parses a single registrant dictionary and upserts into DB."""
    
    # 1. Extract Registrant ID
    # In the detail JSON, 'registrant' is the ID string (e.g., "15343").
    # In search results, it might be different, but this function processes the DETAIL JSON.
    registrant_id = reg.get('registrant')
    
    # Fallback/Safety: if 'registrant' is missing, check other common keys just in case
    if not registrant_id:
        registrant_id = reg.get('registrant_id') or reg.get('id')

    if not registrant_id:
        # If we still don't have an ID, we can't insert.
        # However, if 'reg' itself is a string (which caused previous errors), we should catch it before here.
        # But if we are here, 'reg' is a dict.
        raise ValueError(f"Could not determine registrant_id from data: {str(reg)[:50]}...")

    registrant_id = str(registrant_id)

    oci = reg.get('oci')
    last_name = reg.get('last_name')
    first_name = reg.get('first_name')
    middle_name = reg.get('middle_name')
    gender = reg.get('gender')
    tier = reg.get('tier')
    race = reg.get('race')
    hair_color = reg.get('hair_color')
    eye_color = reg.get('eye_color')
    height_inches = reg.get('height_inches')
    weight_pounds = reg.get('weight_pounds')
    
    # Address - Fields are at top level in the detail JSON
    line_1 = reg.get('line_1')
    line_2 = reg.get('line_2')
    city = reg.get('city')
    state = reg.get('state')
    postal_code = reg.get('postal_code')
    county = reg.get('county')
    lat = reg.get('lat')
    lon = reg.get('lon')
    
    birthdate = parse_date(reg.get('birthdate'))
    victim_minors = reg.get('victim_minors')
    victim_adults = reg.get('victim_adults')
    victim_unknown = reg.get('victim_unknown')
    registrant_cluster = reg.get('registrant_cluster')
    photo_url = reg.get('photo')
    distance_val = reg.get('distance')
    distance = float(distance_val) if distance_val and str(distance_val).lower() != "not available" else None
    last_changed = parse_date(reg.get('last_changed'))

    # Upsert
    cursor.execute("SELECT registrant_id FROM sexoffender_registrants WHERE registrant_id = ?", registrant_id)
    if cursor.fetchone():
        # Update
        update_sql = """
            UPDATE sexoffender_registrants SET
                oci=?, last_name=?, first_name=?, middle_name=?, gender=?, tier=?, race=?,
                hair_color=?, eye_color=?, height_inches=?, weight_pounds=?,
                address_line_1=?, address_line_2=?, city=?, state=?, postal_code=?, county=?,
                birthdate=?, lat=?, lon=?, victim_minors=?, victim_adults=?, victim_unknown=?,
                registrant_cluster=?, photo_url=?, distance=?, last_changed=?, updated_at=GETDATE()
        """
        params = [oci, last_name, first_name, middle_name, gender, tier, race,
                  hair_color, eye_color, height_inches, weight_pounds,
                  line_1, line_2, city, state, postal_code, county,
                  birthdate, lat, lon, victim_minors, victim_adults, victim_unknown,
                  registrant_cluster, photo_url, distance, last_changed]
        
        if photo_blob:
            update_sql += ", photo_data=?"
            params.append(photo_blob)
            
        update_sql += " WHERE registrant_id=?"
        params.append(registrant_id)
        
        cursor.execute(update_sql, params)
    else:
        # Insert
        cols = ["registrant_id", "oci", "last_name", "first_name", "middle_name", "gender", "tier", "race",
                "hair_color", "eye_color", "height_inches", "weight_pounds",
                "address_line_1", "address_line_2", "city", "state", "postal_code", "county",
                "birthdate", "lat", "lon", "victim_minors", "victim_adults", "victim_unknown",
                "registrant_cluster", "photo_url", "distance", "last_changed"]
        vals = [registrant_id, oci, last_name, first_name, middle_name, gender, tier, race,
                hair_color, eye_color, height_inches, weight_pounds,
                line_1, line_2, city, state, postal_code, county,
                birthdate, lat, lon, victim_minors, victim_adults, victim_unknown,
                registrant_cluster, photo_url, distance, last_changed]
        
        if photo_blob:
            cols.append("photo_data")
            vals.append(photo_blob)
            
        q_marks = ", ".join(["?"] * len(cols))
        col_str = ", ".join(cols)
        
        cursor.execute(f"INSERT INTO sexoffender_registrants ({col_str}) VALUES ({q_marks})", vals)

    # Child Tables
    cursor.execute("DELETE FROM sexoffender_convictions WHERE registrant_id = ?", registrant_id)
    convictions = reg.get('convictions', [])
    if convictions:
        for conv in convictions:
            conv_text = conv.get('conviction')
            reg_age = conv.get('registrant_age')
            cursor.execute("""
                INSERT INTO sexoffender_convictions (registrant_id, conviction_text, registrant_age)
                OUTPUT INSERTED.conviction_id
                VALUES (?, ?, ?)
            """, (registrant_id, conv_text, reg_age))
            conviction_id = cursor.fetchone()[0]
            
            # JSON uses lowercase 'victims'
            victims = conv.get('victims', [])
            if victims:
                for vic in victims:
                    v_gender = vic.get('gender')
                    v_age = vic.get('age')
                    cursor.execute("""
                        INSERT INTO sexoffender_conviction_victims (conviction_id, gender, age_group)
                        VALUES (?, ?, ?)
                    """, (conviction_id, v_gender, v_age))

    cursor.execute("DELETE FROM sexoffender_aliases WHERE registrant_id = ?", registrant_id)
    aliases = reg.get('aliases', [])
    if aliases:
        for alias in aliases:
            a_last = alias.get('last_name')
            a_first = alias.get('first_name')
            a_middle = alias.get('middle_name')
            cursor.execute("""
                INSERT INTO sexoffender_aliases (registrant_id, last_name, first_name, middle_name)
                VALUES (?, ?, ?, ?)
            """, (registrant_id, a_last, a_first, a_middle))

    cursor.execute("DELETE FROM sexoffender_skin_markings WHERE registrant_id = ?", registrant_id)
    markings = reg.get('skin_markings', [])
    if markings:
        for mark in markings:
            val = mark if isinstance(mark, str) else mark.get('marking_value')
            cursor.execute("""
                INSERT INTO sexoffender_skin_markings (registrant_id, marking_value)
                VALUES (?, ?)
            """, (registrant_id, val))

# --- WORKER FUNCTIONS ---
def fetch_and_process_registrant(registrant_id, proxy_pool):
    """Worker function to fetch detail and insert into DB."""
    global total_inserted, total_skipped, total_errors
    
    url = f"{DETAIL_BASE_URL}{registrant_id}.json"
    max_retries = 5
    
    for attempt in range(max_retries):
        session, proxy = get_session(proxy_pool)
        try:
            resp = session.get(url, timeout=15)
            if resp.status_code == 404:
                with stats_lock:
                    total_skipped += 1
                return
            
            resp.raise_for_status()
            
            try:
                data = resp.json()
            except json.JSONDecodeError:
                status("Worker", f"Failed to decode JSON for {registrant_id}. Response might be HTML. First 100 chars: {resp.text[:100]}")
                with stats_lock:
                    total_errors += 1
                return

            # Robustness & Debugging
            if isinstance(data, list):
                if not data:
                    status("Worker", f"Empty list returned for {registrant_id}")
                    with stats_lock:
                        total_skipped += 1
                    return
                
                # Check if the list contains dictionaries
                if isinstance(data[0], str):
                    # If it's a list of strings, it might be lines of HTML or text.
                    status("Worker", f"Unexpected format for {registrant_id}: List of strings. First item: {data[0][:100]}")
                    with stats_lock:
                        total_errors += 1
                    return
                
                # Assume the first item is the record we want
                data = data[0]

            if not isinstance(data, dict):
                status("Worker", f"Unexpected data type for {registrant_id}: {type(data)}. Content: {str(data)[:100]}")
                with stats_lock:
                    total_errors += 1
                return
            
            # DB Operation
            conn = get_db_connection()
            cursor = conn.cursor()
            try:
                # Try to download photo if URL exists
                photo_url = data.get('photo')
                photo_data = None
                if photo_url and photo_url.startswith('http'):
                    try:
                        # Use same session/proxy to fetch photo
                        p_resp = session.get(photo_url, timeout=10)
                        if p_resp.status_code == 200:
                            photo_data = p_resp.content
                            # status("Worker", f"Downloaded photo for {registrant_id} ({len(photo_data)} bytes)")
                        else:
                            status("Worker", f"Failed to fetch photo for {registrant_id}: Status {p_resp.status_code}")
                    except Exception as pe:
                        status("Worker", f"Error fetching photo for {registrant_id}: {pe}")

                upsert_registrant(cursor, data, photo_data) # Pass photo_data to upsert
                conn.commit()
                with stats_lock:
                    total_inserted += 1
            except Exception as e:
                status("Worker", f"DB Error for {registrant_id}: {e}")
                with stats_lock:
                    total_errors += 1
            finally:
                conn.close()
            
            return # Success
            
        except requests.RequestException as e:
            # status("Worker", f"Network error for {registrant_id} (Attempt {attempt+1}): {e}")
            pass # Retry
        except Exception as e:
            status("Worker", f"Unexpected error for {registrant_id}: {e}")
            break # Don't retry logic errors
            
    with stats_lock:
        total_errors += 1
    status("Worker", f"Failed to fetch {registrant_id} after retries.")

# --- MAIN ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Iowa Sex Offender Scraper")
    parser.add_argument("--update", action="store_true", help="Fetch only updated records (updated=yesterday)")
    parser.add_argument("--max_workers", type=int, default=10, help="Number of concurrent workers")
    args = parser.parse_args()

    # 1. Get Proxies
    status("Main", "Fetching proxy list...")
    try:
        proxy_resp = requests.get(PROXY_LIST_URL, timeout=10)
        proxy_resp.raise_for_status()
        proxies_list = [line.split("://")[-1].strip() for line in proxy_resp.text.splitlines() if line.strip()]
        valid_proxies = validate_proxies(proxies_list)
    except Exception as e:
        status("Main", f"Proxy fetch failed: {e}. Using direct connection.")
        valid_proxies = []

    if not valid_proxies:
        status("Main", "WARNING: No valid proxies found. Proceeding with direct connections (high risk of blocking).")

    # 2. Search Loop
    all_registrant_ids = set()
    page = 1
    
    status("Main", "Starting Search Loop...")
    
    while True:
        params = {
            "countyname": "Dubuque",
            "per_page": 100,
            "page": page
        }
        if args.update:
            params["updated"] = "yesterday"
            
        # Construct URL manually to ensure order/encoding if needed, but params dict is usually fine.
        # However, the user specified a specific URL structure.
        # https://www.iowasexoffender.gov/api/search/results.json?countyname=Dubuque&per_page=100&page=100&updated=yesterday
        
        query_str = "&".join([f"{k}={v}" for k, v in params.items()])
        search_url = f"{SEARCH_BASE_URL}?{query_str}"
        
        status("Search", f"Fetching Page {page}...")
        
        session, proxy = get_session(valid_proxies)
        try:
            resp = session.get(search_url, timeout=20)
            resp.raise_for_status()
            data = resp.json()
            
            # Structure check: 'records' key or list?
            # User prompt: "object.records for JSON"
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
                # User: "take the results registrant field"
                # Assuming 'registrant' is the ID.
                reg_id = rec.get('registrant')
                if reg_id:
                    all_registrant_ids.add(str(reg_id))
                    count_new += 1
            
            status("Search", f"Page {page}: Found {count_new} IDs.")
            
            if count_new == 0:
                break # Should stop if page returns empty list
                
            page += 1
            time.sleep(1) # Polite delay
            
        except Exception as e:
            status("Search", f"Error fetching page {page}: {e}")
            # Retry logic for search page?
            # For now, let's just retry the same page once or abort.
            # Simple retry:
            time.sleep(5)
            continue

    status("Main", f"Total unique Registrant IDs found: {len(all_registrant_ids)}")

    # 3. Detail Fetch Loop
    status("Main", "Starting Detail Fetch & Insert...")
    
    registrant_list = list(all_registrant_ids)
    batch_size = args.max_workers
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        futures = [executor.submit(fetch_and_process_registrant, rid, valid_proxies) for rid in registrant_list]
        
        # Monitor progress
        completed = 0
        for future in concurrent.futures.as_completed(futures):
            completed += 1
            if completed % 10 == 0:
                status("Progress", f"Processed {completed}/{len(registrant_list)} (Inserted: {total_inserted}, Skipped: {total_skipped}, Errors: {total_errors})")

    status("Main", "Job Complete.")
    print("\n" + "="*30)
    print("      SCRAPING SUMMARY")
    print("="*30)
    print(f"  Total Records Inserted/Updated: {total_inserted}")
    print(f"  Total Skipped (404/Empty):      {total_skipped}")
    print(f"  Total Errors:                   {total_errors}")
    print("="*30 + "\n")
