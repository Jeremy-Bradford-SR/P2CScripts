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
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
MSSQL_SERVER   = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DRIVER   = "{ODBC Driver 18 for SQL Server}"

PROXY_LIST_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"
BASE_URL = "http://p2c.cityofdubuque.org"
JAIL_PAGE_URL = f"{BASE_URL}/jailinmates.aspx"
DATA_URL = f"{BASE_URL}/jqHandler.ashx?op=s"
MUG_URL_TEMPLATE = f"{BASE_URL}/Mug.aspx?Type=4&ImageID={{}}&ss=1"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:126.0) Gecko/20100101 Firefox/126.0",
]

# --- GLOBAL STATS ---
total_processed = 0
total_inserted = 0
total_updated = 0
total_released = 0
total_errors = 0
stats_lock = threading.Lock()

def status(step, message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{step}] {message}")

# --- PROXY LOGIC ---
def check_proxy(proxy):
    test_url = "http://p2c.cityofdubuque.org" # Test against target to ensure reachability
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    try:
        requests.get(test_url, proxies=proxies_dict, timeout=5)
        return proxy
    except:
        return None

def validate_proxies(proxies_list, batch_size=50):
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
        if len(valid_proxies) >= 50: # We don't need too many for this task
            break
    return valid_proxies

def get_session(proxy_pool):
    session = requests.Session()
    session.headers.update({
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    })
    
    proxy = None
    # if proxy_pool:
    #     proxy = random.choice(proxy_pool)
    #     session.proxies = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    
    return session, proxy

# --- DB LOGIC ---
def get_db_connection():
    conn_str = f"DRIVER={MSSQL_DRIVER};SERVER={MSSQL_SERVER};DATABASE={MSSQL_DATABASE};UID={MSSQL_USERNAME};PWD={MSSQL_PASSWORD};TrustServerCertificate=yes;"
    return pyodbc.connect(conn_str)

def parse_date(date_str):
    if not date_str: return None
    try:
        # Format: 9/20/2025 12:00:00 AM
        return datetime.strptime(date_str, '%m/%d/%Y %I:%M:%S %p')
    except ValueError:
        try:
            return datetime.strptime(date_str, '%m/%d/%Y')
        except:
            return None

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
        resp = session.post(JAIL_PAGE_URL, data=data, headers=headers, allow_redirects=False, timeout=10)
        if resp.status_code == 302:
            return resp.headers.get('Location')
    except Exception as e:
        pass
    return None

def fetch_inmate_details(session, record_index, viewstate, viewstategen, eventvalidation):
    # 1. Get URL
    location = get_detail_url(session, record_index, viewstate, viewstategen, eventvalidation)
    if not location:
        return None, []
        
    full_url = f"{BASE_URL}/{location}"
    
    try:
        resp = session.get(full_url, timeout=10)
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

        # 3. Extract Mugshot URL
        mug_url = None
        mug_img = soup.find('img', id='mainContent_CenterColumnContent_imgPhoto')
        if mug_img and mug_img.get('src'):
            mug_url = mug_img.get('src')
            
        # 4. Extract Charges
        # Look for a table that has "Charge" in headers
        charges = []
        # Since we couldn't find the table in our tests, we will look for ANY table that looks like a charges table
        # Or try to parse based on known structure if it appears.
        # For now, we return empty list if not found, but we will log it.
        
        # User said columns: Charge, Status, Docket #, Bond Amount
        # Let's try to find a table row with these headers
        tables = soup.find_all('table')
        for table in tables:
            # Skip navigation tables
            if table.get('id') in ['classicmenu', 'superfishtb', 'Table1']:
                continue
                
            headers = [th.get_text(strip=True).lower() for th in table.find_all(['th', 'td'])]
            
            # Check for specific headers in the first few cells
            # We expect: Charge, Status, Docket #, Bond Amount
            # But sometimes headers are in <th>, sometimes <td> with bold.
            # Let's look for a row that has these values.
            
            rows = table.find_all('tr', recursive=False)
            for r_idx, row in enumerate(rows):
                cols = [c.get_text(strip=True) for c in row.find_all(['td', 'th'], recursive=False)]
                if not cols: continue
                
                # Check if this row is a header row
                if len(cols) < 4: continue
                
                cols_lower = [c.lower() for c in cols]
                # We need to be careful. If the header row has nested elements, get_text might be weird.
                # But usually headers are simple text.
                if "charge" in cols_lower and "status" in cols_lower and "bond amount" in cols_lower:
                    # Found the header row!
                    # Now parse subsequent rows
                    for data_row in rows[r_idx+1:]:
                        d_cols = [c.get_text(strip=True) for c in data_row.find_all('td', recursive=False)]
                        if len(d_cols) < 4: continue
                        
                        # Verify it's not another header or empty
                        if "charge" in d_cols[0].lower(): continue
                        
                        # Skip if it looks like Inmate Info (nested table text leaking or bad row)
                        # Use startswith for stricter check
                        val0 = d_cols[0].lower()
                        if val0.startswith("name") or val0.startswith("age") or val0.startswith("race"): continue
                        
                        charge_obj = {
                            'charge': d_cols[0],
                            'status': d_cols[1],
                            'docket': d_cols[2],
                            'bond': d_cols[3]
                        }
                        charges.append(charge_obj)
                    break # Stop processing this table
            
            if charges:
                break # Found charges, stop looking at other tables
                
        return total_bond, charges, mug_url, detail_name
        
    except Exception as e:
        return None, [], None, None

def upsert_inmate(cursor, record, photo_data, total_bond, charges):
    book_id = record.get('book_id')
    if not book_id: return False

    # Extract fields
    invid = record.get('invid')
    firstname = record.get('firstname')
    lastname = record.get('lastname')
    middlename = record.get('middlename')
    disp_name = record.get('disp_name')
    age = record.get('age')
    dob = parse_date(record.get('dob'))
    sex = record.get('sex')
    race = record.get('race')
    arrest_date = parse_date(record.get('date_arr'))
    agency = record.get('agency')
    disp_agency = record.get('disp_agency')
    
    # Check existence
    cursor.execute("SELECT book_id FROM jail_inmates WHERE book_id = ?", book_id)
    exists = cursor.fetchone()
    
    if exists:
        # Update
        sql = """
            UPDATE jail_inmates SET
                invid=?, firstname=?, lastname=?, middlename=?, disp_name=?,
                age=?, dob=?, sex=?, race=?, arrest_date=?, agency=?, disp_agency=?,
                last_updated=GETDATE(), released_date=NULL, total_bond_amount=?
        """
        params = [invid, firstname, lastname, middlename, disp_name, age, dob, sex, race, arrest_date, agency, disp_agency, total_bond]
        
        if photo_data:
            sql += ", photo_data=?"
            params.append(photo_data)
            
        sql += " WHERE book_id=?"
        params.append(book_id)
        cursor.execute(sql, params)
        result = "updated"
    else:
        # Insert
        sql = """
            INSERT INTO jail_inmates (
                book_id, invid, firstname, lastname, middlename, disp_name,
                age, dob, sex, race, arrest_date, agency, disp_agency, photo_data, total_bond_amount
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        cursor.execute(sql, (book_id, invid, firstname, lastname, middlename, disp_name, age, dob, sex, race, arrest_date, agency, disp_agency, photo_data, total_bond))
        result = "inserted"

    # Handle Charges
    # If we found detailed charges, use them.
    # If not, fall back to the 'disp_charge' from the main record (as a single charge)
    
    if charges:
        # We have detailed charges.
        # Strategy: Delete existing charges for this book_id and re-insert?
        # Or try to match?
        # Deleting and re-inserting is safest to ensure we capture current state (e.g. dropped charges).
        cursor.execute("DELETE FROM jail_charges WHERE book_id=?", book_id)
        for ch in charges:
            cursor.execute("""
                INSERT INTO jail_charges (book_id, charge_description, status, docket_number, bond_amount)
                VALUES (?, ?, ?, ?, ?)
            """, (book_id, ch['charge'], ch['status'], ch['docket'], ch['bond']))
    else:
        # Fallback to main record charge if no details found (and no existing charges?)
        # Or just upsert the main charge.
        charge_desc = record.get('chrgdesc')
        disp_charge = record.get('disp_charge')
        
        # Only insert if we didn't just wipe them (meaning we didn't find detailed charges)
        # Check if we have charges
        cursor.execute("SELECT count(*) FROM jail_charges WHERE book_id=?", book_id)
        if cursor.fetchone()[0] == 0:
             cursor.execute("INSERT INTO jail_charges (book_id, charge_description, disp_charge) VALUES (?, ?, ?)", (book_id, charge_desc, disp_charge))

    return result

# --- MAIN WORKER ---
def process_inmates(valid_proxies):
    global total_processed, total_inserted, total_updated, total_errors
    
    status("Main", "Starting Inmate Scrape...")
    
    # 1. Get Session & Initialize
    session, proxy = get_session(valid_proxies)
    viewstate, viewstategen, eventvalidation = None, None, None
    
    try:
        # Use main.aspx to initialize session
        resp = session.get(f"{BASE_URL}/main.aspx", timeout=15)
        status("Main", f"Session initialized.")
        
        # Now hit jailinmates.aspx to get ViewState
        resp = session.get(JAIL_PAGE_URL, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, 'html.parser')
        
        vs_input = soup.find('input', {'name': '__VIEWSTATE'})
        if not vs_input:
            raise Exception("Could not find __VIEWSTATE")
        viewstate = vs_input.get('value', '')
        
        vsg_input = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})
        viewstategen = vsg_input.get('value', '') if vsg_input else ''
        
        ev_input = soup.find('input', {'name': '__EVENTVALIDATION'})
        eventvalidation = ev_input.get('value', '') if ev_input else ''
        
        # Perform Search to populate session
        jq_url = f"{BASE_URL}/jqHandler.ashx?op=s"
        jq_payload = {
            "t": "ii",
            "_search": "false",
            "nd": int(time.time() * 1000),
            "rows": 10000, # Fetch ALL rows to get complete list
            "page": 1,
            "sidx": "disp_name",
            "sord": "asc"
        }
        jq_headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "Origin": BASE_URL,
            "Referer": JAIL_PAGE_URL,
            "X-Requested-With": "XMLHttpRequest"
        }
        status("Main", "Performing search to populate session...")
        resp = session.post(jq_url, data=jq_payload, headers=jq_headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        all_rows = data.get('rows', [])
        status("Main", f"Search complete. Found {len(all_rows)} records.")
        
    except Exception as e:
        status("Main", f"Failed to init/search: {e}")
        return

    # 3. Process Records
    current_book_ids = set()
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for i, record in enumerate(all_rows):
        book_id = record.get('book_id')
        if not book_id: continue
        
        current_book_ids.add(book_id)
        
        # Fetch Details
        record_index = record.get('my_num')

        
        total_bond, charges, mug_src, detail_name = None, [], None, None
        if record_index is not None:
            # status("Scraper", f"Fetching details for {book_id} (Index {record_index})...")
            total_bond, charges, mug_src, detail_name = fetch_inmate_details(session, record_index, viewstate, viewstategen, eventvalidation)
            
            # Verify Name
            if detail_name:
                # Basic check - first name match?
                # Record name format: "ALANA-RENDILES, ANNEIRO J (W /M/34)"
                # Detail name format: "ALANA-RENDILES, ANNEIRO J"
                rec_last = record.get('lastname', '').upper()
                if rec_last not in detail_name.upper():
                    status("WARN", f"Name Mismatch! Expected {rec_last} in '{detail_name}'")
                    # If name doesn't match, the session didn't update. Skip this one or retry?
                    # For now, let's just log it. If it happens, we know why photos are wrong.

        
        # Download Photo
        photo_data = None
        if mug_src:
            # Resolve URL (handle relative)
            if not mug_src.startswith("http"):
                # Handle leading slash or not
                if mug_src.startswith("/"):
                    photo_url = f"{BASE_URL}{mug_src}"
                else:
                    # Relative to current page (InmateDetail.aspx) -> usually base/Mug.aspx
                    photo_url = f"{BASE_URL}/{mug_src}"
            else:
                photo_url = mug_src
            
            # Add cache buster
            if "?" in photo_url:
                photo_url += f"&_={int(time.time()*1000)}"
            else:
                photo_url += f"?_={int(time.time()*1000)}"
                
            try:
                p_resp = session.get(photo_url, timeout=10)
                if p_resp.status_code == 200:
                    photo_data = p_resp.content
            except:
                pass

        try:
            res = upsert_inmate(cursor, record, photo_data, total_bond, charges)
            if res == "inserted":
                total_inserted += 1
            elif res == "updated":
                total_updated += 1
            total_processed += 1
        except Exception as e:
            status("Scraper", f"DB Error for {book_id}: {e}")
            total_errors += 1
            
        if total_processed % 10 == 0:
            conn.commit()
            status("Progress", f"Processed {total_processed}/{len(all_rows)}")
            
    conn.commit()
    
    # 4. Handle Releases
    status("Releaser", "Checking for released inmates...")
    if current_book_ids:
        cursor.execute("SELECT book_id FROM jail_inmates WHERE released_date IS NULL")
        active_db_ids = [row.book_id for row in cursor.fetchall()]
        released_ids = [bid for bid in active_db_ids if bid not in current_book_ids]
        
        for rid in released_ids:
            cursor.execute("UPDATE jail_inmates SET released_date = GETDATE() WHERE book_id = ?", rid)
            global total_released
            total_released += 1
            
        conn.commit()
        status("Releaser", f"Marked {len(released_ids)} inmates as released.")

    conn.close()

if __name__ == "__main__":
    # Get Proxies
    try:
        proxy_resp = requests.get(PROXY_LIST_URL, timeout=10)
        proxies_list = [line.split("://")[-1].strip() for line in proxy_resp.text.splitlines() if line.strip()]
        valid_proxies = validate_proxies(proxies_list)
    except:
        valid_proxies = []

    process_inmates(valid_proxies)
    
    print("\n" + "="*30)
    print("      JAIL SCRAPE SUMMARY")
    print("="*30)
    print(f"  Total Processed: {total_processed}")
    print(f"  Total Inserted:  {total_inserted}")
    print(f"  Total Updated:   {total_updated}")
    print(f"  Total Released:  {total_released}")
    print(f"  Total Errors:    {total_errors}")
    print("="*30 + "\n")
