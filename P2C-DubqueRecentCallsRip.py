import requests
import random
import time
import pyodbc
import sys
import concurrent.futures
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# --- CONFIG ---
MSSQL_SERVER   = os.getenv("MSSQL_SERVER")
MSSQL_DATABASE = os.getenv("MSSQL_DATABASE")
MSSQL_USERNAME = os.getenv("MSSQL_USERNAME")
MSSQL_PASSWORD = os.getenv("MSSQL_PASSWORD")
MSSQL_DRIVER   = "{ODBC Driver 18 for SQL Server}"

CAD_URL = "http://p2c.cityofdubuque.org/cad/cadHandler.ashx?op=s"
PROXY_LIST_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2; rv:118.0) Gecko/20100101 Firefox/118.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36 Edg/117.0.2045.43",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Mobile Safari/537.36"
]

def status(step_name, message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [STATUS] {step_name}: {message}")

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

# --- Start ---
status("Start", "Beginning CAD import process")

# --- Step 1: Get proxy list ---
status("Proxy Fetch", "Fetching proxy list")
try:
    proxy_resp = requests.get(PROXY_LIST_URL, timeout=10)
    proxy_resp.raise_for_status()
    proxies_list = [line.split("://")[-1].strip() for line in proxy_resp.text.splitlines() if line.strip()]
    status("Proxy Fetch", f"Retrieved {len(proxies_list)} raw proxies")
except Exception as e:
    print(f"[WARN] Could not fetch proxy list: {e}")
    sys.exit(1)

# --- Step 2: Validate proxies ---
status("Proxy Validation", "Validating proxies in parallel")
valid_proxies = validate_proxies(proxies_list)
status("Proxy Validation", f"{len(valid_proxies)} proxies passed validation")

if not valid_proxies:
    print("[ERROR] No working proxies found during validation.")
    sys.exit(1)

# --- Step 3: Generate timestamp and payload ---
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

# --- Step 4: Try CAD endpoint ---
status("CAD Request", "Attempting CAD endpoint with validated proxies")
resp = None
for proxy in valid_proxies:
    headers["User-Agent"] = random.choice(USER_AGENTS)
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    print(f"[INFO] Trying CAD with proxy: {proxy} and UA: {headers['User-Agent']}")
    try:
        resp = requests.post(CAD_URL, data=payload, headers=headers, proxies=proxies_dict, timeout=2)
        resp.raise_for_status()
        print(f"[INFO] Success with proxy: {proxy}")
        break
    except Exception as e:
        print(f"[WARN] CAD failed with proxy {proxy}: {e}")
        resp = None

if resp is None:
    print("[ERROR] All validated proxies failed to reach CAD endpoint.")
    sys.exit(1)

status("CAD Request", "Successfully retrieved CAD data")

# --- Step 5: Process data ---
data = resp.json()
rows = data.get("rows", [])
status("Data Processing", f"Retrieved {len(rows)} CAD rows")

if not rows:
    print("[ERROR] No records retrieved. Exiting with failure code.")
    sys.exit(1)

# --- Step 6: Connect to SQL Server ---
status("SQL Connection", "Connecting to SQL Server")
conn_str = (
    f"DRIVER={MSSQL_DRIVER};"
    f"SERVER={MSSQL_SERVER};"
    f"DATABASE={MSSQL_DATABASE};"
    f"UID={MSSQL_USERNAME};"
    f"PWD={MSSQL_PASSWORD};"
    "TrustServerCertificate=yes;"
)
conn = pyodbc.connect(conn_str)
cursor = conn.cursor()

# --- Step 7: Insert into DB ---
status("SQL Insert", "Inserting new records into database")
insert_sql = """
INSERT INTO dbo.CadHandler (
    invid, starttime, closetime, id, agency, service, nature, address,
    geox, geoy, geog, marker_details_xml, rec_key, icon_url, icon
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
"""

skipped_records = []

for r in rows:
    try:
        rec_id = int(r.get("id")) if r.get("id") else None
    except ValueError:
        continue
    if rec_id is None:
        continue

    cursor.execute("SELECT 1 FROM dbo.CadHandler WHERE id = ?", rec_id)
    if cursor.fetchone():
        skipped_records.append(r)
        continue

    cursor.execute(insert_sql, (
        int(r.get("invid")) if r.get("invid") else None,
        r.get("starttime"),
        r.get("closetime"),
        rec_id,
        r.get("agency"),
        r.get("service"),
        r.get("nature"),
        r.get("address"),
        float(r.get("geox")) if r.get("geox") else None,
        float(r.get("geoy")) if r.get("geoy") else None,
        r.get("marker_details_xml"),
        r.get("rec_key"),
        r.get("icon_url"),
        r.get("icon")
    ))

conn.commit()
cursor.close()
conn.close()

status("SQL Insert", f"Inserted {len(rows) - len(skipped_records)} new records")
status("SQL Insert", f"Skipped {len(skipped_records)} duplicates")


