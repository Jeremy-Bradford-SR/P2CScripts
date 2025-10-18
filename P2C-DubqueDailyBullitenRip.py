import requests
import random
import time
import pyodbc
import sys
import concurrent.futures
from datetime import datetime

# --- CONFIG ---
MSSQL_SERVER   = "192.168.0.43"
MSSQL_DATABASE = "p2cdubuque"
MSSQL_USERNAME = "sa"
MSSQL_PASSWORD = "Thugitout09!"
MSSQL_DRIVER   = "{ODBC Driver 18 for SQL Server}"

PROXY_LIST_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/http/data.txt"
DAILY_BULLETIN_URL = "http://p2c.cityofdubuque.org/dailybulletin.aspx"
DATA_URL = "http://p2c.cityofdubuque.org/jqHandler.ashx?op=s"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2)...Chrome/117.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0)...Firefox/118.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2; rv:118.0)...Firefox/118.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)...Edg/117.0.2045.43",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2)...Safari/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_6)...Mobile Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 6)...Chrome/117.0.0.0 Mobile Safari/537.36"
]

def status(step, message):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [STATUS] {step}: {message}")

def check_proxy(proxy):
    test_url = "http://example.com"
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    try:
        requests.get(test_url, proxies=proxies_dict, timeout=2)
        return proxy
    except:
        return None

def validate_proxies(proxies_list, batch_size=20):
    random.shuffle(proxies_list)
    valid_proxies = []
    for i in range(0, min(len(proxies_list), 100), batch_size):
        batch = proxies_list[i:i + batch_size]
        with concurrent.futures.ThreadPoolExecutor(max_workers=batch_size) as executor:
            results = list(executor.map(check_proxy, batch))
        valid_batch = [proxy for proxy in results if proxy]
        valid_proxies.extend(valid_batch)
        if valid_proxies:
            break
    return valid_proxies

# --- Step 1: Fetch proxy list ---
status("Proxy Fetch", "Fetching proxy list")
try:
    proxy_resp = requests.get(PROXY_LIST_URL, timeout=10)
    proxy_resp.raise_for_status()
    proxies_list = [line.split("://")[-1].split(":")[0].strip() for line in proxy_resp.text.splitlines() if line.strip()]
    status("Proxy Fetch", f"Retrieved {len(proxies_list)} proxies")
except Exception as e:
    print(f"[ERROR] Could not fetch proxy list: {e}")
    sys.exit(1)

# --- Step 2: Validate proxies ---
status("Proxy Validation", "Validating proxies in parallel")
valid_proxies = validate_proxies(proxies_list)
status("Proxy Validation", f"{len(valid_proxies)} proxies passed validation")

if not valid_proxies:
    print("[ERROR] No working proxies found during validation.")
    sys.exit(1)

# --- Step 3: Get ASP.NET_SessionId ---
status("Session Retrieval", "Trying proxies for ASP.NET_SessionId")
session_id = None
for proxy in valid_proxies:
    headers = {"User-Agent": random.choice(USER_AGENTS)}
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    try:
        resp = requests.get(DAILY_BULLETIN_URL, headers=headers, proxies=proxies_dict, timeout=2)
        if "ASP.NET_SessionId" in resp.cookies:
            session_id = resp.cookies.get("ASP.NET_SessionId")
            status("Session Retrieval", f"Got ASP.NET_SessionId via proxy: {proxy}")
            break
    except Exception as e:
        print(f"[WARN] Proxy {proxy} failed for session: {e}")

if not session_id:
    status("Session Retrieval", "All proxies failed, trying direct")
    resp = requests.get(DAILY_BULLETIN_URL, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=10)
    if "ASP.NET_SessionId" in resp.cookies:
        session_id = resp.cookies.get("ASP.NET_SessionId")
        status("Session Retrieval", "Got ASP.NET_SessionId direct")
    else:
        print("[ERROR] Could not retrieve ASP.NET_SessionId")
        sys.exit(1)

# --- Step 4: Prepare payload ---
nd_value = int(time.time() * 1000) + random.randint(0, 999)
status("Payload", f"Using nd={nd_value}")

payload = {
    "t": "db",
    "_search": "false",
    "nd": nd_value,
    "rows": 50,
    "page": 1,
    "sidx": "case",
    "sord": "asc"
}

headers = {
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "Origin": "http://p2c.cityofdubuque.org",
    "Referer": "http://p2c.cityofdubuque.org/dailybulletin.aspx",
    "X-Requested-With": "XMLHttpRequest"
}

cookies = {"ASP.NET_SessionId": session_id}

# --- Step 5: Try proxies for data ---
status("Data Request", "Attempting data fetch via proxies")
data_resp = None
for proxy in valid_proxies:
    headers["User-Agent"] = random.choice(USER_AGENTS)
    proxies_dict = {"http": f"http://{proxy}", "https": f"http://{proxy}"}
    try:
        r = requests.post(DATA_URL, data=payload, headers=headers, cookies=cookies, proxies=proxies_dict, timeout=2)
        r.raise_for_status()
        data_resp = r
        status("Data Request", f"Success with proxy: {proxy}")
        break
    except Exception as e:
        print(f"[WARN] Proxy {proxy} failed for data: {e}")

if data_resp is None:
    status("Data Request", "All proxies failed, trying direct")
    headers["User-Agent"] = random.choice(USER_AGENTS)
    r = requests.post(DATA_URL, data=payload, headers=headers, cookies=cookies, timeout=10)
    r.raise_for_status()
    data_resp = r

# --- Step 6: Process JSON ---
data = data_resp.json()
rows = data.get("rows", [])
status("Data Processing", f"Retrieved {len(rows)} rows")

if not rows:
    print("[ERROR] No rows retrieved. Exiting with failure code.")
    sys.exit(1)

print("\n[INFO] All retrieved records:")
for r in rows:
    print(f"ID={r.get('id')}, Name={r.get('name')}, Crime={r.get('crime')}, Case={r.get('case')}, Time={r.get('time')}")

# --- Step 7: Connect to SQL Server ---
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

# --- Step 8: Insert into DB ---
status("SQL Insert", "Inserting new records into database")
insert_sql = """
INSERT INTO dbo.DailyBulletinArrests (
    invid, [key], location, id, name, crime, [time], property,
    officer, [case], description, race, sex, lastname,
    firstname, charge, middlename
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

MAX_BIGINT = 9223372036854775807
MIN_BIGINT = -9223372036854775808
skipped = []
inserted = []

for r in rows:
    try:
        rec_id = int(r.get("id")) if r.get("id") else None
    except (ValueError, TypeError):
        continue

    if rec_id is None or not (MIN_BIGINT <= rec_id <= MAX_BIGINT):
        continue

    try:
        cursor.execute("SELECT 1 FROM dbo.DailyBulletinArrests WHERE id = ?", rec_id)
        if cursor.fetchone():
            skipped.append(r)
            continue
    except OverflowError:
        print(f"[WARN] OverflowError on rec_id: {rec_id}")
        continue

    try:
        cursor.execute(insert_sql, (
            r.get("invid"),
            r.get("key"),
            r.get("location"),
            rec_id,
            r.get("name"),
            r.get("crime"),
            r.get("time"),
            r.get("property"),
            r.get("officer"),
            r.get("case"),
            r.get("description"),
            r.get("race"),
            r.get("sex"),
            r.get("lastname"),
            r.get("firstname"),
            r.get("charge"),
            r.get("middlename")
        ))
        inserted.append(r)
    except pyodbc.Error as db_err:
        print(f"[ERROR] Insert failed for ID {rec_id}: {db_err}")

conn.commit()
cursor.close()
conn.close()

# --- Final status and reporting ---
status("SQL Insert", f"Inserted {len(inserted)} records, skipped {len(skipped)} duplicates")

# --- Show inserted records ---
print("\n[INFO] Inserted records:")
for r in inserted:
    print(f"ID={r.get('id')}, Name={r.get('name')}, Crime={r.get('crime')}, Case={r.get('case')}, Time={r.get('time')}")

# --- Show skipped records ---
if skipped:
    print(f"\n[INFO] Skipped {len(skipped)} duplicate records:")
    for r in skipped:
        print(f"ID={r.get('id')}, Name={r.get('name')}, Crime={r.get('crime')}, Case={r.get('case')}, Time={r.get('time')}")
else:
    print("\n[INFO] No duplicates were skipped.")

status("Complete", "Daily Bulletin import finished successfully")