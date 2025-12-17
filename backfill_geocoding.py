import requests
import json
import time
import pyodbc
import os
import re

# Configuration
PROXY_GEOCODE_URL = "http://localhost:9000/geocode"

def get_db_connection():
    """Establishes a database connection using settings from .env-db."""
    # Path to .env-db (one directory up from scripts/)
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env-db')
    
    conn_str_raw = ""
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                if line.startswith("ConnectionStrings__Default="):
                    conn_str_raw = line.strip().split("=", 1)[1]
                    break
    
    if not conn_str_raw:
        raise Exception("Could not find ConnectionStrings__Default in .env-db")

    # Parse connection string
    parts = [p for p in conn_str_raw.split(';') if p]
    params = {}
    for p in parts:
        if '=' in p:
            k, v = p.split('=', 1)
            params[k.strip()] = v.strip()

    # Build ODBC connection string
    driver = "{ODBC Driver 18 for SQL Server}"
    server = params.get('Server')
    database = params.get('Database')
    uid = params.get('User Id')
    pwd = params.get('Password')
    trust_cert = params.get('TrustServerCertificate', 'no')
    if trust_cert.lower() == 'true':
        trust_cert = 'yes'
    
    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={uid};PWD={pwd};TrustServerCertificate={trust_cert};"
    
    return pyodbc.connect(conn_str)

def execute_sql(sql):
    """Executes SQL directly against the database."""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Simple check to see if we should fetch results
        is_select = sql.strip().upper().startswith("SELECT")
        
        cursor.execute(sql)
        
        if is_select:
            columns = [column[0] for column in cursor.description]
            results = []
            for row in cursor.fetchall():
                results.append(dict(zip(columns, row)))
            return {'data': results}
        else:
            conn.commit()
            return {'data': []}
            
    except Exception as e:
        print(f"Exception executing SQL: {e}")
        return None

def add_columns(table):
    """Adds lat and lon columns if they don't exist."""
    print(f"Checking columns for {table}...")
    # Check if columns exist
    check_sql = f"""
    SELECT COLUMN_NAME 
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_NAME = '{table}' AND COLUMN_NAME IN ('lat', 'lon')
    """
    result = execute_sql(check_sql)
    if result and len(result.get('data', [])) == 2:
        print(f"Columns lat and lon already exist in {table}.")
        return

    print(f"Adding columns to {table}...")
    alter_sql = f"ALTER TABLE {table} ADD lat FLOAT, lon FLOAT;"
    execute_sql(alter_sql)
    print(f"Columns added to {table}.")

def clean_address(address):
    """Cleans address string to improve geocoding success."""
    if not address:
        return ""
    
    # Check for coordinates in the address string (e.g. "487200 -90.6267955")
    # This regex looks for a float-like number, space/comma, float-like number
# --- Enhanced Cleaning & Geocoding Logic ---

PLACE_MAPPING = {
    "DLEC": "770 IOWA ST, DUBUQUE, IA",
    "DUBUQUE LAW ENFORCEMENT CENTER": "770 IOWA ST, DUBUQUE, IA",
    "MERCY HOSPITAL": "250 MERCY DR, DUBUQUE, IA",
    "FINLEY HOSPITAL": "350 N GRANDVIEW AVE, DUBUQUE, IA",
    "CLARKE UNIVERSITY": "1550 CLARKE DR, DUBUQUE, IA",
    "LORAS COLLEGE": "1450 ALTA VISTA ST, DUBUQUE, IA",
    "UNIVERSITY OF DUBUQUE": "2000 UNIVERSITY AVE, DUBUQUE, IA",
    "Q CASINO": "1855 SCHMITT ISLAND RD, DUBUQUE, IA",
    "DIAMOND JO": "301 BELL ST, DUBUQUE, IA",
    "KENNEDY MALL": "555 JFK RD, DUBUQUE, IA",
    "WALMART": "4200 DODGE ST, DUBUQUE, IA", # Default to Dodge
    "CRAL AVE": "CENTRAL AVE, DUBUQUE, IA", # Typo fix
}

def clean_address(address):
    if not address:
        return ""
    
    cleaned = address.upper().strip()
    
    # Pre-cleaning replacements
    cleaned = cleaned.replace("CRAL AVE", "CENTRAL AVE")
    cleaned = cleaned.replace("NW ARTERIAL", "NORTHWEST ARTERIAL")
    cleaned = cleaned.replace("SW ARTERIAL", "SOUTHWEST ARTERIAL")
    cleaned = cleaned.replace("52 S", "US HWY 52 S")
    cleaned = cleaned.replace("52 N", "US HWY 52 N")
    
    # 1. Check Place Mapping
    if cleaned in PLACE_MAPPING:
        return PLACE_MAPPING[cleaned]

    # 2. Handle Narrative Text (e.g., " on X at Y", " at X")
    cleaned = cleaned.replace("<UNKNOWN STREET>", "").replace("&LT;UNKNOWN STREET&GT;", "")

    # Check for original city context (e.g. Peosta) to preserve it
    original_city = None
    if "PEOSTA" in cleaned: original_city = "PEOSTA"
    elif "FARLEY" in cleaned: original_city = "FARLEY"
    elif "EPWORTH" in cleaned: original_city = "EPWORTH"
    elif "DYERSVILLE" in cleaned: original_city = "DYERSVILLE"
    elif "CASCADE" in cleaned: original_city = "CASCADE"
    elif "ASBURY" in cleaned: original_city = "ASBURY"

    # Regex for " at [Address]"
    # Improved regex to capture potential city parts if present
    match_at = re.search(r'(?:^|\s+)AT\s+(.+?)(?:$|\.|,)', cleaned)
    if match_at:
        potential = match_at.group(1).strip()
        if potential and (potential[0].isdigit() or any(x in potential for x in [' ST', ' AVE', ' RD', ' DR', ' LN', ' CT', ' PKWY', ' CIR', ' PL', ' HWY'])):
            cleaned = potential

    # Regex for " on [Street] at [Cross Street]"
    match_on_at = re.search(r'(?:^|\s+)ON\s+(.+?)\s+AT\s+(.+?)(?:$|\.|,)', cleaned)
    if match_on_at:
        street1 = match_on_at.group(1).strip()
        street2 = match_on_at.group(2).strip()
        if street1 and street2:
            cleaned = f"{street1} & {street2}"
    
    # Regex for " on [Street]"
    match_on = re.search(r'(?:^|\s+)ON\s+(.+?)(?:$|\.|,)', cleaned)
    if match_on and " & " not in cleaned:
        potential = match_on.group(1).strip()
        if potential:
            cleaned = potential

    # 3. Handle Block Numbers
    cleaned = re.sub(r'(\d+)-BLK', r'\1', cleaned)

    # 4. Handle Intersections
    if "/" in cleaned:
        parts = cleaned.split("/")
        first_part = parts[0].strip()
        if first_part and first_part[0].isdigit():
             cleaned = first_part
        else:
             cleaned = cleaned.replace("/", " & ").replace(" AND ", " & ")

    # 5. Remove Noise
    cleaned = cleaned.replace("EXIT/ENT", "")
    # Use regex for ENT to avoid corrupting words like CENTRAL
    cleaned = re.sub(r'\bENT\b', '', cleaned) # Entrance
    
    # 6. City/State Normalization
    # If we found a specific non-Dubuque city earlier, ensure it's used
    if original_city and original_city not in cleaned:
        if "," in cleaned: cleaned = cleaned.split(",")[0] # Strip existing city if any
        cleaned += f", {original_city}, IA"
    else:
        # Default to Dubuque if no city present
        if "," not in cleaned:
            cleaned += ", DUBUQUE, IA"
        elif cleaned.endswith(","):
            cleaned += " DUBUQUE, IA"
    
    if not cleaned.endswith(" IA") and not cleaned.endswith(" IOWA"):
        cleaned += ", IA"
    
    cleaned = " ".join(cleaned.split())
    cleaned = cleaned.strip().strip(",")

    return cleaned

def extract_coordinates(address):
    """Attempts to extract lat/lon from the address string."""
    import re
    matches = re.findall(r'-?\d+\.\d+', address)
    if len(matches) >= 2:
        v1 = float(matches[0])
        v2 = float(matches[1])
        
        lat, lon = None, None
        if 40 < v1 < 44 and -97 < v2 < -89:
            lat, lon = v1, v2
        elif 40 < v2 < 44 and -97 < v1 < -89:
            lat, lon = v2, v1
            
        if lat and lon:
            return lat, lon
            
    return None, None

def geocode_and_update(table, id_col, address_col, time_col, target_ids=None):
    """Reads rows with null lat/lon, geocodes, and updates them."""
    # print(f"Processing {table}...")
    
    where_clause_base = f"lat IS NULL AND {address_col} IS NOT NULL"
    
    if target_ids:
        # Construct ID list string
        safe_ids = []
        for x in target_ids:
            if isinstance(x, str):
                safe_ids.append(f"'{x}'")
            else:
                safe_ids.append(str(x))
        
        if not safe_ids:
            return # Nothing to do

        id_list_str = ",".join(safe_ids)
        where_clause_base += f" AND {id_col} IN ({id_list_str})"
    
    # Get total count to process
    count_sql = f"SELECT COUNT(*) as count FROM {table} WHERE {where_clause_base}"
    count_res = execute_sql(count_sql)
    total = count_res['data'][0]['count'] if count_res and count_res.get('data') else 0
    
    if not target_ids:
        print(f"Found {total} records to process in {table}.")

    if total == 0:
        return

    processed = 0
    batch_size = 50
    
    while True:
        # Fetch batch
        fetch_sql = f"SELECT TOP {batch_size} {id_col}, {address_col} FROM {table} WHERE {where_clause_base} ORDER BY {time_col} DESC"
        result = execute_sql(fetch_sql)
        
        if not result or not result.get('data'):
            break
            
        rows = result['data']
        if not rows:
            break
            
        for row in rows:
            record_id = row[id_col]
            raw_address = row[address_col]
            
            if not raw_address:
                continue

            # 0. Check for direct coordinates in the string
            lat, lon = extract_coordinates(raw_address)
            
            if not lat:
                # Normal Geocoding Flow
                address = clean_address(raw_address)
                
                # Skip known bad
                if "PBX" in address or "UNKNOWN" in address:
                    print(f"Skipping known bad: {address}")
                    update_sql = f"UPDATE {table} SET lat = 0, lon = 0 WHERE {id_col} = {record_id}"
                    execute_sql(update_sql)
                    continue

                def fetch_coords(query):
                    for attempt in range(2): # Reduced retries for speed
                        try:
                            r = requests.get(PROXY_GEOCODE_URL, params={'q': query}, timeout=3)
                            if r.status_code == 200:
                                d = r.json()
                                if d and 'lat' in d and 'lon' in d:
                                    return float(d['lat']), float(d['lon'])
                        except Exception:
                            time.sleep(0.1)
                        time.sleep(0.1)
                    return None, None

                # 1. Try cleaned address
                lat, lon = fetch_coords(address)

                # 2. Fallback: Intersection Split
                if (lat is None) and " & " in address:
                    parts = address.split(" & ")
                    # Extract city suffix
                    city_suffix = ", DUBUQUE, IA"
                    if "," in parts[-1]:
                        city_suffix = parts[-1][parts[-1].find(","):]

                    valid_coords = []
                    for part in parts:
                        part = part.strip()
                        if not part: continue
                        query = part if "," in part else part + city_suffix
                        plat, plon = fetch_coords(query)
                        if plat: valid_coords.append((plat, plon))
                    
                    if valid_coords:
                        lat = sum(c[0] for c in valid_coords) / len(valid_coords)
                        lon = sum(c[1] for c in valid_coords) / len(valid_coords)
                        print(f"  -> Resolved intersection: {lat}, {lon}")

                # 3. Fallback: Street Only (with City) - Logic: Street w/o house number
                if (lat is None) and address[0].isdigit():
                    parts = address.split(" ", 1)
                    if len(parts) > 1:
                        # parts[1] includes city "JACKSON ST, DUBUQUE, IA"
                        street_with_city = parts[1]
                        print(f"  -> Trying street fallback (w/ City): {street_with_city}")
                        lat, lon = fetch_coords(street_with_city)
                
                # 4. Fallback: Bare Street (No City) - Critical for local Nominatim quirk
                if (lat is None):
                    # Try stripping city info
                    bare_addr = address.split(',')[0].strip()
                    # Also strip house number if present
                    if bare_addr[0].isdigit() and " " in bare_addr:
                         bare_addr = bare_addr.split(" ", 1)[1]
                    
                    print(f"  -> Trying bare street fallback (no City): {bare_addr}")
                    lat, lon = fetch_coords(bare_addr)
                    if not lat and "NORTHWEST ARTERIAL" in address:
                         # Try specific alias
                         print(f"  -> Trying alias fallback: NW ARTERIAL")
                         lat, lon = fetch_coords("NW ARTERIAL")

                # 5. Fallback: County Search (if City failed)
                if (lat is None) and "DUBUQUE" in address:
                    # Try replacing city with county
                    county_addr = address.replace("DUBUQUE", "DUBUQUE COUNTY")
                    print(f"  -> Trying county fallback: {county_addr}")
                    lat, lon = fetch_coords(county_addr)

            if lat is not None and lon is not None:
                # Update DB
                id_val = f"'{record_id}'" if isinstance(record_id, str) else record_id
                update_sql = f"UPDATE {table} SET lat = {lat}, lon = {lon} WHERE {id_col} = {id_val}"
                execute_sql(update_sql)
                print(f"Updated {table} {record_id}: {lat}, {lon}")
            else:
                print(f"No geocode for {raw_address} (cleaned: {address})")
                # Mark as failed (0,0) to prevent re-processing loop
                id_val = f"'{record_id}'" if isinstance(record_id, str) else record_id
                update_sql = f"UPDATE {table} SET lat = 0, lon = 0 WHERE {id_col} = {id_val}"
                execute_sql(update_sql)
            
            processed += 1
            time.sleep(0.05)
            
        print(f"Processed {processed}/{total}...")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', help='Specific table to process (cadHandler or DailyBulletinArrests)')
    args = parser.parse_args()

    # 1. Add columns
    add_columns('cadHandler')
    add_columns('DailyBulletinArrests')

    # 2. Process based on argument
    if args.table == 'cadHandler' or not args.table:
        # PK is 'id' (bigint), address column is 'address', time is 'starttime'
        geocode_and_update('cadHandler', 'id', 'address', 'starttime')

    if args.table == 'DailyBulletinArrests' or not args.table:
        # PK is 'id' (nvarchar), address column is 'location', time is 'event_time'
        geocode_and_update('DailyBulletinArrests', 'id', 'location', 'event_time')

if __name__ == "__main__":
    main()
