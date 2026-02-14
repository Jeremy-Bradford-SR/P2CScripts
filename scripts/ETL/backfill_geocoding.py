import requests
import json
import time

import os
import re

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

import shared_utils

# Configuration
PROXY_GEOCODE_URL = os.getenv("PROXY_GEOCODE_URL", "http://p2cproxy:9000/geocode")

def ensure_columns(table):
    """Ensures lat/lon columns exist via API."""
    print(f"Ensuring columns for {table}...")
    try:
        api = shared_utils.APIClient()
        api.post("tools/schema/ensure-geocode-columns", {"table": table})
        print(f"Columns ensured for {table}.")
    except Exception as e:
        print(f"Error ensuring columns: {e}")

def clean_address(address):
    """Cleans address string to improve geocoding success."""
    if not address:
        return ""
    
    cleaned = address.upper().strip()
    
    # Pre-cleaning replacements
    cleaned = cleaned.replace("CRAL AVE", "CENTRAL AVE")
    cleaned = cleaned.replace("NW ARTERIAL", "NORTHWEST ARTERIAL")
    cleaned = cleaned.replace("SW ARTERIAL", "SOUTHWEST ARTERIAL")
    cleaned = cleaned.replace("52 S", "US HWY 52 S")
    cleaned = cleaned.replace("52 N", "US HWY 52 N")
    
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
    """Reads rows with null lat/lon, geocodes, and updates them via API."""
    api = shared_utils.APIClient()
    
    # Pre-check or just loop
    total_processed = 0
    
    while True:
        candidates = []
        try:
            if target_ids:
                # Targeted Fetch
                candidates = api.post("tools/geocode/fetch-addresses", {"ids": [str(x) for x in target_ids], "table": table})
                # Only run once if targeted
                target_ids = None # Clear after first run check?
                # Actually, if we pass target_ids, we process them and break.
            else:
                # Batch Fetch
                candidates = api.get(f"tools/geocode/candidates?table={table}&count=50")
        except Exception as e:
            print(f"API Fetch Error: {e}")
            raise e
            
        if not candidates:
            break
            
        updates = []
        for row in candidates:
            record_id = row.get('id') or row.get('Id')
            raw_address = row.get('address') or row.get('Address')
            
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
                    updates.append({"Id": str(record_id), "Lat": 0.0, "Lon": 0.0, "Table": table})
                    continue

                def fetch_coords(query):
                    for attempt in range(2): # Reduced retries for speed
                        try:
                            # Use requests directly for proxy as it might be internal or direct
                            # Usually PROXY_GEOCODE_URL is external or internal service
                            # "http://localhost:9000/geocode" -> p2cproxy:9000
                            # If running in orchestrator, localhost:9000 IS available (if host net?)
                            # No, orchestrator is in container. localhost refers to itself.
                            # It should use "http://p2cproxy:9000/geocode"
                            # But wait, PROXY_GEOCODE_URL is defined at top.
                            # I'll rely on it being correct or update it.
                            # Assuming "http://p2cproxy:9000/geocode" for container networking.
                            # Or PROXY_GEOCODE_URL
                            
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

                # 3. Fallback: Street Only (with City)
                if (lat is None) and address[0].isdigit():
                    parts = address.split(" ", 1)
                    if len(parts) > 1:
                        street_with_city = parts[1]
                        # print(f"  -> Trying street fallback: {street_with_city}")
                        lat, lon = fetch_coords(street_with_city)
                
                # 4. Fallback: Bare Street
                if (lat is None):
                    bare_addr = address.split(',')[0].strip()
                    if bare_addr[0].isdigit() and " " in bare_addr:
                         bare_addr = bare_addr.split(" ", 1)[1]
                    # print(f"  -> Trying bare street: {bare_addr}")
                    lat, lon = fetch_coords(bare_addr)
                    if not lat and "NORTHWEST ARTERIAL" in address:
                         lat, lon = fetch_coords("NW ARTERIAL")

                # 5. Fallback: County
                if (lat is None) and "DUBUQUE" in address:
                    county_addr = address.replace("DUBUQUE", "DUBUQUE COUNTY")
                    # print(f"  -> Trying county: {county_addr}")
                    lat, lon = fetch_coords(county_addr)

            if lat is not None and lon is not None:
                updates.append({"Id": str(record_id), "Lat": lat, "Lon": lon, "Table": table})
                print(f"Geocoded {record_id}: {lat}, {lon}")
            else:
                print(f"Failed Geocode {record_id} ({raw_address}) -> Cleaned: {address}")
                updates.append({"Id": str(record_id), "Lat": 0.0, "Lon": 0.0, "Table": table})
            
            total_processed += 1
            time.sleep(0.05)
            
        # Send updates
        if updates:
            try:
                api.post("tools/geocode/update", updates)
            except Exception as e:
                print(f"Update Batch Error: {e}")
                raise e
        
        # If we were doing targeted, we are done
        if target_ids is None and not candidates: 
           break
        if candidates and target_ids is not None: 
           # We processed the target batch, so break
           break

    print(f"Processed {total_processed} records.")

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--table', help='Specific table to process (cadHandler or DailyBulletinArrests)')
    args = parser.parse_args()

    # 1. Add columns (Via API)
    ensure_columns('cadHandler')
    ensure_columns('DailyBulletinArrests')

    # 2. Process based on argument
    if args.table == 'cadHandler' or not args.table:
        # PK is 'id' (bigint), address column is 'address', time is 'starttime'
        geocode_and_update('cadHandler', 'id', 'address', 'starttime')

    if args.table == 'DailyBulletinArrests' or not args.table:
        # PK is 'id' (nvarchar), address column is 'location', time is 'event_time'
        geocode_and_update('DailyBulletinArrests', 'id', 'location', 'event_time')

if __name__ == "__main__":
    main()
