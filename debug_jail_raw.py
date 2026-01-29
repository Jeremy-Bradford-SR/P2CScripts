import requests
import time
import json

BASE_URL = "http://p2c.cityofdubuque.org"
JAIL_PAGE_URL = f"{BASE_URL}/jailinmates.aspx"

def fetch_raw_data():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
    })

    print("Initializing session...")
    try:
        resp = session.get(f"{BASE_URL}/main.aspx", timeout=15)
        resp = session.get(JAIL_PAGE_URL, timeout=15)
        
        jq_url = f"{BASE_URL}/jqHandler.ashx?op=s"
        jq_payload = {
            "t": "ii",
            "_search": "false",
            "nd": int(time.time() * 1000),
            "rows": 10, # Just get 10
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
        
        print("Fetching raw JSON...")
        resp = session.post(jq_url, data=jq_payload, headers=jq_headers, timeout=30)
        data = resp.json()
        
        rows = data.get('rows', [])
        if rows:
            print(f"Found {len(rows)} rows.")
            first_row = rows[0]
            print("\n--- KEYS IN RAW DATA ---")
            for k, v in first_row.items():
                print(f"{k}: {v}")
            
            # --- FETCH DETAIL PAGE ---
            print("\nAttempting to fetch detail page for first record...")
            
            # Extract ViewState from main page (initially fetched)
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, 'html.parser') # This is actually the JSON response text, wait. 
            # We need to fetch the initial page again or use the one from session.get(JAIL_PAGE_URL) earlier.
            # But we overwrote resp variable. Let's re-fetch JAIL_PAGE_URL properly.
            
            print("Refreshing Jail Page to get fresh ViewState...")
            page_resp = session.get(JAIL_PAGE_URL, timeout=15)
            soup = BeautifulSoup(page_resp.text, 'html.parser')
            
            viewstate = soup.find('input', {'name': '__VIEWSTATE'}).get('value', '')
            viewstategen = soup.find('input', {'name': '__VIEWSTATEGENERATOR'}).get('value', '')
            eventvalidation = soup.find('input', {'name': '__EVENTVALIDATION'}).get('value', '')
            
            # Trigger PostBack
            record_index = first_row.get('my_num')
            print(f"Triggering PostBack for index {record_index}...")
            
            post_data = {
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
            
            detail_resp = session.post(JAIL_PAGE_URL, data=post_data, headers=headers, allow_redirects=False)
            
            if detail_resp.status_code == 302:
                location = detail_resp.headers.get('Location')
                full_url = f"{BASE_URL}/{location}"
                print(f"Redirect found! Fetching detail: {full_url}")
                
                final_resp = session.get(full_url)
                
                # Save to file
                with open("jail_detail_dump.html", "w", encoding="utf-8") as f:
                    f.write(final_resp.text)
                print("Saved detailed HTML to 'jail_detail_dump.html'")
                
                # Also print image URL specifically
                detail_soup = BeautifulSoup(final_resp.text, 'html.parser')
                img = detail_soup.find('img', id='mainContent_CenterColumnContent_imgPhoto')
                if img:
                    print(f"FOUND IMAGE TAG: src='{img.get('src')}'")
                else:
                    print("NO IMAGE TAG FOUND.")
            else:
                print(f"PostBack failed. Status: {detail_resp.status_code}")
                # Save the failure page just in case
                with open("id_fail_dump.html", "w", encoding="utf-8") as f:
                    f.write(detail_resp.text)
        else:
            print("No rows found.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_raw_data()
