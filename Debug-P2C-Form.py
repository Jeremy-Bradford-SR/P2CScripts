import requests
from bs4 import BeautifulSoup
import time
from datetime import datetime

# --- CONFIG ---
SESSION_INIT_URL = "http://p2c.cityofdubuque.org/main.aspx"
DAILY_BULLETIN_URL = "http://p2c.cityofdubuque.org/dailybulletin.aspx"
DATA_URL = "http://p2c.cityofdubuque.org/jqHandler.ashx?op=s"
TARGET_DATE = "11/15/2025" # A hardcoded date in the past to test with.

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"

def status(step, message):
    """Prints a timestamped status message."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{step}] {message}")

def run_debug():
    """
    Performs a single-threaded test of the form submission and saves the HTML responses.
    """
    session = requests.Session()
    session.headers.update({'User-Agent': USER_AGENT})

    try:
        # --- STEP 1: Get the initial form page ---
        status("Step 1", f"Getting initial form from {DAILY_BULLETIN_URL}")
        initial_resp = session.get(DAILY_BULLETIN_URL, timeout=20)
        initial_resp.raise_for_status()

        # Save the initial HTML for debugging
        with open("debug_step1_initial_form.html", "w", encoding="utf-8") as f:
            f.write(initial_resp.text)
        status("Step 1", "Saved initial HTML to 'debug_step1_initial_form.html'")
        
        soup = BeautifulSoup(initial_resp.text, 'html.parser')
        viewstate = soup.find('input', {'name': '__VIEWSTATE'}).get('value', '')
        viewstategen = soup.find('input', {'name': '__VIEWSTATEGENERATOR'}).get('value', '')
        eventvalidation = soup.find('input', {'name': '__EVENTVALIDATION'}).get('value', '')

        status("Step 1", f"Initial __VIEWSTATE found: {viewstate[:50]}...")

        # --- STEP 2: POST back to the form to set the date ---
        form_data = {
            '__EVENTTARGET': 'btnGet',
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': viewstategen,
            '__EVENTVALIDATION': eventvalidation,
            'txtDate': TARGET_DATE,
            'btnGet': 'Get'
        }
        
        status("Step 2", f"Posting form to set date to {TARGET_DATE}")
        post_headers = {'Referer': DAILY_BULLETIN_URL}
        set_date_resp = session.post(DAILY_BULLETIN_URL, data=form_data, headers=post_headers, timeout=20)
        set_date_resp.raise_for_status()

        # Save the HTML response AFTER the POST for debugging
        with open("debug_step2_after_date_set.html", "w", encoding="utf-8") as f:
            f.write(set_date_resp.text)
        status("Step 2", "Saved response HTML to 'debug_step2_after_date_set.html'")

        # Check if the viewstate changed
        new_soup = BeautifulSoup(set_date_resp.text, 'html.parser')
        new_viewstate = new_soup.find('input', {'name': '__VIEWSTATE'}).get('value', '')
        status("Step 2", f"New __VIEWSTATE received: {new_viewstate[:50]}...")
        if viewstate == new_viewstate:
            status("ANALYSIS", "WARNING: The __VIEWSTATE did not change after POST. The server likely ignored the date change.")
        else:
            status("ANALYSIS", "SUCCESS: The __VIEWSTATE changed, which is a good sign.")

        # --- STEP 3: Attempt to fetch data ---
        data_payload = { "t": "db", "d": TARGET_DATE, "_search": "false", "nd": int(time.time() * 1000), "rows": 10, "page": 1, "sidx": "case", "sord": "asc" }
        data_headers = { "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8", "Origin": "http://p2c.cityofdubuque.org", "Referer": "http://p2c.cityofdubuque.org/dailybulletin.aspx", "X-Requested-With": "XMLHttpRequest" }
        
        status("Step 3", f"Attempting to fetch data for {TARGET_DATE}")
        data_resp = session.post(DATA_URL, data=data_payload, headers=data_headers, timeout=20)
        data_resp.raise_for_status()
        
        print("\n--- DATA RESPONSE ---")
        print(data_resp.text)
        print("---------------------\n")

    except Exception as e:
        status("ERROR", f"The debug script failed: {e}")

if __name__ == "__main__":
    run_debug()

