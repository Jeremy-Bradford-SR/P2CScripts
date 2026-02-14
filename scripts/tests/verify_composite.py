import sys
import os
import random
import time

# Override API URL for local test running on host
os.environ["API_BASE_URL"] = "http://localhost:8083/api"
os.environ["API_KEY"] = "SuperSecretKey123!"

# Add P2CScripts root to path
# P2CScripts/scripts/tests/verify_composite.py -> ... -> P2CScripts/
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
from shared_utils import APIClient, status

def run_test():
    api = APIClient()
    
    # Generate a random ID base to avoid conflicts with real data
    run_id = f"TEST-{int(time.time())}-{random.randint(1000,9999)}"
    status("Test", f"Starting verification with Run ID: {run_id}")
    
    # Payload: 
    # 1. Unique Record A (AR)
    # 2. Unique Record B (TC) - SAME ID as A, different KEY
    # 3. Duplicate Record A (AR) - SAME ID and ID as A -> Should be skipped
    
    batch = [
        {
            "id": run_id, 
            "key": "AR", 
            "name": "Test Person A", 
            "time": "12:00", 
            "description": "Arrest Test",
            "invid": "1001",
            "location": "Test Loc",
            "crime": "Test Crime",
            "property": "Test Prop",
            "officer": "Test Officer",
            "case": "Test Case 1",
            "race": "W",
            "sex": "M",
            "lastname": "Person",
            "firstname": "Test",
            "charge": "Test Charge",
            "middlename": "A"
        },
        {
            "id": run_id, 
            "key": "TC", 
            "name": "Test Person A", 
            "time": "12:05", 
            "description": "Citation Test",
            "invid": "1002",
             "location": "Test Loc",
            "crime": "Test Crime",
            "property": "Test Prop",
            "officer": "Test Officer",
            "case": "Test Case 2",
            "race": "W",
            "sex": "M",
            "lastname": "Person",
            "firstname": "Test",
            "charge": "Test Charge",
            "middlename": "A"
        },
        {
            "id": run_id, 
            "key": "AR", 
            "name": "Test Person A", 
            "time": "12:00", 
            "description": "Arrest Test Duplicate",
            "invid": "1001",
            "location": "Test Loc",
            "crime": "Test Crime",
            "property": "Test Prop",
            "officer": "Test Officer",
            "case": "Test Case 1",
            "race": "W",
            "sex": "M",
            "lastname": "Person",
            "firstname": "Test",
            "charge": "Test Charge",
            "middlename": "A"
        }
    ]
    
    status("Test", "Sending batch...")
    try:
        response = api.post_ingestion("daily-bulletin/batch", batch)
        status("Test", f"Response: {response}")
        
        inserted = response.get('inserted', 0)
        skipped = response.get('skipped', 0)
        
        # Expect inserted=2, skipped=1
        if inserted == 2 and skipped == 1:
            status("Test", "SUCCESS: Correctly handled composite keys and duplicates.")
            print("VERIFICATION_SUCCESS")
        else:
            status("Test", f"FAILURE: Expected inserted=2, skipped=1. Got inserted={inserted}, skipped={skipped}")
            print("VERIFICATION_FAILURE")
            
    except Exception as e:
        status("Test", f"Exception during test: {e}")
        if hasattr(e, 'response') and e.response is not None:
             status("Test", f"Response Body: {e.response.text}")
        print("VERIFICATION_FAILURE")

if __name__ == "__main__":
    run_test()
