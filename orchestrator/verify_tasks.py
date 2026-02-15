
import requests
import json
import time

BASE_URL = "http://localhost:8005/api"

def test_tasks_api():
    print("1. Creating Task...")
    payload = {
        "job_id": 1, # Assuming job_id 1 exists (from setup_orchestrator_db)
        "name": "Integration Test Task",
        "interval_minutes": 5,
        "config": {"test": "true"},
        "enabled": True
    }
    
    try:
        res = requests.post(f"{BASE_URL}/tasks", json=payload)
        if res.status_code != 200:
            print(f"FAILED: {res.text}")
            return
        
        task_id = res.json()["task_id"]
        print(f"SUCCESS: Created Task ID {task_id}")
        
        print("2. Listing Tasks...")
        res = requests.get(f"{BASE_URL}/tasks")
        tasks = res.json()
        found = False
        for t in tasks:
            if t["task_id"] == task_id:
                found = True
                print(f"Found Task: {t['name']} (Next Run: {t['next_run']})")
                break
        
        if not found:
            print("FAILED: Task not found in list")
            return
            
        print("3. Updating Task...")
        update_payload = {"name": "Updated Test Task", "interval_minutes": 10}
        requests.put(f"{BASE_URL}/tasks/{task_id}", json=update_payload)
        
        res = requests.get(f"{BASE_URL}/tasks")
        tasks = res.json()
        for t in tasks:
            if t["task_id"] == task_id:
                if t["name"] == "Updated Test Task" and t["interval_minutes"] == 10:
                     print("SUCCESS: Task Updated")
                else:
                     print(f"FAILED: Update mismatch: {t}")
        
        print("4. Triggering Run...")
        res = requests.post(f"{BASE_URL}/tasks/{task_id}/run")
        if res.status_code == 200:
             print("SUCCESS: Run Triggered")
        else:
             print(f"FAILED: {res.text}")

        # Clean up
        print("5. Deleting Task...")
        requests.delete(f"{BASE_URL}/tasks/{task_id}")
        print("SUCCESS: Task Deleted")
        
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    test_tasks_api()
