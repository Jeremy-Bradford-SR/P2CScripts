import requests
import sys
import json

BASE_URL = "http://localhost:8005"

def test_endpoint(name, url):
    print(f"Testing {name} ({url})...", end=" ")
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            print("OK")
            return resp.json()
        else:
            print(f"FAILED ({resp.status_code})")
            print(resp.text)
            return None
    except Exception as e:
        print(f"ERROR: {e}")
        return None

def main():
    print("=== API SMOKE TESTS ===")
    
    # 1. Jobs
    jobs = test_endpoint("Jobs", f"{BASE_URL}/api/jobs")
    if jobs is None: sys.exit(1)
    print(f"  Found {len(jobs)} jobs.")

    # 2. History
    history = test_endpoint("History", f"{BASE_URL}/api/history")
    if history is None: sys.exit(1)
    print(f"  Found {len(history)} history records.")

    # 3. Logs (if history exists)
    if history:
        latest_run = history[0]
        run_id = latest_run.get('run_id')
        print(f"  Testing logs for Run ID {run_id}...")
        logs = test_endpoint("Logs", f"{BASE_URL}/api/logs/{run_id}")
        if logs is None: sys.exit(1)
        print(f"  Found {len(logs)} log entries.")
    else:
        print("  Skipping Logs test (no history).")

    # 4. Proxies
    print("\nTesting Proxies...")
    # Status
    status = test_endpoint("Proxy Status", f"{BASE_URL}/api/proxies/status")
    if status is None: 
        print("  FAILED: Endpoint unreachable")
    else:
        print(f"  Status: Active={status.get('active_proxies')}, Raw={status.get('total_raw')}, Validating={status.get('is_validating')}")
        
    # Config Update
    print("Testing Config Update...", end=" ")
    try:
        new_conf = {"concurrency": 25}
        resp = requests.post(f"{BASE_URL}/api/proxies/config", json=new_conf, timeout=5)
        if resp.status_code == 200:
            if resp.json().get("config", {}).get("concurrency") == 25:
                print("OK")
            else:
                print("FAILED (Config mismatch)")
        else:
            print(f"FAILED ({resp.status_code})")
    except Exception as e:
        print(f"ERROR: {e}")

    print("\nALL TESTS PASSED")

if __name__ == "__main__":
    main()
