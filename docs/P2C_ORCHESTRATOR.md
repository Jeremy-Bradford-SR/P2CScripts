# P2C Stack Orchestrator Documentation

## System Overview
The **P2C Orchestrator** is a centralized management system for scraping and ingesting data from P2C (Police to Citizen) and DOC (Department of Corrections) sources. It manages:
- **Scheduling**: Corn-job style execution of Python scrapers.
- **Proxy Management**: Automatic validation, rotation, and injection of proxies into scripts.
- **Reliability**: Infinite retry loops, backoff strategies, and fallback logic.
- **Observability**: Real-time log streaming and historic run tracking.

## 2026 Reliability & Refactor Updates
### 1. Infinite Persistence (No "Max Retries")
We have removed fixed "Max Retry" limits from all ingestion scripts.
- **Old Behavior**: Scripts would retry 3-5 times and then exit with failure.
- **New Behavior**: Scripts loop **indefinitely** until they succeed or a fatal errors occurs (e.g. forced kill).
- **Backoff**: Scripts sleep for 1-5 seconds between failures to prevent server spamming.

### 2. Project Directory Structure
Scripts have been refactored into specialized directories for better organization:
- `scripts/ingestion/`: Core scrapers (e.g. `P2C-DubqueRecentCallsRip.py`).
- `scripts/ETL/`: Post-processing helpers (e.g. `backfill_geocoding.py`).
- `orchestrator/`: The FastAPI backend and React frontend.

### 3. Proxy "Smart Pool"
The proxy manager now actively maintains a target of **100 valid proxies**.
- It pauses validation when the pool is full.
- It automatically resumes refreshing when the count drops.
- **Strict Validation**: Proxies must pass a check against `iowasexoffender.gov` to be considered valid.

---

## API Reference
The Orchestrator provides a REST API via FastAPI (Port 8005 by default).

### Jobs & Execution
#### `GET /api/jobs`
Returns a list of all registered jobs.
- **Response**: `[{ "job_id": 1, "name": "P2C-DubqueRecentCallsRip", "enabled": 1, ... }]`

#### `POST /api/jobs/{job_id}/run`
Triggers an immediate run of a specific job.
- **Payload** (Optional):
  ```json
  {
    "config": {
      "days": 5,
      "workers": 10
    }
  }
  ```
- **Response**: `{"status": "Job started", "job_id": 1}`

#### `GET /api/history`
Returns execution history (Success/Failure status).
- **Query Params**: `?limit=50`

#### `GET /api/logs/{run_id}`
Returns console logs for a specific execution run.

### Proxy Management
#### `GET /api/proxies/status`
Returns current proxy pool statistics.
- **Response**:
  ```json
  {
    "total": 102,
    "validating": false,
    "sources": ["spys.me", "proxyscrape"]
  }
  ```

#### `POST /api/proxies/refresh`
Forces a refresh of the proxy list from external sources.

#### `POST /api/proxies/config`
Updates proxy manager settings.
- **Payload**:
  ```json
  {
    "target_count": 100,
    "validation_url": "https://..."
  }
  ```

---

## Script Development Guide
### 1. Config Injection
All scripts must accept a `--config` argument containing a JSON string.
```python
# Standard Boilerplate
import argparse
import json
import shared_utils

parser = argparse.ArgumentParser()
parser.add_argument("--config", type=str, default="{}")
args = parser.parse_args()
config = json.loads(args.config)
```

### 2. Shared Utilities
Properly import `shared_utils` by adding the root directory to `sys.path`:
```python
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
import shared_utils
```

### 3. Infinite Retry Pattern
Do not use fixed counters. Use `while True:`
```python
while True:
    session, proxy = shared_utils.get_session(proxies)
    try:
        resp = session.get(...)
        break # Success
    except Exception:
        time.sleep(2) 
        # Loop continues
```
