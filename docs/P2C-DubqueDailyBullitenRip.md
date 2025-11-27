# P2C-DubqueDailyBullitenRip.py

## Purpose
- Fetches the City of Dubuque "Daily Bulletin" dataset (arrests) via P2C's jqHandler endpoint and inserts rows into `DailyBulletinArrests` in MSSQL.

## Endpoint
- URL: `http://p2c.cityofdubuque.org/jqHandler.ashx?op=s`
- The endpoint expects a POST with form-encoded parameters resembling jqGrid/DataTables requests (t, _search, nd, rows, page, sidx, sord).
- The site uses ASP.NET session cookies (`ASP.NET_SessionId`) so the script first requests `dailybulletin.aspx` to obtain a session cookie. This may be done via proxies or direct connection.

## Request details
- Headers used:
  - Content-Type: `application/x-www-form-urlencoded; charset=UTF-8`
  - Origin and Referer set to `http://p2c.cityofdubuque.org`
  - X-Requested-With: `XMLHttpRequest`
  - User-Agent rotated from `USER_AGENTS` list
- Payload example:
  - t=db
  - _search=false
  - nd=<timestamp + random jitter>
  - rows=50
  - page=1
  - sidx=case
  - sord=asc

## Proxy behavior
- The script downloads a proxy list from `PROXY_LIST_URL` and validates a subset in parallel.
- **Improved Validation**: Proxies are now checked against `http://example.com` for general connectivity and speed.
- **Large Pool**: The script attempts to find up to 1000 working proxies to ensure a robust pool for long-running scrapes.
- Valid proxies are used to try getting the initial ASP.NET_SessionId and later to fetch the JSON data.

## Parsing and DB insertion
- The response is JSON with a `rows` array. Each row contains fields like `id`, `invid`, `name`, `crime`, `case`, `time` etc.
- The script protects against integer overflow when mapping `id` to DB types. It checks for duplicates via a `SELECT 1 FROM dbo.DailyBulletinArrests WHERE id = ?` before inserting.
- Insert SQL maps the response fields to columns in `dbo.DailyBulletinArrests`.

## Database schema (`dbo.DailyBulletinArrests`) — suggested types
- invid: BIGINT NULL
- [key]: NVARCHAR(100) NULL
- location: NVARCHAR(200) NULL
- id: BIGINT PRIMARY KEY
- name: NVARCHAR(200) NULL
- crime: NVARCHAR(200) NULL
- [time]: NVARCHAR(100) NULL -- original raw string
- property: NVARCHAR(200) NULL
- officer: NVARCHAR(100) NULL
- [case]: NVARCHAR(100) NULL
- description: NVARCHAR(MAX) NULL
- race: NVARCHAR(50) NULL
- sex: CHAR(1) NULL
- lastname, firstname, middlename: NVARCHAR(100)
- charge: NVARCHAR(200) NULL
- event_time: DATETIME NULL -- populated post-hoc by `UpdateDAB-TimetoEventTime.py`

## ETL notes
- **Session handling**: The script ensures session cookie presence. If proxies fail, it falls back to direct connection.
- **Retry Logic**: The `process_day` function includes a retry loop (e.g., 3 attempts) to handle transient failures or bad proxies.
- **De-duplication**: uses id as a unique key to avoid duplicates.

## Running
1.  Ensure `.env` file is configured with MSSQL credentials.
2.  Install dependencies: `pip install -r requirements.txt`.
3.  Run: `python3 P2C-DubqueDailyBullitenRip.py`
    - Optional arguments: `--DAYS_TO_SCRAPE`, `--MAX_WORKERS`, `--CHUNK_SIZE`.
