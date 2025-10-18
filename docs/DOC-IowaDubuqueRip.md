# DOC-IowaDubuqueRip.py

Purpose
- Scrapes Iowa DOC offender search endpoints to collect offender list and detail pages. Inserts offender records and charges into MSSQL tables.

How the endpoint works
- The search flow uses a public site at `https://doc-search.iowa.gov`.
- Primary endpoints used:
  - LIST AJAX: `https://doc-search.iowa.gov/api/offender/GetOffenderListAjax` — expects form-urlencoded POST used to fetch a page of offenders. Uses server-side paging, ordering, and a `searchModel` object in form keys (flattened into `searchModel.FirsName`, etc.).
  - DETAIL page: `https://doc-search.iowa.gov/offender/detail?offenderNumber={OFFENDER_NUM}` — returns an HTML detail page with charge and demographic info.

Request mechanics
- Uses POST with headers:
  - Content-Type: `application/x-www-form-urlencoded; charset=UTF-8`
  - X-Requested-With: `XMLHttpRequest`
  - Origin: `https://doc-search.iowa.gov`
- The script assembles `LIST_BASE_DATA` covering DataTables-style keys: `draw`, `columns[*]`, `order[0][column]`, `length`, `start`, and flattened searchModel fields.
- Uses proxies (fetched from a free proxy list) and random User-Agent rotation to avoid blocking.

Parsing
- The list endpoint returns JSON rows with fields like `Name`, `OffenderNumber`, `Age`, `Gender`.
- The detail page is HTML parsed with BeautifulSoup; dates are normalized via `parse_date_string()` to Python `date` objects.

ETL Behavior
- Extract: Poll the list API pages until no more results.
- Transform: Normalize names, parse and cast dates, extract charges into separate `charges` records.
- Load: Uses `pyodbc` to write to two tables (assumed names: `Offender` and `OffenderCharges` or similar). Batch inserts are performed with fallback to single inserts on failure.

Database schema (assumed based on insertion code)
- `dbo.Offender` (example columns and types):
  - id (INT or BIGINT) — primary key
  - offender_number (VARCHAR) — offender identifier from site
  - name (NVARCHAR)
  - age (INT)
  - gender (CHAR(1))
  - birth_date (DATE)
  - created_at (DATETIME)

- `dbo.OffenderCharge` (example columns and types):
  - id (INT or BIGINT)
  - offender_id (INT) — FK to Offender.id
  - charge_text (NVARCHAR(MAX))
  - charge_date (DATE)

Notes and edge cases
- The site might throttle or block repeated requests; proxies help but are unreliable.
- The script attempts to manage proxy rotation and multithreading; ensure MSSQL timeouts and transactions are tuned for batch sizes.

How to run
- Configure MSSQL_* constants at top of script.
- Install Python deps: `requests`, `beautifulsoup4`, `pyodbc`.
- Run: `python3 DOC-IowaDubuqueRip.py`
