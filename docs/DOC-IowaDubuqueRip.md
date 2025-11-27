# DOC-IowaDubuqueRip.py

## Purpose
- Scrapes Iowa DOC offender search endpoints to collect offender list and detail pages. Inserts offender records and charges into MSSQL tables.

## How the endpoint works
- The search flow uses a public site at `https://doc-search.iowa.gov`.
- Primary endpoints used:
  - LIST AJAX: `https://doc-search.iowa.gov/api/offender/GetOffenderListAjax` — expects form-urlencoded POST used to fetch a page of offenders. Uses server-side paging, ordering, and a `searchModel` object in form keys (flattened into `searchModel.FirsName`, etc.).
  - DETAIL page: `https://doc-search.iowa.gov/offender/detail?offenderNumber={OFFENDER_NUM}` — returns an HTML detail page with charge and demographic info.

## Request mechanics
- **Session Management**: The script now uses a robust `get_fresh_session` function that:
    1.  Hits the base search page to initialize the ASP.NET session.
    2.  Extracts the `__RequestVerificationToken` from the HTML.
    3.  Sets the token as a cookie and header for subsequent requests.
    4.  Navigates to the search results page to set the correct `Referer`.
- **Parallel Processing**:
    - **List Scrape**: Uses `ThreadPoolExecutor` to fetch list pages in parallel batches (e.g., 10 pages at a time).
    - **Detail Scrape**: Uses `ThreadPoolExecutor` to fetch detail pages for missing offenders in parallel.
- **Proxies**: Uses a large pool of proxies (validated against `http://example.com`) and rotates them. If a session fails, it automatically retries with a new proxy and session.

## Parsing
- The list endpoint returns JSON rows with fields like `Name`, `OffenderNumber`, `Age`, `Gender`.
- The detail page is HTML parsed with BeautifulSoup; dates are normalized via `parse_date_string()` to Python `date` objects.

## ETL Behavior
- **Pre-load**: Connects to the database at startup and loads all existing `OffenderNumber`s into memory (Set) to avoid redundant database queries.
- **Extract**: Parallel fetch of list pages and detail pages.
- **Transform**: Normalize names, parse and cast dates, extract charges into separate `charges` records.
- **Load**: Uses `pyodbc` to write to `Offender_Summary`, `Offender_Detail`, and `Offender_Charges`.
    - **Batch Insert**: Tries to insert records in batches.
    - **Fallback**: If a batch fails (e.g., integrity error), it falls back to single-row inserts to ensure valid records are saved and duplicates are skipped.

## Database schema
- `dbo.Offender_Summary`: Basic info from the list (OffenderNumber, Name, Gender, Age).
- `dbo.Offender_Detail`: Detailed info (Location, Offense, Dates, etc.).
- `dbo.Offender_Charges`: One-to-many charges for each offender.

## Notes and edge cases
- The site is sensitive to session state. The `get_fresh_session` logic is critical.
- Parallelism is tuned to avoid overwhelming the server while maximizing throughput.

## How to run
1.  Ensure `.env` file is configured with MSSQL credentials.
2.  Install dependencies: `pip install -r requirements.txt`.
3.  Run: `python3 DOC-IowaDubuqueRip.py`
