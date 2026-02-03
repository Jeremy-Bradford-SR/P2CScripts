# Dubuque P2C & DOC Scrapers

This repository contains scripts for scraping Dubuque P2C endpoints and Iowa DOC offender search and loading the data into an MSSQL database.

## Key Features
*   **Centralized Configuration**: Uses `.env` file for secure database credentials.
*   **Robust Proxy Handling**: Validates proxies against reliable targets and rotates them to avoid blocking.
*   **Session Management**: Handles ASP.NET session cookies and Anti-Forgery tokens automatically.
*   **Parallel Processing**: Uses threading to speed up list and detail scraping (especially for DOC data).
*   **Resilience**: Implements retry logic for network requests and database connections.

## Scripts
- `DOC-IowaDubuqueRip.py` — DOC offender list/detail scraper (Parallelized, Robust)
- `P2C-DubqueDailyBulletinRip.py` — Daily Bulletin (arrests) scraper (Retry logic, Configurable days)
- `P2C-DubqueRecentCallsRip.py` — CAD recent calls scraper (Robust session handling)
- `P2C-DubqueRecentCallsDump.py` — Resumable CAD dump importer
- `P2C-SexOffenderParser.py` — Iowa Sex Offender Registry scraper (Proxies, Photos, Upsert)
- `P2C-JailInmatesRip.py` — Current Jail Inmates scraper (Photos, Charges, Release Tracking)
- `UpdateDAB-TimetoEventTime.py` / `UpdateDBA-Eventtime.ps1` — Convert raw `time` text to `event_time` DATETIME
- `UpdateCADHandler-GeoG.ps1` — Convert geox/geoy (EPSG:26975) to WGS84 `geog`
- `P2C-DubuqueDatabaseBackup.ps1` — SQL Server backup/prune helper
- `backfill_geocoding.py` — Backfills missing latitude/longitude coordinates for addresses using a local geocoding proxy.
- `create_view_violators.py` — Creates the `vw_DistinctViolators` view for identifying repeat offenders.
- `check_view.py` / `check_schema.py` — Utility scripts for verifying database schema and view health.
- `debug_jail_raw.py` — Diagnostic script for inspecting raw jail scraper HTML output.

## Prerequisites
- Python 3.8+
- ODBC Driver for SQL Server (17 or 18)

## Installation
1.  Clone the repository.
2.  Install Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

## Configuration
1.  Create a `.env` file in the root directory (copy from a template if available, or create new).
2.  Add your MSSQL database credentials:
    ```env
    MSSQL_SERVER="your_server_ip"
    MSSQL_DATABASE="your_database_name"
    MSSQL_USERNAME="your_username"
    MSSQL_PASSWORD="your_password"
    ```

## Documentation
- Detailed per-script documentation, ETL notes, and schema samples live under `docs/` (see `../docs/README.md`).

## Operational Notes
- The scrapers use public proxies and user-agent rotation to reduce the chance of being blocked.
- For `DOC-IowaDubuqueRip.py`, the script now pre-loads existing IDs to minimize database hits and uses parallel workers for faster scraping.
- `P2C` scripts now include command-line arguments for flexibility (e.g., `--rows`, `--days`).
