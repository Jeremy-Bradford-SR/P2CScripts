# Dubuque P2C & DOC Scrapers

This repository contains scripts for scraping Dubuque P2C endpoints and Iowa DOC offender search and loading the data into an MSSQL database.

## Key Features
*   **Centralized Configuration**: Uses `.env` file for secure database credentials.
*   **Robust Proxy Handling**: Validates proxies against reliable targets and rotates them to avoid blocking.
*   **Session Management**: Handles ASP.NET session cookies and Anti-Forgery tokens automatically.
*   **Parallel Processing**: Uses threading to speed up list and detail scraping (especially for DOC data).
*   **Resilience**: Implements retry logic for network requests and database connections.

## System Updates (2026)
Major refactor and reliability updates have been applied:
- **Infinite Retry Logic**: Scripts now retry indefinitely on failure (no fixed limits).
- **Directory Structure**: Scripts are now located in `scripts/ingestion/` and `scripts/ETL/`.
- **Orchestrator**: Centralized management with robust proxy handling.

**[Read Detailed Orchestrator Documentation](docs/P2C_ORCHESTRATOR.md)**

## Scripts
> **Note**: Primary scripts have moved to `scripts/ingestion/`.

- `scripts/ingestion/DOC-IowaDubuqueRip.py` — DOC offender list/detail scraper.
- `scripts/ingestion/P2C-DubqueDailyBulletinRip.py` — Daily Bulletin (arrests) scraper.
- `scripts/ingestion/P2C-DubqueRecentCallsRip.py` — CAD recent calls scraper.
- `scripts/ingestion/P2C-SexOffenderParser.py` — Iowa Sex Offender Registry scraper.
- `scripts/ingestion/P2C-JailInmatesRip.py` — Current Jail Inmates scraper.
- `scripts/ETL/backfill_geocoding.py` — Geocoding helper.
- `scripts/ETL/UpdateDAB_TimetoEventTime.py` — Timestamp parser.

## Configuration
1.  Create a `.env` file in the root directory (copy from a template if available, or create new).
2.  Add your MSSQL database credentials.

## Documentation
- See `docs/README.md` for historical docs.
- See `docs/P2C_ORCHESTRATOR.md` for current API and System usage.
