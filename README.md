
# Dubuque P2C & DOC Scrapers

This repository contains scripts for scraping Dubuque P2C endpoints and Iowa DOC offender search and loading the data into an MSSQL database.

Quick list of scripts
- `DOC-IowaDubuqueRip.py` — DOC offender list/detail scraper
- `P2C-DubqueDailyBullitenRip.py` — Daily Bulletin (arrests) scraper
- `P2C-DubqueRecentCallsRip.py` — CAD recent calls scraper (single page)
- `P2C-DubqueRecentCallsDump.py` — Resumable CAD dump importer
- `UpdateDAB-TimetoEventTime.py` / `UpdateDBA-Eventtime.ps1` — Convert raw `time` text to `event_time` DATETIME
- `UpdateCADHandler-GeoG.ps1` — Convert geox/geoy (EPSG:26975) to WGS84 `geog`
- `P2C-DubuqueDatabaseBackup.ps1` — SQL Server backup/prune helper

Prerequisites
- Python 3.8+
- Python packages: `requests`, `beautifulsoup4`, `pyodbc` (plus `pyproj` for coordinate conversion)
- ODBC Driver for SQL Server (17 or 18)

Configuration
- Update MSSQL_* constants in each script OR set credentials via environment variables (recommended for production).

Documentation
- Detailed per-script documentation, ETL notes, and schema samples live under `docs/` (see `docs/INDEX.md`).

Operational notes
- The scrapers use public proxies and user-agent rotation to reduce the chance of being blocked; this is imperfect. For reliable long-term ingestion, run from a static IP or use a dedicated proxy/VPN with controlled rate limiting.

