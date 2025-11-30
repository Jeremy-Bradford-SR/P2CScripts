# P2C-SexOffenderParser.py

This script scrapes the Iowa Sex Offender Registry (specifically for Dubuque County) and stores the data in a SQL Server database.

## Features

*   **Proxy Rotation**: Automatically fetches and validates a list of free proxies to avoid IP blocking. Rotates proxies for every request.
*   **Concurrent Processing**: Uses multi-threading to fetch search results and individual registrant details in parallel.
*   **Robust Parsing**: Handles various API response formats and errors gracefully.
*   **Photo Storage**: Downloads and stores registrant photos directly in the database (`VARBINARY(MAX)`).
*   **Upsert Logic**: Updates existing records if they have changed, and inserts new ones.
*   **Child Table Handling**: Manages related data (convictions, victims, aliases, markings) by deleting old records and re-inserting the current state to ensure consistency.
*   **Incremental Updates**: Supports an `--update` flag to fetch only records updated "yesterday".

## Usage

```bash
# Full scrape (default 10 workers)
python3 P2C-SexOffenderParser.py

# Incremental update (fetch only recently changed records)
python3 P2C-SexOffenderParser.py --update

# Custom number of workers
python3 P2C-SexOffenderParser.py --max_workers 20
```

## Database Tables

The script populates the following tables (prefixed with `sexoffender_`):

*   `sexoffender_registrants`: Main registrant details (name, address, physical description, photo).
*   `sexoffender_convictions`: Criminal convictions associated with the registrant.
*   `sexoffender_conviction_victims`: Details about victims for each conviction.
*   `sexoffender_aliases`: Known aliases.
*   `sexoffender_skin_markings`: Tattoos, scars, and other markings.

## Configuration

Requires the standard `.env` configuration used by other scripts in this repository:

*   `MSSQL_SERVER`
*   `MSSQL_DATABASE`
*   `MSSQL_USERNAME`
*   `MSSQL_PASSWORD`
