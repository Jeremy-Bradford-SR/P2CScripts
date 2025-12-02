# P2C-JailInmatesRip.py

## Overview
This script scrapes the "Inmate Inquiry" section of the Dubuque P2C website (`http://p2c.cityofdubuque.org/jailinmates.aspx`). It retrieves a list of current inmates, downloads their mugshots, and fetches detailed charge information for each inmate.

## Features
*   **Full Inmate List**: Fetches all currently incarcerated inmates.
*   **Detail Scraping**: Navigates to the individual detail page for each inmate to retrieve:
    *   Total Bond Amount
    *   Detailed Charges (Charge Description, Status, Docket Number, Bond Amount per charge)
*   **Mugshot Downloading**: Downloads and stores the inmate's photo as a binary blob (`VARBINARY(MAX)`) in the database.
*   **Release Tracking**: Automatically detects when an inmate is no longer on the active list and marks them as released with a timestamp.
*   **Proxy Support**: Uses a rotating list of proxies to avoid IP bans and handle rate limiting.
*   **Session Management**: robustly handles ASP.NET session state (`__VIEWSTATE`, `ASP.NET_SessionId`) to navigate complex postback-driven pages.

## Usage
```bash
python3 P2C-JailInmatesRip.py
```

## How It Works
1.  **Proxy Validation**: Validates a pool of proxies from `proxies.txt`.
2.  **Session Initialization**: 
    *   Hits `main.aspx` to establish a session.
    *   Hits `jailinmates.aspx` to get the initial `__VIEWSTATE`.
    *   Performs a "search" via `jqHandler.ashx` to populate the server-side session with the inmate list.
3.  **Data Fetching**:
    *   Iterates through the JSON data returned by the search.
    *   For each inmate, simulates a postback to get the redirect URL for their detail page.
    *   Parses the detail page HTML to extract the Total Bond and a table of Charges.
    *   Downloads the mugshot image.
4.  **Database Upsert**:
    *   Inserts or Updates the `jail_inmates` table with inmate details and photo.
    *   Replaces records in the `jail_charges` table for the inmate to reflect the current charges.
5.  **Release Processing**:
    *   Compares the list of scraped Book IDs with the currently "active" (non-released) IDs in the database.
    *   Any ID in the database but not in the scrape is marked as released (`released_date = GETDATE()`).

## Database Tables
*   `jail_inmates`: Stores the main inmate record.
*   `jail_charges`: Stores individual charges associated with an inmate.

## Dependencies
*   `requests`
*   `beautifulsoup4`
*   `pyodbc`
*   `python-dotenv`
