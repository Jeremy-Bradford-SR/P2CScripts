# P2C-DubqueRecentCallsDump.py

Purpose
- A resumable, paginated importer for the CAD dataset. Designed to fetch pages of results in sequence and resume from the last processed page (state saved to `cad_import_state.json`). Useful for initial backfills or periodic full imports.

Behavior
- Maintains `STATE_FILE = cad_import_state.json` which stores `{"last_page": N}`. After successfully processing a page, it calls `save_state(page)`.
- Fetches proxy list, validates proxies, connects to MSSQL and inserts rows similar to `P2C-DubqueRecentCallsRip.py` but supports larger payloads (e.g., `rows=max_rows` up to 200).

Fault tolerance
- Each page uses a rotating proxy and will skip or retry on errors. If proxies fail entirely, the script exits with a non-zero code.

Schema and ETL considerations
- Same `dbo.CadHandler` structure as `P2C-DubqueRecentCallsRip.py`.
