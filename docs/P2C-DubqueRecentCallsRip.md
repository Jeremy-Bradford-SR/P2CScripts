# P2C-DubqueRecentCallsRip.py

Purpose
- Retrieves a single page of the City of Dubuque CAD (recent calls) data from `cadHandler.ashx` and inserts new rows into `dbo.CadHandler`.

Endpoint
- URL: `http://p2c.cityofdubuque.org/cad/cadHandler.ashx?op=s`
- Accepts POST body parameters similar to jqGrid requests (t=css, _search=false, nd, rows, page, sidx, sord).

Request & proxies
- Similar to the Daily Bulletin scraper: fetch a public proxy list, validate proxies in parallel, rotate through them and use random User-Agent values.
- The script constructs `payload` with `rows=200` and `sord=desc` to fetch recent events.

Parsing & insertion
- JSON response includes `rows` array; each row contains `invid`, `starttime`, `closetime`, `id`, `agency`, `service`, `nature`, `address`, `geox`, `geoy`, `rec_key`, `icon_url`, `icon`.
- The script converts numeric geox/geoy to floats and inserts into `geox`, `geoy` columns and leaves `geog` column NULL; a separate PowerShell helper (`UpdateCADHandler-GeoG.ps1`) computes `geog` points.
- Duplicate detection via `SELECT 1 FROM dbo.CadHandler WHERE id = ?`.

Database schema (`dbo.CadHandler`) — suggested types
- invid: BIGINT NULL
- starttime: DATETIME NULL
- closetime: DATETIME NULL
- id: BIGINT PRIMARY KEY
- agency: NVARCHAR(100) NULL
- service: NVARCHAR(100) NULL
- nature: NVARCHAR(200) NULL
- address: NVARCHAR(300) NULL
- geox: FLOAT NULL
- geoy: FLOAT NULL
- geog: geography NULL -- populated later
- marker_details_xml: XML or NVARCHAR(MAX) NULL
- rec_key: NVARCHAR(200) NULL
- icon_url: NVARCHAR(400) NULL
- icon: NVARCHAR(200) NULL

Notes
- The script is intended to be robust to proxy failures and will abort if no validated proxies can reach the endpoint. `P2C-DubqueRecentCallsDump.py` provides a resumable full dump option.
