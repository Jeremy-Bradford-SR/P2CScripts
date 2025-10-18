# Helpers, Post-processing, and ETL overview

Files covered
- `UpdateDAB-TimetoEventTime.py` — Python helper to convert the `time` text field on `DailyBulletinArrests` to a normalized `event_time` DATETIME using `TRY_CONVERT`.
- `UpdateDBA-Eventtime.ps1` — PowerShell wrapper doing the same update using `Invoke-Sqlcmd`.
- `UpdateCADHandler-GeoG.ps1` — PowerShell script that reads `geox` and `geoy` and uses `pyproj` (EPSG:26975 -> EPSG:4326) to populate `geog` as `geography::Point(lat, lon, 4326)`.
- `P2C-DubuqueDatabaseBackup.ps1` — Backups and prunes old backups.

ETL flow (high level)
1. Extract: Scripts request JSON/HTML from remote endpoints (P2C or DOC).
2. Transform: Parse JSON or HTML, normalize types (ints, floats, dates), and produce canonical row objects. For Daily Bulletin, `time` is kept as raw string and later converted to `event_time`.
3. Load: Insert into MSSQL tables using `pyodbc`. Duplicate prevention is done with `SELECT 1 WHERE id = ?` prior to INSERT.
4. Post-process: Run `UpdateDAB-TimetoEventTime.py` or `UpdateDBA-Eventtime.ps1` to populate `event_time`. Run `UpdateCADHandler-GeoG.ps1` to convert coordinates to `geog` points.

Database constraints and indexing suggestions
- `dbo.DailyBulletinArrests`:
  - Primary key on `id` (BIGINT)
  - Index on `event_time` for temporal queries
  - Index on `lastname` or `name` for lookup

- `dbo.CadHandler`:
  - Primary key on `id` (BIGINT)
  - Spatial index on `geog` after population
  - Index on `starttime` and `rec_key`

Security & operational notes
- Avoid storing DB credentials in scripts. Use environment variables, Windows Credential Manager, or a secrets service.
- Proxies: the scripts use public proxy lists which are unreliable. Consider running these from a static server or VPN with fixed IP and rate limits.
- Monitor failures: schedule scripts under a task runner or CI system and notify on non-zero exit codes.
