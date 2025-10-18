# Database schema and CREATE TABLE examples

This file provides suggested schemas for the tables the scrapers insert into. Adjust types and sizes to match actual data and indexing needs.

Suggested `DailyBulletinArrests` table

```sql
CREATE TABLE dbo.DailyBulletinArrests (
  id BIGINT PRIMARY KEY,
  invid BIGINT NULL,
  [key] NVARCHAR(100) NULL,
  location NVARCHAR(200) NULL,
  name NVARCHAR(200) NULL,
  crime NVARCHAR(200) NULL,
  [time] NVARCHAR(100) NULL,
  property NVARCHAR(200) NULL,
  officer NVARCHAR(100) NULL,
  [case] NVARCHAR(100) NULL,
  description NVARCHAR(MAX) NULL,
  race NVARCHAR(50) NULL,
  sex CHAR(1) NULL,
  lastname NVARCHAR(100) NULL,
  firstname NVARCHAR(100) NULL,
  middlename NVARCHAR(100) NULL,
  charge NVARCHAR(200) NULL,
  event_time DATETIME NULL
);
CREATE INDEX IX_DBA_EventTime ON dbo.DailyBulletinArrests(event_time);
```

Suggested `CadHandler` table

```sql
CREATE TABLE dbo.CadHandler (
  id BIGINT PRIMARY KEY,
  invid BIGINT NULL,
  starttime DATETIME NULL,
  closetime DATETIME NULL,
  agency NVARCHAR(100) NULL,
  service NVARCHAR(100) NULL,
  nature NVARCHAR(200) NULL,
  address NVARCHAR(300) NULL,
  geox FLOAT NULL,
  geoy FLOAT NULL,
  geog geography NULL,
  marker_details_xml NVARCHAR(MAX) NULL,
  rec_key NVARCHAR(200) NULL,
  icon_url NVARCHAR(400) NULL,
  icon NVARCHAR(200) NULL
);
CREATE INDEX IX_CAD_StartTime ON dbo.CadHandler(starttime);
-- After populating geog, create a spatial index
-- CREATE SPATIAL INDEX SI_CadHandler_Geog ON dbo.CadHandler(geog) USING GEOGRAPHY_GRID;
```

Suggested `Offender` and `OffenderCharge` tables

```sql
CREATE TABLE dbo.Offender (
  id INT IDENTITY(1,1) PRIMARY KEY,
  offender_number NVARCHAR(50) NULL,
  name NVARCHAR(300) NULL,
  age INT NULL,
  gender CHAR(1) NULL,
  birth_date DATE NULL,
  created_at DATETIME DEFAULT GETDATE()
);

CREATE TABLE dbo.OffenderCharge (
  id INT IDENTITY(1,1) PRIMARY KEY,
  offender_id INT NOT NULL FOREIGN KEY REFERENCES dbo.Offender(id),
  charge_text NVARCHAR(MAX) NULL,
  charge_date DATE NULL
);
```

Notes
- All columns are nullable by default to make ingestion robust; tighten constraints once data characteristics are verified.
