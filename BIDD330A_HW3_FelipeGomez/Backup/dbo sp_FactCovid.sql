USE [Black_Unemployment]
GO

/****** Object:  StoredProcedure [dbo].[sp_FactCovid]    Script Date: 4/23/2024 1:06:35 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[sp_FactCovid]
AS
--Step 2: Drop Table
DROP TABLE IF EXISTS [dbo].[FactCovid]

--Step 3: Create Table with updated types
CREATE TABLE [dbo].[FactCovid]
(
	FactCovid_Key int IDENTITY (1,1) --future Surrogate Key
	,ID INT
	,Updated date
	,Confirmed int
	,Confirmed_Change int
	,Deaths int
	,Deaths_Change int
	,Recovered int
	,Recovered_Change int
	,Latitude nvarchar(50)
	,Longitude nvarchar(50)
	,Iso2 nvarchar(50)
	,Iso3  nvarchar(50)
	,Country_Region nvarchar(50)
	,Admin_Region_1 nvarchar(50)
	,Iso_Subdivision nvarchar(50)
	,Admin_Region_2 nvarchar(50)
	--,Load_Time date
)

--Step 4:Write ETL fixing datatypes
INSERT INTO [dbo].[FactCovid]
SELECT
--TOP 10  --Please start with TOP 10 rows. This will speed up your attempts
	CAST([id] as INT)
	, CAST([updated] as date)
	, CAST([confirmed] as INT)
	, CAST([confirmed_change] as INT)
	, CAST([deaths] as INT)
	, CAST([deaths_change] as INT)
	, CAST([recovered] as INT)
	, CAST([recovered_change] as INT)
	, [latitude]
	, [longitude]
	, [iso2]
	, [iso3]
	, [country_region]
	, [admin_region_1]
	, [iso_subdivision]
	, [admin_region_2]
	--, CAST([load_time] as date)
FROM [Black_Unemployment].[dbo].[Raw_BingCovid] RAW WITH (NOLOCK)
GO


