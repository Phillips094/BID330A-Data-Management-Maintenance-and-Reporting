USE [Gold_Debt]
GO


CREATE OR ALTER PROCEDURE [dbo].[sp_FactCovid]
AS
/**************
*Developer: Tim Pauley
*Date: 04/16/2024
*Description: ETL Process for Raw Fact Covid 
*
*
*****************/

--Step 2: Drop Table
DROP TABLE IF EXISTS [dbo].[FactCovidOld]

--Step 3: Create Table with updated types
CREATE TABLE [dbo].[FactCovidOld]
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
INSERT INTO [dbo].[FactCovidOld]
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
FROM [dbo].[bing_covid-19_data_Raw] RAW WITH (NOLOCK)
GO


