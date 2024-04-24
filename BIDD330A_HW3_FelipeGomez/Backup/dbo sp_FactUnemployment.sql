USE [Black_Unemployment]
GO

/****** Object:  StoredProcedure [dbo].[sp_FactUnemployment]    Script Date: 4/23/2024 1:06:40 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[sp_FactUnemployment]
AS
--Step 2: Drop Table
DROP TABLE IF EXISTS [dbo].[FactUnemployment]

--Step 3: Create Table with updated types
CREATE TABLE [dbo].[FactUnemployment]
(
	FactUnemployment_Key int IDENTITY (1,1) --future Surrogate Key
	,State nvarchar(50)
	, [Filed week ended] date
	, [Initial Claims] int
	, [Reflecting Week Ended] date
	, [Continued Claims] int
	, [Covered Employment] int
	, [Insured Unemployment Rate] float
)

--Step 4:Write ETL fixing datatypes
INSERT INTO [dbo].[FactUnemployment]
SELECT  --Please start with TOP 10 rows. This will speed up your attempts
	[State]
	, CAST([Filed week ended] as date)
	, CAST([Initial Claims] as INT)
	, CAST([Reflecting Week Ended] as date)
	, CAST([Continued Claims] as INT)
	, CAST([Covered Employment] as INT)
	, CAST([Insured Unemployment Rate] as float)
FROM [Black_Unemployment].[dbo].[Raw_Unemployment] RAW WITH (NOLOCK)
GO


