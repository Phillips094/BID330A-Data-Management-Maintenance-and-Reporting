USE [Black_Unemployment]
GO

/****** Object:  StoredProcedure [dbo].[sp_DimState]    Script Date: 4/23/2024 1:06:31 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE PROCEDURE [dbo].[sp_DimState]
AS
--Step 2: Drop Table
DROP TABLE IF EXISTS [dbo].[DimState]

--Step 3: Create Table with updated types
CREATE TABLE [dbo].[DimState]
(
	DimState_Key int IDENTITY (1,1) --future Surrogate Key
	,State nvarchar(50)
)

--Step 4:Write ETL fixing datatypes
INSERT INTO [dbo].[DimState]
SELECT DISTINCT --Please start with TOP 10 rows. This will speed up your attempts
	[State]
FROM [Black_Unemployment].[dbo].[FactUnemployment] FACT WITH (NOLOCK)
GO


