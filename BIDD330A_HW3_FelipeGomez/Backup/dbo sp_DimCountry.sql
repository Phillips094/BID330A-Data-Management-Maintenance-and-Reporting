USE [Black_Unemployment]
GO

/****** Object:  StoredProcedure [dbo].[sp_DimCountry]    Script Date: 4/23/2024 1:06:27 AM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[sp_DimCountry]
AS
BEGIN
    DROP TABLE IF EXISTS [dbo].[DimCountry];
    CREATE TABLE [dbo].[DimCountry] (
        DimID     INT            IDENTITY (1, 1),
        [Country] NVARCHAR (100)
    );
    INSERT INTO [dbo].[DimCountry]
    SELECT DISTINCT [Country_Region]
    FROM   [Black_Unemployment].[dbo].[FactCovid];
END

GO


