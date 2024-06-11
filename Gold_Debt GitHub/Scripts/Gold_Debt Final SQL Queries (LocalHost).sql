--*************************************************************************--
-- Title: BIDD330A Final
-- Author: Felipe Gomez
-- Desc: This file creates our Gold_Data Database, Staging Tables, BULK INSERT INTO our Staging Tables, Dimension and Fact Tables & our Foreign Key Constraints, Flush and Fill ETL process with SQL code
-- Change Log: When,Who,What
-- 2024-05-27,Felipe Gomez,Created File
-- TODO: 05/27/2024,Felipe Gomez,Updated code to include logging and transaction handling
---------------------------------
--PLEASE MAKE SURE TO ADD BOTH COVID19 AND DEBT PENNY CSV FILES DO YOUR C:\Data directory!!!!!!!!!!!!!!!!!!!
USE MASTER
GO

CREATE OR ALTER PROCEDURE sp_ETLGoldDebtDatabaseCreation AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Gold_Debt')
		ALTER DATABASE [Gold_Debt] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE [Gold_Debt];
		CREATE DATABASE [Gold_Debt]
		ALTER DATABASE [Gold_Debt] SET MULTI_USER;
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating Our Database/DataWarehouse Gold_Debt'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLGoldDebtDatabaseCreation;
--GO
------------------------------------
--CREATE STAGING TABLES
USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLStagingBingCovid19DataRaw AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		If (SELECT Object_ID('StagingBingCovid19DataRaw')) IS NOT NULL
		DROP TABLE StagingBingCovid19DataRaw;
		CREATE TABLE StagingBingCovid19DataRaw (
			[id] NVARCHAR(200),
			[updated] NVARCHAR(200),
			[confirmed] NVARCHAR(200),
			[confirmed_change] NVARCHAR(200),
			[deaths] NVARCHAR(200),
			[deaths_change] NVARCHAR(200),
			[recovered] NVARCHAR(200),
			[recovered_change] NVARCHAR(200),
			[latitude] NVARCHAR(200),
			[longitude] NVARCHAR(200),
			[iso2] NVARCHAR(200),
			[iso3] NVARCHAR(200),
			[country_region] NVARCHAR(200),
			[admin_region_1] NVARCHAR(200),
			[iso_subdivision] NVARCHAR(200),
			[admin_region_2] NVARCHAR(MAX),
			[load_time] NVARCHAR(MAX)
		);
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating Staging Table StagingBingCovid19DataRaw for BULK INSERT'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLStagingBingCovid19DataRaw;
--GO

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLStagingDebtPennyRaw AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		If (SELECT Object_ID('StagingDebtPennyRaw')) IS NOT NULL
		DROP TABLE StagingDebtPennyRaw;
		CREATE TABLE StagingDebtPennyRaw (
			[Record Date] NVARCHAR(200),
			[Debt Held by the Public] NVARCHAR(200),
			[Intragovernmental Holdings] NVARCHAR(200),
			[Total Public Debt Outstanding] NVARCHAR(200),
			[Source Line Number] NVARCHAR(200),
			[Fiscal Year] NVARCHAR(200),
			[Fiscal Quarter Number] NVARCHAR(200),
			[Calendar Year] NVARCHAR(200),
			[Calendar Quarter Number] NVARCHAR(200),
		    [Calendar Month Number] NVARCHAR(200),
			[Calendar Day Number]NVARCHAR(200)
		);
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating Staging Table StagingDebtPennyRaw for BULK INSERT'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLStagingDebtPennyRaw;
--GO

--------------------------------------------------------BULK INSERT RAW CSV FILES INTO OUR STAGING TABLES

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLImportBingCovid19DataRawToStagingTable AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		BULK INSERT [StagingBingCovid19DataRaw]
		FROM 'C:\Data\bing_covid-19_data.csv' --'\\DESKTOP\Data\bing_covid-19_data.csv' --
		WITH (
			DATAFILETYPE = 'char',
			FORMAT = 'CSV',
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			FIRSTROW = 2,
			MAXERRORS = 0
		);
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Importing COVID19 Raw Data to Staging Table'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLImportBingCovid19DataRawToStagingTable;
--GO

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLImportDebtPennyRawToStagingTable AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		BULK INSERT [StagingDebtPennyRaw]
		FROM 'C:\Data\DebtPenny_20140426_20240425.csv'
		WITH (
			DATAFILETYPE = 'char',
			FORMAT = 'CSV',
			FIELDTERMINATOR = ',',
			ROWTERMINATOR = '0x0a',
			FIRSTROW = 2,
			MAXERRORS = 0
		);
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Importing Debt Penny Raw Data to Staging Table'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLImportDebtPennyRawToStagingTable;
--GO

-------------------------------------------------------------------CREATE DIMENSION AND FACT TABLES (INCLUDING PRIMARY AND FOREGIN KEYS)

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLGoldDebtDimensionAndFactTablesCreation AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		/****** [dbo].[DimDates] ******/
		DROP TABLE IF EXISTS [dbo].[DimDates]
		CREATE TABLE [dbo].[DimDates] (
			[DateKey] INT NOT NULL,
			[FullDate] DATETIME NOT NULL,
			[FullDateName] NVARCHAR(200) NULL,
			[WeekID] INT NOT NULL,
			[MonthID] INT NOT NULL,
			[MonthName] NVARCHAR(200) NOT NULL,
			[Quarter] INT NOT NULL,
			[YearQuarter] INT NOT NULL,
			[YearID] INT NOT NULL,
			[YearName] NVARCHAR(200) NOT NULL,
			CONSTRAINT [PK_DimDates] PRIMARY KEY CLUSTERED
			([DateKey])
		)

		DROP TABLE IF EXISTS [dbo].[DimCountries]
		CREATE TABLE [dbo].[DimCountries] (
			[CountryKey] INT IDENTITY (1, 1) NOT NULL,
			[Country] NVARCHAR (200)
			CONSTRAINT [PK_DimCountries] PRIMARY KEY CLUSTERED
			([CountryKey])
		)

		DROP TABLE IF EXISTS [dbo].[DimRegions]
		CREATE TABLE [dbo].[DimRegions] (
			[RegionKey] INT IDENTITY (1, 1) NOT NULL,
			[Region] NVARCHAR (200)
			CONSTRAINT [PK_DimRegion] PRIMARY KEY CLUSTERED
			([RegionKey])
		)

		DROP TABLE IF EXISTS [dbo].[FactCovid]
		CREATE TABLE [dbo].[FactCovid] (
			[FactCovidKey] INT IDENTITY (1,1) NOT NULL, --future Surrogate Key
			[ID] INT,
			[Updated] DATE,
			[DateKey] INT,
			[Confirmed] INT,
			[Confirmed_Change] INT,
			[Deaths] INT,
			[Deaths_Change] INT,
			[Recovered] INT,
			[Recovered_Change] INT,
			[Latitude] NVARCHAR(200),
			[Longitude] NVARCHAR(200),
			[Iso2] NVARCHAR(200),
			[Iso3] NVARCHAR(200),
			[Country_Region] NVARCHAR(200),
			[CountryKey] INT,
			[Admin_Region_1] NVARCHAR(200),
			[RegionKey] INT NULL,
			[Iso_Subdivision] NVARCHAR(200),
			[Admin_Region_2] NVARCHAR(200),
			[Load_Time] datetime2,
			CONSTRAINT [PK_FactCovid] PRIMARY KEY CLUSTERED ([FactCovidKey], [ID], [DateKey], [CountryKey])
		) ON [PRIMARY]

		DROP TABLE IF EXISTS [dbo].[FactDebt]
		CREATE TABLE [dbo].[FactDebt] (
			[FactDebtKey] INT IDENTITY(1,1) NOT NULL,
			[Record_Date] DATE NULL,
			[DateKey] INT,
			[Public_Debt] FLOAT NULL,
			[Intragovernmental_Holdings] FLOAT NULL,
			[Total_Public_Debt_Outstanding] FLOAT NULL,
			[Source_Line_Number] INT NULL,
			[Fiscal_Year] INT NULL,
			[Fiscal_Quarter_Number] INT NULL,
			[Calendar_Year] INT NULL,
			[Calendar_Quarter_Number] INT NULL,
			[Calendar_Month_Number] INT NULL,
			[Calendar_Day_Number] INT NULL,
			CONSTRAINT [PK_FactDebt] PRIMARY KEY CLUSTERED ([FactDebtKey], [DateKey])
		) ON [PRIMARY]
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating Dimension and Fact Tables'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLGoldDebtDimensionAndFactTablesCreation;
--GO

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLGoldDebtForeignKeyConstraints AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		ALTER TABLE [dbo].[FactCovid] ADD CONSTRAINT [FK_FactCovid_DimDates] 
			FOREIGN KEY ([DateKey]) REFERENCES [dbo].[DimDates] ([DateKey])
		ALTER TABLE [dbo].[FactCovid] ADD CONSTRAINT [FK_FactCovid_DimCountries] 
			FOREIGN KEY ([CountryKey]) REFERENCES [dbo].[DimCountries] ([CountryKey])
		ALTER TABLE [dbo].[FactCovid] ADD CONSTRAINT [FK_FactCovid_DimRegions] 
			FOREIGN KEY ([RegionKey]) REFERENCES [dbo].[DimRegions] ([RegionKey])
		ALTER TABLE [dbo].[FactDebt] ADD CONSTRAINT [FK_FactDebt_DimDates] 
			FOREIGN KEY ([DateKey]) REFERENCES [dbo].[DimDates] ([DateKey])
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating Foreign Key Constraints'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLGoldDebtForeignKeyConstraints;
--GO

-------------------------------------------------------PERFORM OUR ETL FLUSH AND FILL PROCESS

-- 1) CREATE OUR ETL TABLE, VIEW AND INSERT STORED PROCEDURE
USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLLogTable AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		IF NOT Exists(SELECT * FROM Sys.tables WHERE [name] = 'ETLLog')
		  CREATE -- Drop
		  TABLE ETLLog
		  (ETLLogID INT IDENTITY PRIMARY KEY
		  ,ETLDateAndTime DATETIME DEFAULT GETDATE()
		  ,ETLAction NVARCHAR(100)
		  ,ETLLogMessage NVARCHAR(2000)
		  );
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating ETL Log Table'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLLogTable;
--GO

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLLogView AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		EXEC('CREATE OR ALTER VIEW vETLLog
		AS
		  SELECT
		   ETLLogID
		  ,ETLDate = Format(ETLDateAndTime, ''D'', ''en-us'')
		  ,ETLTime = Format(Cast(ETLDateAndTime as datetime2), ''HH:mm'', ''en-us'')
		  ,ETLAction
		  ,ETLLogMessage
		  FROM ETLLog');
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating ETL Log View For Our Power BI Report'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLLogView;
--GO


USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLInsertETLLog (@ETLAction NVARCHAR(100), @ETLLogMessage NVARCHAR(2000)) AS
BEGIN
  DECLARE @RC INT = 0;
  BEGIN TRY
    BEGIN TRAN;
      INSERT INTO [ETLLog] (ETLAction, ETLLogMessage)
      Values
       (@ETLAction,@ETLLogMessage)
    Commit Tran;
    Set @RC = 1;
  End Try
  Begin Catch
    If @@TRANCOUNT > 0 Rollback Tran;
    Set @RC = -1;
  End Catch
  Return @RC;
End
GO -- If you don't put a go here the test code will be included in the Sproc body!

-- 2) CREATE OUR DROP FOREIGN KEY CONSTRAINTS STORED PROCEDURE

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLDropForeignKeyConstraints AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY

    -- ETL Processing Code --
    ALTER TABLE [dbo].[FactCovid] 
     DROP CONSTRAINT [FK_FactCovid_DimDates] 

    ALTER TABLE [dbo].[FactCovid] 
     DROP CONSTRAINT [FK_FactCovid_DimCountries] 

    ALTER TABLE [dbo].[FactCovid] 
     DROP CONSTRAINT [FK_FactCovid_DimRegions] 

    ALTER TABLE [dbo].[FactDebt] 
     DROP CONSTRAINT [FK_FactDebt_DimDates] 

    -- Optional: Unlike the other tables DimDates does not change often --
    --Alter Table [dbo].[FactSalesOrders] 
    -- Drop Constraint [FK_FactSalesOrders_DimDates]

    EXEC sp_ETLInsertETLLog
	        @ETLAction = 'sp_ETLDropForeignKeyConstraints'
	       ,@ETLLogMessage = 'Foreign Keys dropped';
    SET @RC = +1
  END TRY
  BEGIN CATCH
     DECLARE @ErrorMessage nvarchar(1000) = Error_Message();
	 EXEC sp_ETLInsertETLLog 
	      @ETLAction = 'sp_ETLDropForeignKeyConstraints'
	     ,@ETLLogMessage = 'Foreign Keys cannot be dropped (They may be missing or misnamed)';
    SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO

--EXEC sp_ETLDropForeignKeyConstraints;
--GO

-- 3) CREATE OUR TRUNCATE TABLES STORED PROCEDURE

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLTruncateTables AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
	    -- ETL Processing Code --
    TRUNCATE TABLE [Gold_Debt].dbo.DimCountries;
    TRUNCATE TABLE [Gold_Debt].dbo.DimRegions;
    TRUNCATE TABLE [Gold_Debt].dbo.FactCovid;   
	TRUNCATE TABLE [Gold_Debt].dbo.FactDebt;   
    -- Optional: Unlike the other tables DimDates does not change often --
    TRUNCATE TABLE [Gold_Debt].dbo.DimDates; 

    EXEC sp_ETLInsertETLLog
	        @ETLAction = 'sp_ETLTruncateTables'
	       ,@ETLLogMessage = 'Tables data removed';
    SET @RC = +1
  END TRY
  BEGIN CATCH
     DECLARE @ErrorMessage NVARCHAR(1000) = Error_Message();
	 EXEC sp_ETLInsertETLLog 
	      @ETLAction = 'sp_ETLTruncateTables'
	     ,@ETLLogMessage = @ErrorMessage;
    SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO

--EXEC sp_ETLTruncateTables;
--GO

-- 4) CREATE OUR STORED PROCEDURES TO FILL IN OUR DIMENSIONS

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLFillDimDates AS
BEGIN
  DECLARE @RC INT = 0;
  BEGIN TRY

    -- ETL Processing Code --
      DECLARE @StartDate DATETIME = '01/01/2014'
      DECLARE @EndDate DATETIME = '12/31/2024' 
      DECLARE @DateInProcess DATETIME  = @StartDate
      -- Loop through the dates until you reach the end date
      BEGIN TRAN
      WHILE @DateInProcess <= @EndDate
       BEGIN
       -- Add a row Into the date dimension table for this date
       INSERT INTO DimDates 
       ( [DateKey],[FullDate],[FullDateName],[WeekID],[MonthID],[MonthName],[Quarter],[YearQuarter],[YearID],[YearName])
       VALUES ( 
         Cast(Convert(NVARCHAR(50), @DateInProcess, 112) AS INT) -- [DateKey]
        ,@DateInProcess -- [FullDate]
        ,DateName(WEEKDAY, @DateInProcess) + ', ' + Convert(NVARCHAR(50), @DateInProcess, 110) -- [FullDateName]
		,CAST(DATEPART(ISO_WEEK, @DateInProcess) AS INT) --[WeekID]
        ,Cast(Left(Convert(NVARCHAR(50), @DateInProcess, 112), 6) AS INT)  -- [MonthID]
        ,DateName(month, @DateInProcess) + ' - ' + DateName(YYYY,@DateInProcess) -- [MonthName]
		,DATEPART(QUARTER,@DateInProcess) -- [Quarter]
		,CAST(CONCAT(YEAR(@DateInProcess), DATEPART(QUARTER,@DateInProcess)) AS INT) -- [YearQuarter]
        ,Year(@DateInProcess) -- [YearID] 
        ,Cast(Year(@DateInProcess ) AS NVARCHAR(50)) -- [YearName] 
        )  
       -- Add a day and loop again
       SET @DateInProcess = DateAdd(d, 1, @DateInProcess)
       END
	   COMMIT TRAN

    EXEC sp_ETLInsertETLLog
	        @ETLAction = 'sp_ETLFillDimDates'
	       ,@ETLLogMessage = 'DimDates filled';
    SET @RC = +1
  END TRY
  BEGIN CATCH
     IF @@TRANCOUNT > 0 ROLLBACK TRAN;
     DECLARE @ErrorMessage NVARCHAR(1000) = Error_Message();
	 EXEC sp_ETLInsertETLLog
	      @ETLAction = 'sp_ETLFillDimDates'
	     ,@ETLLogMessage = @ErrorMessage;
    SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO

--EXEC sp_ETLFillDimDates;
--GO

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLvETLDimCountries AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		EXEC('CREATE OR ALTER VIEW [dbo].[vETLDimCountries]
			 /* Desc: Extracts and transforms data for DimCountries*/
		     AS
			 SELECT DISTINCT [Country_Region] AS [Country]
			 FROM [Gold_Debt].[dbo].[StagingBingCovid19DataRaw] [C19]');
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating ETL DimCountries View For Inserting Data Into Our DimCountries Table'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLvETLDimCountries;
--GO

--NO LONGER NEEDED BECAUSE WE CREATE A STORED PROCEDURE TO CREATE OR ALTER THE VIEW FOR vETLDimCountries!!!!!!!!!!!!!!!!!
--CREATE OR ALTER VIEW vETLDimCountries
--/* Desc: Extracts and transforms data for DimCountries
--*/
--AS SELECT DISTINCT [Country_Region] AS [Country] FROM [Gold_Debt].[dbo].[StagingBingCovid19DataRaw] [C19]
--GO

CREATE OR ALTER PROCEDURE sp_ETLFillDimCountries AS
BEGIN
  DECLARE @RC INT = 0;
  BEGIN TRY

    -- ETL Processing Code --
    IF ((SELECT Count(*) FROM DimCountries) = 0)
     BEGIN TRAN
      INSERT INTO [Gold_Debt].dbo.DimCountries
      ([Country]
      )
      SELECT [Country]
      FROM vETLDimCountries
    COMMIT TRAN

    EXEC sp_ETLInsertETLLog
	        @ETLAction = 'sp_ETLFillDimCountries'
	       ,@ETLLogMessage = 'DimCountries filled';
    SET @RC = +1
  END TRY
  BEGIN CATCH
     IF @@TRANCOUNT > 0 ROLLBACK TRAN;
     DECLARE @ErrorMessage NVARCHAR(1000) = Error_Message();
	 EXEC sp_ETLInsertETLLog 
	      @ETLAction = 'sp_ETLFillDimCountries'
	     ,@ETLLogMessage = @ErrorMessage;
    SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO

--EXEC sp_ETLFillDimCountries;
--GO

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLvETLDimRegions AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		EXEC('CREATE OR ALTER VIEW [dbo].[vETLDimRegions]
			 /* Desc: Extracts and transforms data for DimRegions*/
		     AS
			 SELECT DISTINCT [Admin_Region_1] AS [Region]
			 FROM [Gold_Debt].[dbo].[StagingBingCovid19DataRaw] [C19]');
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating ETL DimRegions View For Inserting Data Into Our DimRegions Table'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLvETLDimRegions;
--GO

--NO LONGER NEEDED BECAUSE WE CREATE A STORED PROCEDURE TO CREATE OR ALTER THE VIEW FOR vETLDimRegions!!!!!!!!!!!!!!!!!
--CREATE OR ALTER VIEW vETLDimRegions
--/* Desc: Extracts and transforms data for DimRegions
--*/
--AS SELECT DISTINCT [Admin_Region_1] AS [Region] FROM [Gold_Debt].[dbo].[StagingBingCovid19DataRaw] [C19]
--GO

CREATE OR ALTER PROCEDURE sp_ETLFillDimRegions AS
BEGIN
  DECLARE @RC INT = 0;
  BEGIN TRY

    -- ETL Processing Code --
    IF ((SELECT COUNT(*) FROM DimRegions) = 0)
     BEGIN TRAN
      INSERT INTO [Gold_Debt].dbo.DimRegions
      ([Region]
      )
      SELECT [Region]
      FROM vETLDimRegions
    COMMIT TRAN

    EXEC sp_ETLInsertETLLog
	        @ETLAction = 'sp_ETLFillDimRegions'
	       ,@ETLLogMessage = 'DimRegions filled';
    SET @RC = +1
  END TRY
  BEGIN CATCH
     IF @@TRANCOUNT > 0 ROLLBACK TRAN;
     DECLARE @ErrorMessage NVARCHAR(1000) = Error_Message();
	 EXEC sp_ETLInsertETLLog 
	      @ETLAction = 'sp_ETLFillDimRegions'
	     ,@ETLLogMessage = @ErrorMessage;
    SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO

--EXEC sp_ETLFillDimRegions;
--GO

-- 5) CREATE OUR STORED PROCEDURES TO FILL IN OUR FACTS

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLvETLFactCovid AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		EXEC('CREATE OR ALTER VIEW [dbo].[vETLFactCovid]
			 /* Desc: Extracts and transforms data for FactCovid*/
		     AS
			  SELECT
				   --[FactCovidKey]
				   [ID]
				  ,[Updated]
				  ,[DateKey] = [D].[DateKey]
				  ,[Confirmed]
				  ,[Confirmed_Change]
				  ,[Deaths]
				  ,[Deaths_Change]
				  ,[Recovered]
				  ,[Recovered_Change]
				  ,[Latitude]
				  ,[Longitude]
				  ,[Iso2]
				  ,[Iso3]
				  ,[Country_Region]
				  ,[CountryKey] = [C].[CountryKey]
				  ,[Admin_Region_1]
				  ,[RegionKey] = [R].[RegionKey]
				  ,[Iso_Subdivision]
				  ,[Admin_Region_2]
				  ,[Load_Time]
			  FROM [Gold_Debt].[dbo].[StagingBingCovid19DataRaw] [C19]
			  LEFT JOIN [Gold_Debt].[dbo].[DimCountries] [C]
			   ON [C19].[Country_Region] = [C].[Country]
			  LEFT JOIN [Gold_Debt].[dbo].[DimRegions] [R]
			   ON [C19].[Admin_Region_1] = [R].[Region]
			  LEFT JOIN [Gold_Debt].[dbo].[DimDates] [D]
			   ON CAST([C19].[Updated] AS DATE) = CAST([D].[FullDate] AS DATE)');
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating ETL FactCovid View For Inserting Data Into Our FactCovid Table'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLvETLFactCovid;
--GO

--NO LONGER NEEDED BECAUSE WE CREATE A STORED PROCEDURE TO CREATE OR ALTER THE VIEW FOR vETLFactCovid!!!!!!!!!!!!!!!!!
--CREATE OR ALTER VIEW vETLFactCovid
--/* Desc: Extracts and transforms data for FactCovid
--*/
--AS
--  SELECT
--       --[FactCovidKey]
--       [ID],[Updated],[CovidDateKey] = [D].[DateKey],[Confirmed],[Confirmed_Change],[Deaths],[Deaths_Change],[Recovered],[Recovered_Change],[Latitude] ,[Longitude],[Iso2],[Iso3]
--      ,[Country_Region],[CountryKey] = [C].[CountryKey],[Admin_Region_1],[RegionKey] = [R].[RegionKey],[Iso_Subdivision],[Admin_Region_2],[Load_Time]
--  FROM [Gold_Debt].[dbo].[StagingBingCovid19DataRaw] [C19]
--  LEFT JOIN [Gold_Debt].[dbo].[DimCountries] [C] ON [C19].[Country_Region] = [C].[Country]
--  LEFT JOIN [Gold_Debt].[dbo].[DimRegions] [R] ON [C19].[Admin_Region_1] = [R].[Region]
--  LEFT JOIN [Gold_Debt].[dbo].[DimDates] [D] ON CAST([C19].[Updated] AS DATE) = CAST([D].[FullDate] AS DATE)
--GO

CREATE OR ALTER PROCEDURE sp_ETLFillFactCovid
/* Desc: Inserts data Into FactCovid using the vETLFactCovid view
*/
AS
 BEGIN
  DECLARE @RC INT = 0;
  BEGIN TRY 

    -- ETL Processing Code --
    IF ((SELECT COUNT(*) FROM [Gold_Debt].[dbo].[FactCovid]) = 0)
     BEGIN TRAN
      INSERT INTO [Gold_Debt].[dbo].[FactCovid]
      ([ID]
      ,[Updated]
      ,[DateKey]
      ,[Confirmed]
      ,[Confirmed_Change]
      ,[Deaths]
      ,[Deaths_Change]
      ,[Recovered]
      ,[Recovered_Change]
      ,[Latitude]
      ,[Longitude]
      ,[Iso2]
      ,[Iso3]
      ,[Country_Region]
      ,[CountryKey]
      ,[Admin_Region_1]
      ,[RegionKey]
      ,[Iso_Subdivision]
      ,[Admin_Region_2]
      ,[Load_Time]
      )
      SELECT
       [ID]
      ,[Updated]
      ,[DateKey]
      ,[Confirmed]
      ,[Confirmed_Change]
      ,[Deaths]
      ,[Deaths_Change]
      ,[Recovered]
      ,[Recovered_Change]
      ,[Latitude]
      ,[Longitude]
      ,[Iso2]
      ,[Iso3]
      ,[Country_Region]
      ,[CountryKey]
      ,[Admin_Region_1]
      ,[RegionKey]
      ,[Iso_Subdivision]
      ,[Admin_Region_2]
      ,[Load_Time]
      FROM [dbo].[vETLFactCovid]
    COMMIT TRAN

    EXEC sp_ETLInsertETLLog
	        @ETLAction = 'sp_ETLFillFactCovid'
	       ,@ETLLogMessage = 'FactCovid filled';
    SET @RC = +1
  END TRY
  BEGIN CATCH
     IF @@TRANCOUNT > 0 ROLLBACK TRAN;
     DECLARE @ErrorMessage NVARCHAR(1000) = Error_Message();
	 EXEC sp_ETLInsertETLLog 
	      @ETLAction = 'sp_ETLFillFactCovid'
	     ,@ETLLogMessage = @ErrorMessage;
    SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO

--EXEC sp_ETLFillFactCovid;
--GO

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLvETLFactDebt AS
BEGIN
	DECLARE @RC INT = 0;--CAST(LEFT(TRIM([Calendar Day Number]), 2) AS char(2))
	BEGIN TRY
		EXEC('CREATE OR ALTER VIEW [dbo].[vETLFactDebt]
			 /* Desc: Extracts and transforms data for FactDebt*/
		     AS
			  SELECT
				   --[FactDebtKey]
				   [RecordDate] = [Record Date]
				  ,[DateKey] = [D].[DateKey]
				  ,[Public_Debt] = CAST([Debt Held by the Public] AS money)
				  ,[Intragovernmental_Holdings] = CAST([Intragovernmental Holdings] AS money)
				  ,[Total_Public_Debt_Outstanding] = CAST([Total Public Debt Outstanding] AS money)
				  ,[Source_Line_Number] = [Source Line Number]
				  ,[Fiscal_Year] = [Fiscal Year]
				  ,[Fiscal_Quarter_Number] = [Fiscal Quarter Number]
				  ,[Calendar_Year] = [Calendar Year]
				  ,[Calendar_Quarter_Number] = CAST(TRIM([Calendar Quarter Number]) AS INT)
				  ,[Calendar_Month_Number] = CAST(TRIM([Calendar Month Number]) AS INT)
				  ,[Calendary_Day_Number] = CAST(TRIM(REPLACE([Calendar Day Number], CHAR(13), '''')) AS INT)
			  FROM [Gold_Debt].[dbo].[StagingDebtPennyRaw] [DPR]
			  LEFT JOIN [Gold_Debt].[dbo].[DimDates] [D]
			   ON CAST([DPR].[Record Date] AS DATE) = CAST([D].[FullDate] AS DATE)');
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating ETL FactDebt View For Inserting Data Into Our FactDebt Table'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLvETLFactDebt;
--GO

--NO LONGER NEEDED BECAUSE WE CREATE A STORED PROCEDURE TO CREATE OR ALTER THE VIEW FOR vETLFactDebt!!!!!!!!!!!!!!!!!
--CREATE OR ALTER VIEW vETLFactDebt
--/* Desc: Extracts and transforms data for FactCovid
--*/
--AS
--  SELECT
--       --[FactDebtKey]
--	   [RecordDate] = [Record Date],[DebtDateKey] = [D].[DateKey],[Public_Debt] = [Debt Held by the Public],[Intragovernmental_Holdings] = [Intragovernmental Holdings],[Total_Public_Debt_Outstanding] = [Total Public Debt Outstanding],[Source_Line_Number] = [Source Line Number],[Fiscal_Year] = [Fiscal Year]
--      ,[Fiscal_Quarter_Number] = [Fiscal Quarter Number],[Calendar_Year] = [Calendar Year],[Calendar_Quarter_Number] = [Calendar Quarter Number],[Calendar_Month_Number] = [Calendar Month Number],[Calendary_Day_Number] = [Calendar Day Number]
--  FROM [Gold_Debt].[dbo].[StagingDebtPennyRaw] [DPR]
--  LEFT JOIN [Gold_Debt].[dbo].[DimDates] [D] ON CAST([DPR].[Record Date] AS DATE) = CAST([D].[FullDate] AS DATE)
--GO

CREATE OR ALTER PROCEDURE sp_ETLFillFactDebt
/* Desc: Inserts data Into FactCovid using the vETLFactCovid view
*/
AS
 BEGIN
  DECLARE @RC INT = 0;
  BEGIN TRY 

    -- ETL Processing Code --
    IF ((SELECT COUNT(*) FROM FactDebt) = 0)
     BEGIN TRAN
      INSERT INTO [Gold_Debt].[dbo].[FactDebt]
      ([Record_Date]
      ,[DateKey]
      ,[Public_Debt]
      ,[Intragovernmental_Holdings]
      ,[Total_Public_Debt_Outstanding]
      ,[Source_Line_Number]
      ,[Fiscal_Year]
      ,[Fiscal_Quarter_Number]
      ,[Calendar_Year]
      ,[Calendar_Quarter_Number]
      ,[Calendar_Month_Number]
      ,[Calendar_Day_Number]
      )
      SELECT
	   [RecordDate]
	  ,[DateKey]
      ,[Public_Debt]
      ,[Intragovernmental_Holdings]
      ,[Total_Public_Debt_Outstanding]
      ,[Source_Line_Number]
      ,[Fiscal_Year]
      ,[Fiscal_Quarter_Number]
      ,[Calendar_Year]
      ,[Calendar_Quarter_Number]
      ,[Calendar_Month_Number]
      ,[Calendary_Day_Number]
      FROM vETLFactDebt
    COMMIT TRAN

    EXEC sp_ETLInsertETLLog
	        @ETLAction = 'sp_ETLFillFactDebt'
	       ,@ETLLogMessage = 'FactDebt filled';
    SET @RC = +1
  END TRY
  BEGIN CATCH
     IF @@TRANCOUNT > 0 ROLLBACK TRAN;
     DECLARE @ErrorMessage NVARCHAR(1000) = Error_Message();
	 EXEC sp_ETLInsertETLLog 
	      @ETLAction = 'sp_ETLFillFactDebt'
	     ,@ETLLogMessage = @ErrorMessage;
    SET @RC = -1
  END CATCH
  RETURN @RC;
 END
GO

--EXEC sp_ETLFillFactDebt;
--GO

-- 6) Add Back Foreign Keys

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLAddForeignKeyConstraints
/* Desc: This Sproc Replaces the Foreign Keys Constraints.
*/
AS
BEGIN
	DECLARE @RC INT = 1;
	DECLARE @Message NVARCHAR(1000);
  BEGIN TRY
	ALTER TABLE [dbo].[FactCovid] ADD CONSTRAINT [FK_FactCovid_DimDates] 
		FOREIGN KEY ([DateKey]) REFERENCES [dbo].[DimDates] ([DateKey])
	ALTER TABLE [dbo].[FactCovid] ADD CONSTRAINT [FK_FactCovid_DimCountries] 
		FOREIGN KEY ([CountryKey]) REFERENCES [dbo].[DimCountries] ([CountryKey])
	ALTER TABLE [dbo].[FactCovid] ADD CONSTRAINT [FK_FactCovid_DimRegions] 
		FOREIGN KEY ([RegionKey]) REFERENCES [dbo].[DimRegions] ([RegionKey])
	ALTER TABLE [dbo].[FactDebt] ADD CONSTRAINT [FK_FactDebt_DimDates] 
		FOREIGN KEY ([DateKey]) REFERENCES [dbo].[DimDates] ([DateKey])
	SET @Message = 'Foreign Keys replaced on all tables';
	EXEC sp_ETLInsertETLLog
 	       @ETLAction = 'sp_ETLAddForeignKeyConstraints'
 	      ,@ETLLogMessage = @Message;
    SET @RC = 1;
  END TRY
  BEGIN CATCH
    IF @@TRANCOUNT > 0 ROLLBACK;
    DECLARE @ErrorMessage NVARCHAR(1000) = Error_Message();
    EXEC sp_ETLInsertETLLog 
         @ETLAction = 'sp_ETLAddForeignKeyConstraints'
        ,@ETLLogMessage = @ErrorMessage;
    SET @RC = -1;
  END CATCH
  RETURN @RC;
END
GO

--EXEC sp_ETLAddForeignKeyConstraints;
--GO

--********************************************************************--
-- D) Review the results of this script
--********************************************************************--
--go
--Declare @Status int;
--Exec @Status = sp_ETLDropForeignKeyConstraints;
--Select [Object] = 'pETLDropForeignKeyConstraints', [Status] = @Status;

--Exec @Status = sp_ETLTruncateTables;
--Select [Object] = 'pETLTruncateTables', [Status] = @Status;

--Exec @Status = sp_ETLFillDimDates;
--Select [Object] = 'pETLFillDimDates', [Status] = @Status;

--Exec @Status = sp_ETLFillDimCountries;
--Select [Object] = 'pETLFillDimProducts', [Status] = @Status;

--Exec @Status = sp_ETLFillDimRegions;
--Select [Object] = 'pETLFillDimCustomers', [Status] = @Status;

--Exec @Status = sp_ETLFillFactCovid;
--Select [Object] = 'pETLFillFactOrders', [Status] = @Status;

--Exec @Status = sp_ETLAddForeignKeyConstraints;
--Select [Object] = 'pETLAddForeignKeyConstraints', [Status] = @Status;

--go
--Select [DimDates] = Count(*) From [dbo].[DimDates];
--Select [DimCountries] = Count(*) From [dbo].[DimCountrires];
--Select [DimRegions] = Count(*) From [dbo].[DimRegions];
--Select [FactCovid] = Count(*) From [dbo].[FactCovid];
--Select [FactDebt] = Count(*) From [dbo].[FactDebt];
--Select * From vETLLog;
----Delete From ETLLog;

-------------------------------------------------
------CREATE FINAL VIEWS FOR POWER BI------------

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLDimDatesView AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		EXEC('CREATE OR ALTER VIEW [dbo].[vDimDates]
		      AS
		      SELECT *
		      FROM [Gold_Debt].[dbo].[DimDates] WITH (NOLOCK)
			  ORDER BY [FullDate] ASC
			  OFFSET 0 ROWS');
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating ETL Final DimDates View For Our Power BI Report'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLDimDatesView;
--GO


USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLDimCountriesView AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		EXEC('CREATE OR ALTER VIEW [dbo].[vDimCountries]
		      AS
		      SELECT *
		      FROM [Gold_Debt].[dbo].[DimCountries] WITH (NOLOCK)');
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating ETL Final DimCountries View For Our Power BI Report'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLDimCountriesView;
--GO

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLDimRegionsView AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		EXEC('CREATE OR ALTER VIEW [dbo].[vDimRegions]
		      AS
		      SELECT *
		      FROM [Gold_Debt].[dbo].[DimRegions] WITH (NOLOCK)');
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating ETL Final DimRegions View For Our Power BI Report'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLDimRegionsView;
--GO

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLFactCovidView AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		EXEC('CREATE OR ALTER VIEW [dbo].[vFactCovid]
		      AS
		      SELECT *
		      FROM [Gold_Debt].[dbo].[FactCovid] WITH (NOLOCK)
			  ORDER BY [Updated] ASC
			  OFFSET 0 ROWS');
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating ETL Final FactCovid View For Our Power BI Report'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLFactCovidView;
--GO

USE [Gold_Debt]
GO

CREATE OR ALTER PROCEDURE sp_ETLFactDebtView AS
BEGIN
	DECLARE @RC INT = 0;
	BEGIN TRY
		EXEC('CREATE OR ALTER VIEW [dbo].[vFactDebt]
		      AS
		      SELECT *
		      FROM [Gold_Debt].[dbo].[FactDebt] WITH (NOLOCK)
			  ORDER BY [Record_Date] ASC
			  OFFSET 0 ROWS');
		SET @RC = 1;
	END TRY
	BEGIN CATCH
		PRINT 'Error Creating ETL Final FactDebt View For Our Power BI Report'
		PRINT ERROR_MESSAGE()
		SET @RC = -1
	END CATCH
END
GO

--EXEC sp_ETLFactDebtView;
--GO

--WE ARE SETTING UP DAILY SCHEDULED REFRESHES USING SQL SERVER AGENT! --THIS CODE IS NOT COPIED IN THE FIRST SEQUENCE CONTAINERS THAT RUNS CODE. WE RUN THIS AS A SEPARATE SQL SCRIPT IN THE FIRST AND THIRD SEQUENCE CONTAINER AS WELL AS THE LAST SEQUENCE CONTAINER "SQL SERVER AGENT SCHEDULE REFRESH & NEXT STEPS"
-------------------------------------------------------------------------------------------------------
--SQL SERVER AGENT SCHEDULED REFRESH FOR LOCAL INSTANCE
--DEPENDING ON WHICH SERVER YOU ARE ON, YOU WILL HAVE TO COMMENT ON AND OFF THE SQL SEVER AGENT BLOCKS OF CODE.


USE [msdb]
GO
--USE [Gold_Debt]
--GO 

--DECLARE @jobID1 Binary(16)
--SELECT @jobID1 = job_id FROM msdb.dbo.sysjobs WHERE name = N'Gold_Debt Daily Refresh'
--IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = N'Gold_Debt Daily Refresh') EXEC msdb.dbo.sp_delete_job @jobID1
--GO

CREATE OR ALTER PROCEDURE sp_Gold_Debt_SQLServerAgentDailyRefresh AS
BEGIN

/****** Object:  Job [Gold_Debt Daily Refresh]    Script Date: 5/27/2024 4:21:54 PM ******/
BEGIN TRANSACTION
DECLARE @ReturnCode INT
SELECT @ReturnCode = 0
DECLARE @jobID1 Binary(16)
SELECT @jobID1 = job_id FROM msdb.dbo.sysjobs WHERE name = N'Gold_Debt Daily Refresh'
IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = N'Gold_Debt Daily Refresh') EXEC msdb.dbo.sp_delete_job @jobID1

/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 5/27/2024 4:21:54 PM ******/
IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
BEGIN
EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

END

DECLARE @jobId BINARY(16)
EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Gold_Debt Daily Refresh', 
		@enabled=1, 
		@notify_level_eventlog=0, 
		@notify_level_email=0, 
		@notify_level_netsend=0, 
		@notify_level_page=0, 
		@delete_level=0, 
		@description=N'No description available.', 
		@category_name=N'[Uncategorized (Local)]', 
		@owner_login_name=N'DESKTOP-L3H8CVG\Felipe', @job_id = @jobId OUTPUT
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
/****** Object:  Step [Gold_Debt SSIS ETL Process]    Script Date: 5/27/2024 4:21:54 PM ******/
EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Gold_Debt SSIS ETL Process', 
		@step_id=1, 
		@cmdexec_success_code=0, 
		@on_success_action=1, 
		@on_success_step_id=0, 
		@on_fail_action=2, 
		@on_fail_step_id=0, 
		@retry_attempts=0, 
		@retry_interval=0, 
		@os_run_priority=0, @subsystem=N'SSIS', 
		@command=N'/FILE "\"C:\_BISolutions\Gold_Debt\Gold_Debt\Gold_Debt SSIS ETL Process.dtsx\"" /CHECKPOINTING OFF /REPORTING E', 
		@database_name=N'master', 
		@flags=0
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Gold_Debt SSIS ETL Process Daily Refresh', 
		@enabled=1, 
		@freq_type=4, 
		@freq_interval=1, 
		@freq_subday_type=1, 
		@freq_subday_interval=0, 
		@freq_relative_interval=0, 
		@freq_recurrence_factor=0, 
		@active_start_date=20240527, 
		@active_end_date=99991231, 
		@active_start_time=10000, 
		@active_end_time=235959, 
		@schedule_uid=N'6d3e1ef9-abc3-4a5d-88fa-1c3288595d7d'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
COMMIT TRANSACTION
GOTO EndSave
QuitWithRollback:
    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
EndSave:
END
GO

--EXEC sp_Gold_Debt_SQLServerAgentDailyRefresh;
--GO


------------------------------------------
--SQL SERVER AGENT SSIS DAILY ETL SCHEDULED RUN ON UW SERVER:
--DEPENDING ON WHICH SERVER YOU ARE ON, YOU WILL HAVE TO COMMENT ON AND OFF THE SQL SEVER AGENT BLOCKS OF CODE.




--USE [msdb]
--GO
----USE [Gold_Debt]
----GO 

----DECLARE @jobID1 Binary(16)
----SELECT @jobID1 = job_id FROM msdb.dbo.sysjobs WHERE name = N'Gold_Debt Daily Refresh'
----IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = N'Gold_Debt Daily Refresh') EXEC msdb.dbo.sp_delete_job @jobID1
----GO

--CREATE OR ALTER PROCEDURE sp_Gold_Debt_SQLServerAgentDailyRefresh AS
--BEGIN
--/****** Object:  Job [Gold_Debt Daily Refresh]    Script Date: 5/27/2024 3:48:22 PM ******/
--BEGIN TRANSACTION
--DECLARE @ReturnCode INT
--SELECT @ReturnCode = 0
--DECLARE @jobID1 Binary(16)
--SELECT @jobID1 = job_id FROM msdb.dbo.sysjobs WHERE name = N'Gold_Debt Daily Refresh'
--IF EXISTS (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = N'Gold_Debt Daily Refresh') EXEC msdb.dbo.sp_delete_job @jobID1

--/****** Object:  JobCategory [[Uncategorized (Local)]]    Script Date: 5/27/2024 3:48:23 PM ******/
--IF NOT EXISTS (SELECT name FROM msdb.dbo.syscategories WHERE name=N'[Uncategorized (Local)]' AND category_class=1)
--BEGIN
--EXEC @ReturnCode = msdb.dbo.sp_add_category @class=N'JOB', @type=N'LOCAL', @name=N'[Uncategorized (Local)]'
--IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback

--END

--DECLARE @jobId BINARY(16)
--EXEC @ReturnCode =  msdb.dbo.sp_add_job @job_name=N'Gold_Debt Daily Refresh', 
--		@enabled=1, 
--		@notify_level_eventlog=0, 
--		@notify_level_email=0, 
--		@notify_level_netsend=0, 
--		@notify_level_page=0, 
--		@delete_level=0, 
--		@description=N'No description available.', 
--		@category_name=N'[Uncategorized (Local)]', 
--		@owner_login_name=N'fdgomez', @job_id = @jobId OUTPUT
--IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
--/****** Object:  Step [Gold_Debt SSIS ETL Process]    Script Date: 5/27/2024 3:48:23 PM ******/
--EXEC @ReturnCode = msdb.dbo.sp_add_jobstep @job_id=@jobId, @step_name=N'Gold_Debt SSIS ETL Process', 
--		@step_id=1, 
--		@cmdexec_success_code=0, 
--		@on_success_action=1, 
--		@on_success_step_id=0, 
--		@on_fail_action=2, 
--		@on_fail_step_id=0, 
--		@retry_attempts=0, 
--		@retry_interval=0, 
--		@os_run_priority=0, @subsystem=N'SSIS', 
--		@command=N'/FILE "\"C:\_BISolutions\Gold_Debt\Gold_Debt\Gold_Debt SSIS ETL Process.dtsx\"" /CHECKPOINTING OFF /REPORTING E', 
--		@database_name=N'master', 
--		@flags=0
--IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
--EXEC @ReturnCode = msdb.dbo.sp_update_job @job_id = @jobId, @start_step_id = 1
--IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
--EXEC @ReturnCode = msdb.dbo.sp_add_jobschedule @job_id=@jobId, @name=N'Gold_Debt SSIS ETL Process Daily Refresh', 
--		@enabled=1, 
--		@freq_type=4, 
--		@freq_interval=1, 
--		@freq_subday_type=1, 
--		@freq_subday_interval=0, 
--		@freq_relative_interval=0, 
--		@freq_recurrence_factor=0, 
--		@active_start_date=20240527, 
--		@active_end_date=99991231, 
--		@active_start_time=10000, 
--		@active_end_time=235959, 
--		@schedule_uid=N'b0dc3b6b-5960-4780-8dae-8e6e15b26258'
--IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
--EXEC @ReturnCode = msdb.dbo.sp_add_jobserver @job_id = @jobId, @server_name = N'(local)'
--IF (@@ERROR <> 0 OR @ReturnCode <> 0) GOTO QuitWithRollback
--COMMIT TRANSACTION
--GOTO EndSave
--QuitWithRollback:
--    IF (@@TRANCOUNT > 0) ROLLBACK TRANSACTION
--EndSave:
--END
--GO

----EXEC sp_Gold_Debt_SQLServerAgentDailyRefresh;
----GO