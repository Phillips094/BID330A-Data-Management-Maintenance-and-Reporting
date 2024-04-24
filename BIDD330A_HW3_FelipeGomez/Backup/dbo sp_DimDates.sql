-- ================================================
-- Template generated from Template Explorer using:
-- Create Procedure (New Menu).SQL
--
-- Use the Specify Values for Template Parameters 
-- command (Ctrl-Shift-M) to fill in the parameter 
-- values below.
--
-- This block of comments will not be included in
-- the definition of the procedure.
-- ================================================
USE [Gold_Felipe]
GO

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
-- =============================================
-- Author:		Felipe Gomez
-- Create date: 04/16/2024
-- Description:	ETL Process for Dim Dates
-- =============================================
CREATE PROCEDURE [dbo].[sp_DimDates] AS
BEGIN

--DROP TABLE IF EXISTS [dbo].[DimDates]

DECLARE @StartDate  date = '20200101'; --Covid cases started roughly this date
		
		--declare when to stop
		DECLARE @CutoffDate date = GETDATE(); --We could stop when the dataset stopped refreshing. Perhaps this could be an update
		
		--create table
		DROP TABLE IF EXISTS DimDates
		CREATE TABLE DimDates
		(
		    TheDate date
		    ,TheDay int
		    ,TheDayName nvarchar(100)
		    ,TheWeek  int
		    ,TheISOWeek int
		    ,TheDayOfWeek int
		    ,TheMonth int
		    ,TheMonthName nvarchar(100)
		    ,TheQuarter int
		    ,TheYear int
		    ,TheFirstOfMonth date
		    ,TheLastOfYear date
		    ,TheDayOfYear int
		)
		
		--write code to get all the useable calendar 
		;WITH seq(n) AS 
		(
		  SELECT 0 UNION ALL SELECT n + 1 FROM seq
		  WHERE n < DATEDIFF(DAY, @StartDate, @CutoffDate)
		),
		d(d) AS 
		(
		  SELECT DATEADD(DAY, n, @StartDate) FROM seq
		),
		src AS
		(
		  SELECT
		    TheDate         = CONVERT(date, d),
		    TheDay          = DATEPART(DAY,       d),
		    TheDayName      = DATENAME(WEEKDAY,   d),
		    TheWeek         = DATEPART(WEEK,      d),
		    TheISOWeek      = DATEPART(ISO_WEEK,  d),
		    TheDayOfWeek    = DATEPART(WEEKDAY,   d),
		    TheMonth        = DATEPART(MONTH,     d),
		    TheMonthName    = DATENAME(MONTH,     d),
		    TheQuarter      = DATEPART(Quarter,   d),
		    TheYear         = DATEPART(YEAR,      d),
		    TheFirstOfMonth = DATEFROMPARTS(YEAR(d), MONTH(d), 1),
		    TheLastOfYear   = DATEFROMPARTS(YEAR(d), 12, 31),
		    TheDayOfYear    = DATEPART(DAYOFYEAR, d)
		  FROM d
		)
		
		INSERT INTO DimDates
		SELECT * FROM src
		  ORDER BY TheDate
		  OPTION (MAXRECURSION 0);

END
GO
