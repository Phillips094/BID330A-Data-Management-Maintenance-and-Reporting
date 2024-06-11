USE Gold_Debt
GO



CREATE OR ALTER Procedure [dbo].[sp_DimDate]
AS
/************
*Developer: Jeremy Warren
*Date: 05/12/2024
*Description: Dim Date Sproc
*	5/12: Created Sproc
*************/

---Drop Table
DROP TABLE IF EXISTS [dbo].[DimDate]

-----Create table
Create Table DimDate
(
	 TheDate date
	, TheDay int
	, TheDayName nvarchar (100)
	, TheWeek int
	, TheISOWeek int
	, TheDayOfWeek int
	, TheMonth int
	, TheMonthName nvarchar (100)
	, TheQuarter int
	, TheYear int
	, TheFirstOfMonth date
	, TheLastOfYear date
	, TheDayOfYear int

)


---Perform Insert
Insert into dbo.DimDate
Select
      TheDate
	, TheDay
	, TheDayName
	, TheWeek
	, TheISOWeek
	, TheDayOfWeek
	, TheMonth
	, TheMonthName
	, TheQuarter
	, TheYear
	, TheFirstOfMonth
	, TheLastOfYear
	, TheDayOfYear
   from [Black_Unemployment].[dbo].[DimDate]
go
