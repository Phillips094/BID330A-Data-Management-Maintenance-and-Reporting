USE Gold_Debt
GO


-----Drop SPROC if it exists
--IF EXISTS (SELECT * FROM sys.objects WHERE type = 'P' AND name = 'sp_FactDebt')
--DROP PROCEDURE [dbo].[sp_FactDebt]
--GO


---Create SPROC
CREATE OR ALTER Procedure [dbo].[sp_FactDebt]
AS
/************
*Developer: Jeremy Warren
*Date: 05/12/2024
*Description: Dim Date Sproc
*	5/12: Created Sproc
*************/

---Drop Table
DROP TABLE IF EXISTS [dbo].[FactDebtOld]

---Create table
Create table [dbo].[FactDebtOld]
(
       fact_debt_key int identity (1,1)
	  ,Record_Date Date
	  ,Public_Debt float
      ,Intragovernmental_Holdings float
      ,Total_Public_Debt_Outstanding float
      ,Source_Line_Number int
      ,Fiscal_Year int
      ,Fiscal_Quarter_Number int
      ,Calendar_Year int
      ,Calendar_Quarter_Number int
      ,Calendar_Month_Number int
      ,Calendar_Day_Number int
 )

 ---Perform insert

 Insert into [dbo].[FactDebtOld]
 Select

       [Record_Date]
	  ,[Debt_Held_by_the_Public]
      ,[Intragovernmental_Holdings]
      ,[Total_Public_Debt_Outstanding]
      ,[Source_Line_Number]
      ,[Fiscal_Year]
      ,[Fiscal_Quarter_Number]
      ,[Calendar_Year]
      ,[Calendar_Quarter_Number]
      ,[Calendar_Month_Number]
      ,[Calendar_Day_Number]
	  from [dbo].[DebtPenny_Raw]
 go   

 --exec dbo.sp_FactDebt;
 --go