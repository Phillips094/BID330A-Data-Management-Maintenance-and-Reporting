USE Gold_Debt
GO
IF OBJECT_ID('dbo.vDimDate', 'V') IS NOT NULL 
  DROP View dbo.vDimDate;
go

CREATE OR ALTER VIEW [dbo].[vDimDate]
AS
/************
*Developer: Jeremy Warren
*Date: 05/12/2024
*Description: Dim Date VIew
*	5/12: Created view
*************/
SELECT
* 
FROM [Gold_Debt].[dbo].[DimDate] WITH (NOLOCK)
go

Select * from vDimDate
