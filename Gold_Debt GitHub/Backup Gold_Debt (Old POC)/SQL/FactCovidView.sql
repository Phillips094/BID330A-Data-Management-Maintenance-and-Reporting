---Create View from FactCovid Table
/************
*Developer: Jeremy Warren
*Date: 05/12/2024
*Description: Dim Date VIew
*	5/12: Created view
*************/


USE Gold_Debt
GO
IF OBJECT_ID('dbo.vFactCovid', 'V') IS NOT NULL 
  DROP View dbo.vFactCovid;
go

CREATE OR ALTER VIEW [dbo].[vFactCovid]
AS

SELECT
* 
FROM [Gold_Debt].[dbo].[FactCovidOld] WITH (NOLOCK)
go

Select * from vFactCovid
