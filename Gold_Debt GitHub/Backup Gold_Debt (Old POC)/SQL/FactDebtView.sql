---Create View from FactDebt
/************
*Developer: Jeremy Warren
*Date: 05/12/2024
*Description: Fact Date VIew
*	5/12: Created view
*************/


USE Gold_Debt
GO
IF OBJECT_ID('dbo.vFactDebt', 'V') IS NOT NULL 
  DROP View dbo.vFactDebt;
go

CREATE OR ALTER VIEW [dbo].[vFactDebt]
AS

SELECT
* 
FROM [Gold_Debt].[dbo].[FactDebtOld] WITH (NOLOCK)
go

Select * from vFactDebt