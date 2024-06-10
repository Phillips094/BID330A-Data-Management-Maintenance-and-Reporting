/*********
*Developer: Tim Pauley
*Date: 05/16/2021
*SQL Quiz Question 1
*Walk-through of SQL Programming
*https://www.youtube.com/watch?v=EPUayNC5ku4 - note the answer is slightly different
*due to the limitation of row count
********/

USE Mod7_Quiz;
GO

--Advanced SQL Questions

--STEPS TO SOLVE Complex SQL Problem
--1: Explore Data
--2: List Assumptions
--3: Outline Apporach
--4: Code in Increments
--5: Optimize Code

--Question 1. Percentage of Total Spend

/*
*PERCENTAGE OF TOTAL SPEND
*Find the percentage of the total spend a csutomer speant on each order. Output the
*customers first name, order details, and percentage of total spend for each order
*transaction rounded to the nearest whole numner. Assume each customer has a unique
*first name (ie., there is only 1 customer named Karen in this dataset)
*/ 

IF OBJECT_ID('dbo.Orders', 'u') IS NOT NULL 
  DROP TABLE dbo.Orders
IF OBJECT_ID('dbo.Customers', 'u') IS NOT NULL 
  DROP TABLE dbo.Customers

CREATE TABLE Orders
(
	id INT IDENTITY(1,1)  
	,cust_id INT
	,order_date DATE
	,order_quantity INT
	,order_details nvarchar(50)
	,order_cost DECIMAL
)

CREATE TABLE Customers
(
	id INT  
	,first_name nvarchar(50)
	,last_name nvarchar(50)
	,city nvarchar(50)
	,[address] nvarchar(50)
	,phone_number nvarchar(50)
)


INSERT INTO Customers 
(id,first_name,last_name, city,[address],phone_number)
VALUES
(8, 'John', 'Joseph', 'Arizona', '', 928-386-81634)
,(7, 'Jill', 'Micheal', 'Flordia', '', 206-979-7285)
,(4, 'William', 'Daniel', 'Colorado', '', 928-386-81634)
,(5, 'Henry', 'Jackson', 'Hawaii', '', 928-386-81634)
,(13, 'Emma', 'Issac', 'Hawaii', '', 928-386-81634)
,(14, 'Laim', 'Samuel', 'Hawaii', '', 928-386-81634)
,(15, 'Mia', 'Owen', 'Hawaii', '', 928-386-81634)
,(1, 'Mark', 'Thoma', 'Arizona', '4476 {arkway Drive', 928-386-81634)

SELECT * FROM Customers

INSERT INTO Orders 
(cust_id, order_date, order_quantity,order_details,order_cost)
VALUES
(3, '2019-03-04', 1, 'Coat', 100)
,(3, '2019-03-01', 1, 'Shoes', 80)
,(3, '2019-03-10', 1, 'Skirt', 30)
,(7, '2019-02-01', 1, 'Coat', 100)
,(7, '2019-03-10', 1, 'Shoes', 80)
,(15, '2019-02-01', 2, 'Boats', 100)
,(15, '2019-01-11', 3, 'Shirts', 60)
,(15, '2019-03-11', 1, 'Slipper', 20)
,(15, '2019-03-01', 2, 'Jeans', 80)
,(15, '2019-03-09', 3, 'Shirts', 50)

SELECT * FROM Orders

--Write the query

--Step 1: joins the orders with customers using an inner join
--Step 2: sum the total amount spent by each customers
--Step 3: find the % total spent (order cost / Sum(Order cost) by cusomter)
--Step 4: round(*100)

/***EXPECTED OUTPUT
*first_name	last_name	order_details	% total spent
Jill	Micheal	Coat	55
Jill	Micheal	Shoes	44
Mia	Owen	Boats	32
Mia	Owen	Shirts	19
Mia	Owen	Slipper	6
Mia	Owen	Jeans	25
Mia	Owen	Shirts	16
*
*****/


WITH [CS] AS (
SELECT [C].[id]
     , [CustSum] = SUM([O].[order_cost])
FROM [Mod7_Quiz].[dbo].[Customers] [C]
INNER JOIN [Mod7_Quiz].[dbo].[Orders] [O] ON [O].[cust_id] = [C].[id]
GROUP BY [C].[id]
)

SELECT [C].[first_name]
	 , [C].[last_name]
	 , [O].[order_details]
	 , [% total spent] = CONVERT(DECIMAL(18,2), ROUND([O].[order_cost] / [CS].[CustSum], 2))
FROM [Mod7_Quiz].[dbo].[Orders] [O]
INNER JOIN [Mod7_Quiz].[dbo].[Customers] [C] ON [O].[cust_id] = [C].[id]
LEFT JOIN [CS] ON [C].[id] = [CS].[id]
GROUP BY [C].[first_name]
		,[C].[last_name]
		,[O].[order_details]
		,CONVERT(DECIMAL(18,2), ROUND([O].[order_cost] / [CS].[CustSum], 2))