/*********
*Developer: Tim Pauley
*Date: 05/16/2021
*SQL Quiz Question 2
*Walk-through of SQL Programming
*https://www.youtube.com/watch?v=EPUayNC5ku4
********/

USE Mod7_Quiz;
GO

--Advanced SQL Questions

--This is a Microsoft / Facebook Question
--STEPS TO SOLVE Complex SQL Problem
--1: Explore Data
--2: List Assumptions
--3: Outline Apporach
--4: Code in Increments
--5: Optimize Code

--New and Exisiting Users
/*Caluclate the the share of new and existing users. Output the month, share of new users
*as a ratio. 
*NEW USERS: are defined as users who started using services in the current month.
*EXISTING USERS: are users who started using services in the current month and used
*services in any previous month
*ASSUME: that the dates are all from the year 2020
*/

IF OBJECT_ID('dbo.fact_events', 'u') IS NOT NULL 
  DROP TABLE dbo.fact_events

CREATE TABLE fact_events
(
	id INT   
	,time_id date
	,[user_id] nvarchar(50)
	,customer_id nvarchar(50)
	,client_id nvarchar(50)
	,event_type nvarchar(50)
	,event_id INT
)
/**********INSERT DATA****************/
INSERT INTO fact_events
SELECT
	CAST([id] as INT) --Cast from string
	, CAST([time_id] as date)  --Cast from string
	, [user_id]
	, [customer_id]
	, [client_id]
	, [event_type]
	, CAST([event_id] as INT)  --Cast from string
FROM [dbo].[fact_events_csv] --I used a CSV file to export this

SELECT * FROM fact_events

--Step 1: Explore data

--Step 2: List Assumptions
--1: Time_id: indicates when the user is using the service. All of the data is 
--	from 2020. This will help identify how new and existing users are delcared
--2: User_id: is all we need to identify users
--3: Each time an user uses the service, it's in the table so users
--	are listed multiple time in the table
--4: Event_type: is the servie but that isn't needed for the solution since we
--	are considering all services or events

--Step 3: Outline your approach (1 logical step or 1 business rule)
--1: Find the new users which are defined as users that have started using services for the first time.
--	I can use min() to find the first date a user service.
--2: Calculate all the users that have used services by month. This will give us exsiting users once we
--	subtract out the new users
--3: Join the new users and all users table by month
--4: Calculate the shares by dividing the count of new users by the count of all users. Caculating the share
--	of exisiting users is merely the differnce between 1 and the share of new users

--Step 4: Code In Increments :)

--Part 1: White a query to identify new users or MIN()


/***EXEPECTED OUTPUT
user_id	new_user_start_date
0280-XJGEX	2020-02-03
1452-KIOVK	2020-02-12
3655-SNQYZ	2020-02-09
3668-QPYBK	2020-02-06
4183-MYFRB	2020-03-25
4190-MFLUW	2020-03-02
5129-JLPIS	2020-02-20
5575-GNVDE	2020-03-01
6388-TABGU	2020-03-02
6713-OKOMC	2020-02-01
7469-LKBCI	2020-02-03
7590-VHVEG	2020-02-07
7795-CFOCW	2020-02-03
7892-POOKP	2020-02-04
8091-TTVAX	2020-03-06
8191-XWSZG	2020-02-17
9237-HQITU	2020-02-03
9305-CDSKC	2020-02-04
9763-GRSKD	2020-04-02
9959-WOFKT	2020-04-01
********************/
WITH [NU] AS (
SELECT [User_id] = [E].[user_id]
     , [New_User_Time] = MIN([E].[time_id])
FROM [Mod7_Quiz].[dbo].[fact_events] [E]
GROUP BY [E].[user_id]
)
SELECT [User_id]
      ,[New_User_Time]
FROM [NU]

--Part 2: Write a Sub Query to identify existing users
WITH [NU] AS (
SELECT [User_id] = [E].[user_id]
     , [New_User_Time] = MIN([E].[time_id])
FROM [Mod7_Quiz].[dbo].[fact_events] [E]
GROUP BY [E].[user_id]
)
SELECT [NU_Month] = MONTH([New_User_Time])
     , [NU_Count] = COUNT(DISTINCT [User_id])
FROM [NU]
GROUP BY MONTH([New_User_Time])

/***EXPECTED OUTPUT
month	new_users
2	13
3	5
4	2
****************/

--Part 3: Make both of them CTE's (Should be ran together 3 & 4)

/********NEW USERS**********/
WITH [NU] AS (
SELECT [User_id] = [E].[user_id]
     , [New_User_Time] = MIN([E].[time_id])
FROM [Mod7_Quiz].[dbo].[fact_events] [E]
GROUP BY [E].[user_id]
)
,
[ANU] AS (
SELECT [ANU_Month] = MONTH([E].[time_id])
     , [ANU_Count] = COUNT(DISTINCT [E].[user_id])
FROM [Mod7_Quiz].[dbo].[fact_events] [E]
GROUP BY MONTH([E].[time_id])
)

SELECT [NU_Month] = MONTH([New_User_Time])
     , [NU_Count] = COUNT(DISTINCT [NU].[User_id])
	 , [ANU_Count] = [ANU].[ANU_Count]
FROM [NU]
INNER JOIN [ANU] ON MONTH([NU].[New_User_Time]) = [ANU].[ANU_Month]
GROUP BY MONTH([New_User_Time])
       , [ANU].[ANU_Count]

--Part 4: 
WITH [NU] AS (
SELECT [User_id] = [E].[user_id]
     , [New_User_Time] = MIN([E].[time_id])
FROM [Mod7_Quiz].[dbo].[fact_events] [E]
GROUP BY [E].[user_id]
)
,
[ANU] AS (
SELECT [ANU_Month] = MONTH([E].[time_id])
     , [ANU_Count] = COUNT(DISTINCT [E].[user_id])
FROM [Mod7_Quiz].[dbo].[fact_events] [E]
GROUP BY MONTH([E].[time_id])
)

SELECT [NU_Month] = MONTH([New_User_Time])
     , [NU_Count] = COUNT(DISTINCT [NU].[User_id])
	 , [ANU_Count] = [ANU].[ANU_Count]
	 , CONVERT(DECIMAL(10,2), (COUNT([NU].[User_id]) / [ANU].[ANU_Count]))
FROM [NU]
INNER JOIN [ANU] ON MONTH([NU].[New_User_Time]) = [ANU].[ANU_Month]
GROUP BY MONTH([New_User_Time])
       , [ANU].[ANU_Count]
ORDER BY [NU_Month] ASC

/***Expected Output
month	share_new_users	Percentage_Change
2	1	0
3	0	1
4	0	1
********/


