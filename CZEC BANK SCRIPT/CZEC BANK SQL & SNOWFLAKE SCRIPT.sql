CREATE DATABASE BANK;
USE BANK;

CREATE TABLE DISTRICT(
District_Code INT PRIMARY KEY	,
District_Name VARCHAR(100)	,
Region VARCHAR(100)	,
No_of_inhabitants	INT,
No_of_municipalities_with_inhabitants_less_499 INT,
No_of_municipalities_with_inhabitants_500_btw_1999	INT,
No_of_municipalities_with_inhabitants_2000_btw_9999	INT,
No_of_municipalities_with_inhabitants_less_10000 INT,	
No_of_cities	INT,
Ratio_of_urban_inhabitants	FLOAT,
Average_salary	INT,
No_of_entrepreneurs_per_1000_inhabitants	INT,
No_committed_crime_2017	INT,
No_committed_crime_2018 INT
) ;

CREATE TABLE ACCOUNT(
account_id INT PRIMARY KEY,
district_id	INT,
frequency	VARCHAR(40),
`Date` DATE ,
Account_type VARCHAR(40),
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);

CREATE TABLE `ORDER`(
order_id	INT PRIMARY KEY,
account_id	INT,
bank_to	VARCHAR(45),
account_to	INT,
amount FLOAT,
FOREIGN KEY (account_id) references ACCOUNT(account_id)
);



CREATE TABLE LOAN(
loan_id	INT ,
account_id	INT,
`Date`	DATE,
amount	INT,
duration	INT,
payments	INT,
`status` VARCHAR(35),
FOREIGN KEY (account_id) references `ACCOUNT`(account_id)
);

CREATE TABLE TRANSACTIONS(
trans_id INT,	
account_id	INT,
`Date`	DATE,
`Type`	VARCHAR(30),
operation	VARCHAR(40),
amount	INT,
balance	FLOAT,
Purpose	VARCHAR(40),
bank	VARCHAR(45),
`account` INT,
FOREIGN KEY (account_id) references ACCOUNT(account_id));


CREATE TABLE CLIENT(
client_id	INT PRIMARY KEY,
Sex	CHAR(10),
Birth_date	DATE,
district_id INT,
FOREIGN KEY (district_id) references DISTRICT(District_Code) 
);

CREATE TABLE DISPOSITION(
disp_id	INT PRIMARY KEY,
client_id INT,
account_id	INT,
`type` CHAR(15),
FOREIGN KEY (account_id) references ACCOUNT(account_id),
FOREIGN KEY (client_id) references CLIENT(client_id)
);

CREATE TABLE CARD(
card_id	INT PRIMARY KEY,
disp_id	INT,
`type` CHAR(10)	,
issued DATE,
FOREIGN KEY (disp_id) references DISPOSITION(disp_id)
);

---------------------------------------------------------------------------------------------

CREATE OR REPLACE STORAGE integration s3_int
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::441615131317:role/bankrole'
STORAGE_ALLOWED_LOCATIONS =('s3://czechbankdata/');

DESC integration s3_int;


CREATE OR REPLACE STAGE BANK
URL ='s3://czechbankdata'
file_format = CSV
storage_integration = s3_int;

LIST @BANKS;

SHOW STAGES;

--CREATE SNOWPIPE THAT RECOGNISES CSV THAT ARE INGESTED FROM EXTERNAL STAGE AND COPIES THE DATA INTO EXISTING TABLE

--The AUTO_INGEST=true parameter specifies to read 
--- event notifications sent from an S3 bucket to an SQS queue when new data is ready to load.


CREATE OR REPLACE PIPE BANK_SNOWPIPE_DISTRICT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."DISTRICT" --yourdatabase -- your schema ---your table
FROM '@BANKS/District/' --s3 bucket subfolde4r name
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_ACCOUNT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."ACCOUNT"
FROM '@BANKS/Account/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_TXNS AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."TRANSACTIONS"
FROM '@BANKS/Trnx/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_DISP AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."DISPOSITION"
FROM '@BANKS/disp/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_CARD AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."CARD"
FROM '@BANKS/Card/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_ORDER_LIST AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."ORDER_LIST"
FROM '@BANKS/Order/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_LOAN AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."LOAN"
FROM '@BANKS/Loan/'
FILE_FORMAT = CSV;

CREATE OR REPLACE PIPE BANK_SNOWPIPE_CLIENT AUTO_INGEST = TRUE AS
COPY INTO "BANK"."PUBLIC"."CLIENT"
FROM '@BANKS/Client/'
FILE_FORMAT = CSV;

SHOW PIPES;

SELECT count(*) FROM DISTRICT;
SELECT count(*) FROM ACCOUNT;
SELECT count(*) FROM TRANSACTIONS;
SELECT count(*) FROM DISPOSITION;
SELECT count(*) FROM CARD;
SELECT count(*) FROM ORDER_LIST;
SELECT count(*) FROM LOAN;
SELECT count(*) FROM CLIENT;

ALTER PIPE BANK_SNOWPIPE_DISTRICT refresh;

ALTER PIPE BANK_SNOWPIPE_ACCOUNT refresh;

ALTER PIPE BANK_SNOWPIPE_TXNS refresh;

ALTER PIPE BANK_SNOWPIPE_DISP refresh;

ALTER PIPE BANK_SNOWPIPE_CARD refresh;

ALTER PIPE BANK_SNOWPIPE_ORDER_LIST refresh;

ALTER PIPE BANK_SNOWPIPE_LOAN refresh;

ALTER PIPE BANK_SNOWPIPE_CLIENT refresh;
--------------------------------------------------------------------------------------------------------------------------------------
SELECT * FROM DISTRICT;
SELECT * FROM ACCOUNT;
SELECT * FROM TRANSACTIONS;
SELECT * FROM DISPOSITION;
SELECT * FROM CARD;
SELECT * FROM ORDER_LIST;
SELECT * FROM LOAN;
SELECT * FROM CLIENT;


ALTER TABLE ACCOUNT                 -- Adding Custom column in account table
ADD COLUMN CARD_Assign VARCHAR;

SELECT * FROM ACCOUNT;

UPDATE ACCOUNT                    -- Adding the categories data in column
SET Card_assign = 
    CASE
        WHEN FREQUENCY = 'MONTHLY ISSUANCE' THEN 'SILVER'
        WHEN FREQUENCY = 'WEEKLY ISSUANCE' THEN 'GOLD'
        WHEN FREQUENCY = 'DAILY ISSUANCE' THEN 'DIAMOND'
        ELSE 'OTHER'
    END;


select min(`date`),max(`date`) from transactions; --check start-to-end of date transactions 

SELECT YEAR(`DATE`) AS TXN_YEAR, COUNT(*) AS TOT_TXNS --Checking the total count of transactions based on years
FROM TRANSACTIONS
WHERE BANK IS NULL
GROUP BY 1
ORDER BY 2 DESC;

UPDATE transactions
SET `date` = DATEADD(year, 6, `date`)
WHERE YEAR(`date`) = 2016;

UPDATE transactions
SET `date` = DATEADD(year, 4, `date`) -- Adjusted to add 4 years, as 2017 should become 2021
WHERE YEAR(`date`) = 2017;

UPDATE transactions
SET `date` = DATEADD(year, 2, `date`) -- Adjusted to add 2 years, as 2018 should become 2020
WHERE YEAR(`date`) = 2018;

-- No update needed for 2019 as it remains unchanged

UPDATE transactions
SET `date` = DATEADD(year, -2, `date`) -- Adjusted to subtract 2 years, as 2020 should become 2018
WHERE YEAR(`date`) = 2020;

UPDATE transactions
SET `date` = DATEADD(year, -4, `date`) -- Adjusted to subtract 4 years, as 2021 should become 2017
WHERE YEAR(`date`) = 2021;
;


-- update the trasnsaction with requirements
update transactions
set bank = 'sky bank 'where bank is null and year(`date`) = '2022';

update transactions
set bank = 'DBS Bank' where bank is null and year(`date`) = '2021';

update transactions
set bank = 'Northen Bank' where bank is null and year(`date`) = '2019';

update transactions
set bank = 'Southern Bank' where bank is null and year(`date`) = '2018';

update transactions
set bank = 'Adb Bank' where bank is null and year(`date`) = '2017';



select distinct (sex)from client group by 1 order by 1 desc;



1. --find how many male and female clients are there 
CREATE TABLE MALE_AND_FEMALE_CLIENTS AS
SELECT SUM(CASE WHEN upper(sex) = 'MALE' THEN 1 ELSE 0 END) AS MALE_CLIENTS,
SUM(CASE WHEN upper(sex)= 'FEMALE' THEN 1 ELSE 0 END) AS FEMALE_CLIENTS
FROM CLIENT;

2. --find the precentage of male and female clients
CREATE TABLE PERCENTAGE_OF_MALE_AND_FEMALE_CLIENTS AS
SELECT SUM(CASE WHEN upper(sex) = 'MALE' THEN 1 ELSE 0 END)/count(*)*100.0 AS MALE_CLIENTS,
SUM(CASE WHEN upper(sex)= 'FEMALE' THEN 1 ELSE 0 END)/count(*)*100.0 AS FEMALE_CLIENTS
FROM CLIENT;

3. --Add age column to client tabel and update there age date function

ALTER TABLE CLIENT
ADD COLUMN Age int

UPDATE CLIENT
SET AGE = DATEDIFF('YEAR',BIRTH_DATE,'2022-12-19');

select * from client;


4. --FIND EVERY LAST MONTH TRANSACTION OF CUSTOMER ACCOUNT

CREATE OR REPLACE TABLE ACC_LATEST_TXNS_WITH_BALANCE AS

SELECT LTD.*,TXN.BALANCE
FROM TRANSACTIONS AS TXN
INNER JOIN 
(
   SELECT ACCOUNT_ID,YEAR(`DATE`) AS TXN_YEAR,
   MONTH(`DATE`) AS TXN_MONTH,
   MAX(`DATE`) AS LATEST_TXN_DATE
   FROM TRANSACTIONS
   GROUP BY 1,2,3
   ORDER BY 1,2,3
) AS LTD ON TXN.ACCOUNT_ID = LTD.ACCOUNT_ID AND TXN.`DATE` = LTD.LATEST_TXN_DATE
WHERE TXN."`TYPE`" = 'Credit'            -- this is the assumptions Im having : month end txn data is credit
ORDER BY TXN.ACCOUNT_ID,LTD.TXN_YEAR,LTD.TXN_MONTH;
    
SELECT * FROM ACC_LATEST_TXNS_WITH_BALANCE;

SELECT LATEST_TXN_DATE,COUNT(*) AS TOTAL_TXNS
FROM ACC_LATEST_TXNS_WITH_BALANCE
GROUP BY 1
ORDER BY 1;

SELECT * FROM acc_latest_txns_with_balance;;

5. -- CREATE AND ESSENTIAL KEY MEASURES FOR BANK FOR ANALYSIS 
--Creating banking kpis for BANK
CREATE OR REPLACE TABLE BANKING_KPIS AS
SELECT  ALWB.TXN_YEAR, ALWB.TXN_MONTH,T.BANK,A.ACCOUNT_TYPE,ALWB.ACCOUNT_ID, 
COUNT(DISTINCT ALWB.ACCOUNT_ID) AS TOT_ACCOUNT, 
COUNT(DISTINCT T.TRANS_ID) AS TOT_TXNS,
COUNT(CASE WHEN T."`TYPE`" = 'Credit' THEN 1 END) AS DEPOSIT_COUNT,
COUNT(CASE WHEN T."`TYPE`" = 'Withdrawal' THEN 1 END) AS WITHDRAWAL_COUNT,
SUM(ALWB.BALANCE) AS TOT_BALANCE,
ROUND((DEPOSIT_COUNT / TOT_TXNS) * 100, 2) AS DEPOSIT_PERC,
ROUND((WITHDRAWAL_COUNT / TOT_TXNS) * 100, 2) AS WITHDRAWAL_PERC,
NVL(TOT_BALANCE / TOT_ACCOUNT, 0) AS AVG_BALANCE,
ROUND(TOT_TXNS / TOT_ACCOUNT, 0) AS TPA
FROM TRANSACTIONS AS T
INNER JOIN ACC_LATEST_TXNS_WITH_BALANCE AS ALWB ON T.ACCOUNT_ID = ALWB.ACCOUNT_ID
LEFT OUTER JOIN ACCOUNT AS A ON T.ACCOUNT_ID = A.ACCOUNT_ID
GROUP BY ALWB.TXN_YEAR, ALWB.TXN_MONTH, T.BANK, A.ACCOUNT_TYPE, ALWB.ACCOUNT_ID 
ORDER BY ALWB.TXN_YEAR, ALWB.TXN_MONTH, T.BANK, A.ACCOUNT_TYPE;


6. --What is the demographic profile of the bank's clients and how does it vary across districts?
CREATE TABLE Czechoslovaki_Bank_Demographic_Profile as

Select C.DISTRICT_ID ,D.DISTRICT_NAME,D.AVERAGE_SALARY,ROUND(AVG(C.AGE),0)AS AVG_AGE,
SUM(CASE WHEN upper(sex) = 'MALE' THEN 1 ELSE 0 END) AS MALE_CLIENTS,
SUM(CASE WHEN upper(sex)= 'FEMALE' THEN 1 ELSE 0 END) AS FEMALE_CLIENTS,
ROUND((FEMALE_CLIENTS/MALE_CLIENTS)*100,2) AS MALE_FEMALE_PER_RATIO,
COUNT(*) AS TOTAL_CLINEST 
FROM CLIENT C
INNER JOIN DISTRICT D ON C.DISTRICT_ID = D.DISTRICT_CODE
GROUP BY 1,2,3
ORDER BY 1;

SELECT * FROM CZECHOSLOVAKI_BANK_DEMOGRAPHIC_PROFILE;


7. -- How the banks have performed over the years. Give their detailed analysis year & month-wise.

CREATE TABLE BANK_PERFROMANCE_OVER_YEAR_AND_MONTH_WISE AS
SELECT  TXN_YEAR AS Year,TXN_MONTH AS Month,BANK AS Bank,
TOT_ACCOUNT AS Total_Accounts,
TOT_TXNS AS Total_Transactions,
DEPOSIT_COUNT AS Total_Deposits,
WITHDRAWAL_COUNT AS Total_Withdrawals,
TOT_BALANCE AS Total_Balance,
DEPOSIT_PERC AS Deposit_Percentage,
WITHDRAWAL_PERC AS Withdrawal_Percentage,
AVG_BALANCE AS Average_Balance,
TPA AS Transactions_Per_Account
FROM BANKING_KPIS
ORDER BY  TXN_YEAR DESC, TXN_MONTH DESC, BANK;

SELECT * FROM BANK_PERFROMANCE_OVER_YEAR_AND_MONTH_WISE;


8. --What are the most common types of accounts and how do they differ in terms of usage and profitability? 
SELECT  A.ACCOUNT_TYPE, 
COUNT(DISTINCT A.ACCOUNT_ID) AS Total_Accounts, 
SUM(ALWB.BALANCE) AS Total_Balance, 
AVG(ALWB.BALANCE) AS Average_Balance,
-- Calculating the deposit percentage (based on 'Credit' transactions)
ROUND(SUM(CASE WHEN T.`TYPE` = 'Credit' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS Deposit_Percentage,
-- Calculating the withdrawal percentage (based on 'Withdrawal' transactions)
ROUND(SUM(CASE WHEN T.`TYPE` = 'Withdrawal' THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS Withdrawal_Percentage,
-- Profitability metrics:
-- Calculate the average transaction per account (TPA)
ROUND(COUNT(DISTINCT T.TRANS_ID) / NULLIF(COUNT(DISTINCT A.ACCOUNT_ID), 0), 0) AS TPA,
-- Calculating the average balance per account
ROUND(SUM(ALWB.BALANCE) / NULLIF(COUNT(DISTINCT A.ACCOUNT_ID), 0), 2) AS Avg_Balance_Per_Account
FROM ACCOUNT AS A
JOIN TRANSACTIONS AS T ON A.ACCOUNT_ID = T.ACCOUNT_ID
JOIN ACC_LATEST_TXNS_WITH_BALANCE AS ALWB ON A.ACCOUNT_ID = ALWB.ACCOUNT_ID
GROUP BY  A.ACCOUNT_TYPE
ORDER BY Total_Balance DESC;  -- Sorting based on total balance to highlight the most profitable account types


9. --Which types of cards are most frequently used by the bank's clients and what is the overall profitability of the credit card business? 

-- 1. FIND MOST USING CARD TYPE 
SELECT  
    C.`TYPE`, 
    COUNT(DISTINCT T.TRANS_ID) AS TOTAL_TRANSACTIONS,
    COUNT(DISTINCT T.ACCOUNT_ID) AS TOTAL_ACCOUNTS
FROM TRANSACTIONS AS T
INNER JOIN ACCOUNT AS A ON T.ACCOUNT_ID = A.ACCOUNT_ID
JOIN DISPOSITION AS D ON A.ACCOUNT_ID = D.ACCOUNT_ID
JOIN CARD AS C ON D.DISP_ID = C.DISP_ID
GROUP BY C."`TYPE`"
ORDER BY TOTAL_TRANSACTIONS DESC;

-- 2. NOW Calculate Overall Profitability of the Credit Card Business
SELECT SUM(CASE WHEN BKI.TOT_BALANCE > 300000 THEN BKI.TOT_BALANCE * 0.05  -- Interest for balances > 300,000
WHEN BKI.TOT_BALANCE BETWEEN 150000 AND 300000 THEN BKI.TOT_BALANCE * 0.03  -- Interest for balances between 150,000 and 300,000
ELSE BKI.TOT_BALANCE * 0.01  -- Interest for balances below 150,000
END) AS TOTAL_INTEREST_INCOME,
SUM(CASE WHEN T.`TYPE` = 'Credit' THEN 1 ELSE 0 END) * 1.5 AS TRANSACTION_FEES,  -- Assuming a fee of 1.5 per transaction
SUM(BKI.TOT_BALANCE) * 0.05 AS OPERATIONAL_COSTS,  -- Assuming 5% of balance as operational costs
SUM(BKI.TOT_BALANCE) * 0.02 AS LOAN_LOSSES,  -- Assuming 2% of balance as loan losses
    
-- 3. Calculate the overall profitability 
-- FORMULA: Profitability = (Interest Income + Transaction Fees - Operational Costs - Loan Losses)
(SUM(CASE WHEN BKI.TOT_BALANCE > 300000 THEN BKI.TOT_BALANCE * 0.05
WHEN BKI.TOT_BALANCE BETWEEN 150000 AND 300000 THEN BKI.TOT_BALANCE * 0.03
ELSE BKI.TOT_BALANCE * 0.01
END) + SUM(CASE WHEN T.`TYPE` = 'Credit' THEN 1 ELSE 0 END) * 1.5 - SUM(BKI.TOT_BALANCE) * 0.05 - SUM(BKI.TOT_BALANCE) * 0.02) AS PROFITABILITY
FROM TRANSACTIONS AS T
INNER JOIN ACCOUNT AS A ON T.ACCOUNT_ID = A.ACCOUNT_ID
JOIN DISPOSITION AS D ON A.ACCOUNT_ID = D.ACCOUNT_ID
JOIN CARD AS C ON D.DISP_ID = C.DISP_ID
JOIN BANKING_KPIS AS BKI ON A.ACCOUNT_ID = BKI.ACCOUNT_ID
GROUP BY C."`TYPE`";


10. --What are the major expenses of the bank and how can they be reduced to improve profitability? 

CREATE TABLE BANK_MAJOR_EXPENSES AS
SELECT 'Loan Expenses' AS EXPENSE_TYPE,  -- Label for loan expenses
"`STATUS`" AS loan_status,
SUM(AMOUNT) AS TOTAL_EXPENSE
FROM LOAN  
WHERE "`STATUS`" = 'Loan not payed'  -- Filter for loans not paid
GROUP BY "`STATUS`"  -- Group by loan status

UNION ALL

SELECT 'Transaction Expenses' AS EXPENSE_TYPE,  -- Label for transaction expenses
"`TYPE`" AS trasns_type,  -- Transaction type (Credit or Withdrawal)
SUM(AMOUNT) AS TOTAL_EXPENSE  -- Total transaction amount as expense
FROM TRANSACTIONS
WHERE "`TYPE`" IN ('Credit', 'Withdrawal')  -- Filter for credit and withdrawal transactions
GROUP BY "`TYPE`" -- Group by transaction type

UNION ALL

SELECT 'Interest Paid Expense' AS EXPENSE_TYPE,  -- Label for interest paid expense
'Interest' AS interest_label,  -- Fixed label for interest expense
SUM(CASE WHEN BKI.TOT_BALANCE > 300000 THEN BKI.TOT_BALANCE * 0.05  -- Interest for balances > 300,000
WHEN BKI.TOT_BALANCE BETWEEN 150000 AND 300000 THEN BKI.TOT_BALANCE * 0.03  -- Interest for balances between 150,000 and 300,000
ELSE BKI.TOT_BALANCE * 0.01  -- Interest for balances below 150,000
END) AS TOTAL_EXPENSE  -- Calculated interest as an expense
FROM BANKING_KPIS AS BKI  -- Assuming BANKING_KPIS holds the balance information for accounts
GROUP BY 'Interest'  -- Single group for interest expenses (since it's a fixed label)
ORDER BY TOTAL_EXPENSE DESC;


-- Query to calculate major expenses and potential reduction strategies
SELECT EXPENSE_TYPE,loan_status AS STATUS, trasns_type as transactions_type,
    
-- Calculating total expenses for each category
SUM(TOTAL_EXPENSE) AS TOTAL_EXPENSE,
    
-- Calculating the percentage of total expense for each category
ROUND((SUM(TOTAL_EXPENSE) / (SELECT SUM(TOTAL_EXPENSE) FROM BANK_MAJOR_EXPENSES) ) * 100, 2) AS PERCENTAGE_OF_TOTAL_EXPENSE,

-- For Loan Expenses: Example strategy to reduce
CASE 
    WHEN EXPENSE_TYPE = 'Loan Expenses' THEN 'Improve collection efforts, restructure loans, incentivize early repayments'
    ELSE NULL
    END AS LOAN_EXPENSE_STRATEGY,

-- For Transaction Expenses: Example strategy to reduce
CASE 
    WHEN EXPENSE_TYPE = 'Transaction Expenses' THEN 'Encourage digital transactions, automate processing, optimize fees'
    ELSE NULL
    END AS TRANSACTION_EXPENSE_STRATEGY,

-- For Interest Paid Expenses: Example strategy to reduce
CASE 
    WHEN EXPENSE_TYPE = 'Interest Paid Expenses' THEN 'Optimize deposit rates, target higher-value customers'
    ELSE NULL
    END AS INTEREST_EXPENSE_STRATEGY

FROM BANK_MAJOR_EXPENSES
GROUP BY EXPENSE_TYPE, loan_status
ORDER BY TOTAL_EXPENSE DESC;
