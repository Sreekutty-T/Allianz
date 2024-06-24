-- Create the database
CREATE DATABASE pet_insurance;

-- Use the database
USE pet_insurance;

-- Create the claim_data table
CREATE TABLE claim_data (
    CLAIM_ID INT PRIMARY KEY
);


-- Load data into claim_data table
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Claim_data.csv'
INTO TABLE claim_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(CLAIM_ID);




-- Create the audit_status table
CREATE TABLE audit_status (
    CLAIM_ID INT,
    AUDIT_STATUS VARCHAR(50),
    PRIMARY KEY (CLAIM_ID, AUDIT_STATUS),
    FOREIGN KEY (CLAIM_ID) REFERENCES claim_data(CLAIM_ID)
);



-- Load data into audit_status table
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Audit_status.csv'
INTO TABLE audit_status
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(CLAIM_ID, AUDIT_STATUS);


-- Create a table to load the condition data
CREATE TABLE condition_data (
    CLAIM_ID INT,
    CONDITION_ID INT,
    CONDITION_MIGRATED_FLAG INT,
    CONDITION_TYPE_DESC VARCHAR(255),
    CONDITION_TYPE_CODE VARCHAR(10),
    CONDITION_TREATMENT_START_DATE VARCHAR(20),
    CONDITION_KNOWN_FROM_DATE VARCHAR(20),
    CONDITION_CLAIMED_AMOUNT DECIMAL(10, 2),
    CONDITION_NET_AMOUNT DECIMAL(10, 2),
    CONDITION_REJECTED_AMOUNT DECIMAL(10, 2),
    CONDITION_EXCESS_AMOUNT DECIMAL(10, 2)
);

-- Load data into the table
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\Condition_data.csv'
INTO TABLE condition_data
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 ROWS
(CLAIM_ID, CONDITION_ID, CONDITION_MIGRATED_FLAG, CONDITION_TYPE_DESC, CONDITION_TYPE_CODE, 
 @CONDITION_TREATMENT_START_DATE, @CONDITION_KNOWN_FROM_DATE, @CONDITION_CLAIMED_AMOUNT, @CONDITION_NET_AMOUNT, 
 @CONDITION_REJECTED_AMOUNT, @CONDITION_EXCESS_AMOUNT)
SET 
 CONDITION_TREATMENT_START_DATE = STR_TO_DATE(@CONDITION_TREATMENT_START_DATE, '%d-%m-%Y %H:%i'),
 CONDITION_KNOWN_FROM_DATE = STR_TO_DATE(@CONDITION_KNOWN_FROM_DATE, '%d-%m-%Y %H:%i'),
 CONDITION_CLAIMED_AMOUNT = CASE WHEN @CONDITION_CLAIMED_AMOUNT = '' THEN NULL ELSE @CONDITION_CLAIMED_AMOUNT END,
 CONDITION_NET_AMOUNT = CASE WHEN @CONDITION_NET_AMOUNT = '' THEN NULL ELSE @CONDITION_NET_AMOUNT END,
 CONDITION_REJECTED_AMOUNT = CASE WHEN @CONDITION_REJECTED_AMOUNT = '' THEN NULL ELSE @CONDITION_REJECTED_AMOUNT END,
 CONDITION_EXCESS_AMOUNT = CASE WHEN @CONDITION_EXCESS_AMOUNT = '' THEN NULL ELSE @CONDITION_EXCESS_AMOUNT END;


-- Disable safe update mode
SET SQL_SAFE_UPDATES = 0;

-- Re-enable safe update mode
SET SQL_SAFE_UPDATES = 1;


-- Check for missing values in critical fields (condition_data)
SELECT COUNT(*) AS Missing_Condition_IDs FROM condition_data WHERE CONDITION_ID IS NULL;
SELECT COUNT(*) AS Missing_Claim_IDs FROM condition_data WHERE CLAIM_ID IS NULL;
SELECT COUNT(*) AS Missing_Treatment_Start_Dates FROM condition_data WHERE CONDITION_TREATMENT_START_DATE IS NULL;
SELECT COUNT(*) AS Missing_Known_From_Dates FROM condition_data WHERE CONDITION_KNOWN_FROM_DATE IS NULL;

-- Alternate query
SELECT * FROM condition_data WHERE CLAIM_ID IS NULL OR CONDITION_ID IS NULL OR CONDITION_TREATMENT_START_DATE IS NULL OR CONDITION_KNOWN_FROM_DATE IS NULL;

-- Check for missing values in critical fields (audit_data)
SELECT * FROM audit_status WHERE CLAIM_ID IS NULL;


-- Check for missing values in critical fields (claim_data)
SELECT * FROM claim_data WHERE CLAIM_ID IS NULL;


-- Remove records with missing CONDITION_ID or CLAIM_ID
DELETE FROM Condition_data WHERE CONDITION_ID IS NULL OR CLAIM_ID IS NULL;


-- Check for Duplicates

SELECT CLAIM_ID, COUNT(*) FROM condition_data GROUP BY CLAIM_ID HAVING COUNT(*) > 1;
SELECT CLAIM_ID, COUNT(*) FROM audit_status GROUP BY CLAIM_ID HAVING COUNT(*) > 1;
SELECT CLAIM_ID, COUNT(*) AS duplicate_count FROM Claim_data GROUP BY CLAIM_ID HAVING COUNT(*) > 1;

 
 
 -- Remove duplicates from audit_data 
 
 DELETE FROM audit_status
WHERE (CLAIM_ID, AUDIT_STATUS) IN (
    SELECT CLAIM_ID, AUDIT_STATUS FROM (
        SELECT CLAIM_ID, AUDIT_STATUS, ROW_NUMBER() OVER (PARTITION BY CLAIM_ID ORDER BY CLAIM_ID) AS rnum
        FROM audit_status
    ) t
    WHERE t.rnum > 1
);


-- Remove duplicates from condition_data

CREATE TEMPORARY TABLE unique_condition_data AS
SELECT 
    CLAIM_ID,
    CONDITION_ID,
    CONDITION_MIGRATED_FLAG,
    CONDITION_TYPE_DESC,
    CONDITION_TYPE_CODE,
    CONDITION_TREATMENT_START_DATE,
    CONDITION_KNOWN_FROM_DATE,
    CONDITION_CLAIMED_AMOUNT,
    CONDITION_NET_AMOUNT,
    CONDITION_REJECTED_AMOUNT,
    CONDITION_EXCESS_AMOUNT
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY CLAIM_ID ORDER BY CLAIM_ID) AS rn
    FROM condition_data
) sub
WHERE rn = 1;


DELETE FROM condition_data;


INSERT INTO condition_data
SELECT 
    CLAIM_ID,
    CONDITION_ID,
    CONDITION_MIGRATED_FLAG,
    CONDITION_TYPE_DESC,
    CONDITION_TYPE_CODE,
    CONDITION_TREATMENT_START_DATE,
    CONDITION_KNOWN_FROM_DATE,
    CONDITION_CLAIMED_AMOUNT,
    CONDITION_NET_AMOUNT,
    CONDITION_REJECTED_AMOUNT,
    CONDITION_EXCESS_AMOUNT
FROM unique_condition_data;


DROP TEMPORARY TABLE unique_condition_data;


-- Verify if duplicates are removed
SELECT CLAIM_ID, COUNT(*) FROM condition_data GROUP BY CLAIM_ID HAVING COUNT(*) > 1;


-- Explanation
-- Identify Duplicates: The initial query helps you identify how many duplicates exist.
-- Create Temporary Table: The temporary table helps in isolating the unique records based on your specified criteria.
-- Delete Non-unique Records: This step ensures that all records not in the temporary table (i.e., duplicates) are removed.
-- Clean Up: Dropping the temporary table ensures no unnecessary data is left in the database.



-- Merge Claim_data with Audit_status to include the latest audit status for each claim:

CREATE TABLE Claim_with_Audit AS
SELECT c.*, a.AUDIT_STATUS
FROM Claim_data c
LEFT JOIN (
    SELECT CLAIM_ID, AUDIT_STATUS
    FROM Audit_status
    WHERE (CLAIM_ID, AUDIT_STATUS) IN (
        SELECT CLAIM_ID, MAX(AUDIT_STATUS)
        FROM Audit_status
        GROUP BY CLAIM_ID
    )
) a
ON c.CLAIM_ID = a.CLAIM_ID;

select * from Claim_with_Audit;

-- Check for duplicates
SELECT CLAIM_ID, COUNT(*) FROM Claim_with_Audit GROUP BY CLAIM_ID HAVING COUNT(*) > 1;


-- Merge the resulting dataset with Condition_data
CREATE TABLE Expanded_Claim_dataset AS
SELECT 
    c.CLAIM_ID AS claim_id,
    c.AUDIT_STATUS,
    cd.CONDITION_ID,
    cd.CONDITION_MIGRATED_FLAG,
    cd.CONDITION_TYPE_DESC,
    cd.CONDITION_TYPE_CODE,
    cd.CONDITION_TREATMENT_START_DATE,
    cd.CONDITION_KNOWN_FROM_DATE,
    cd.CONDITION_CLAIMED_AMOUNT,
    cd.CONDITION_NET_AMOUNT,
    cd.CONDITION_REJECTED_AMOUNT,
    cd.CONDITION_EXCESS_AMOUNT
FROM 
    Claim_with_Audit c
LEFT JOIN 
    Condition_data cd
ON 
    c.CLAIM_ID = cd.CLAIM_ID;



-- Verify the expanded dataset
SELECT * FROM Expanded_Claim_dataset;




-- Check for duplicates
SELECT CLAIM_ID, COUNT(*) FROM expanded_claim_dataset GROUP BY CLAIM_ID HAVING COUNT(*) > 1;



-- check the null count for all columns

SELECT
    SUM(CASE WHEN claim_id IS NULL THEN 1 ELSE 0 END) AS null_count_claim_id,
    SUM(CASE WHEN CONDITION_ID IS NULL THEN 1 ELSE 0 END) AS null_count_condition_id,
    SUM(CASE WHEN CONDITION_MIGRATED_FLAG IS NULL THEN 1 ELSE 0 END) AS null_count_condition_migrated_flag,
    SUM(CASE WHEN CONDITION_TYPE_DESC IS NULL THEN 1 ELSE 0 END) AS null_count_condition_type_desc,
    SUM(CASE WHEN CONDITION_TYPE_CODE IS NULL THEN 1 ELSE 0 END) AS null_count_condition_type_code,
    SUM(CASE WHEN CONDITION_TREATMENT_START_DATE IS NULL THEN 1 ELSE 0 END) AS null_count_condition_treatment_start_date,
    SUM(CASE WHEN CONDITION_KNOWN_FROM_DATE IS NULL THEN 1 ELSE 0 END) AS null_count_condition_known_from_date,
    SUM(CASE WHEN CONDITION_CLAIMED_AMOUNT IS NULL THEN 1 ELSE 0 END) AS null_count_condition_claimed_amount,
    SUM(CASE WHEN CONDITION_NET_AMOUNT IS NULL THEN 1 ELSE 0 END) AS null_count_condition_net_amount,
    SUM(CASE WHEN CONDITION_REJECTED_AMOUNT IS NULL THEN 1 ELSE 0 END) AS null_count_condition_rejected_amount,
    SUM(CASE WHEN CONDITION_EXCESS_AMOUNT IS NULL THEN 1 ELSE 0 END) AS null_count_condition_excess_amount,
    SUM(CASE WHEN AUDIT_STATUS IS NULL THEN 1 ELSE 0 END) AS null_count_audit_status
FROM 
    Expanded_Claim_dataset;

    
DESCRIBE Expanded_Claim_dataset;
    
-- Filling Null Values with Zero

UPDATE Expanded_Claim_dataset
SET 
    claim_id = IFNULL(claim_id, 0),
    CONDITION_ID = IFNULL(CONDITION_ID, 0),
    CONDITION_MIGRATED_FLAG = IFNULL(CONDITION_MIGRATED_FLAG, 0),
    CONDITION_TYPE_DESC = IFNULL(CONDITION_TYPE_DESC, 'Unknown'),
    CONDITION_TYPE_CODE = IFNULL(CONDITION_TYPE_CODE, 'Unknown'),
    CONDITION_TREATMENT_START_DATE = IFNULL(CONDITION_TREATMENT_START_DATE, '0000-00-00'),
    CONDITION_KNOWN_FROM_DATE = IFNULL(CONDITION_KNOWN_FROM_DATE, '0000-00-00'),
    CONDITION_CLAIMED_AMOUNT = IFNULL(CONDITION_CLAIMED_AMOUNT, 0),
    CONDITION_NET_AMOUNT = IFNULL(CONDITION_NET_AMOUNT, 0),
    CONDITION_REJECTED_AMOUNT = IFNULL(CONDITION_REJECTED_AMOUNT, 0),
    CONDITION_EXCESS_AMOUNT = IFNULL(CONDITION_EXCESS_AMOUNT, 0),
    AUDIT_STATUS = IFNULL(AUDIT_STATUS, 'Unknown');


-- Filter Conditions Known Before Treatment Date:

CREATE TABLE Filtered_Claim_dataset AS
SELECT *
FROM Expanded_Claim_dataset
WHERE CONDITION_KNOWN_FROM_DATE < CONDITION_TREATMENT_START_DATE;

select * from Filtered_Claim_dataset;


-- Presenting Monthly Time Series
SELECT 
    DATE_FORMAT(CONDITION_TREATMENT_START_DATE, '%Y-%m') AS month,
    SUM(CONDITION_CLAIMED_AMOUNT) AS total_claimed_amount
FROM Filtered_Claim_dataset
WHERE CONDITION_TREATMENT_START_DATE >= '2023-01-01' 
AND CONDITION_TREATMENT_START_DATE <= '2024-05-31'
GROUP BY DATE_FORMAT(CONDITION_TREATMENT_START_DATE, '%Y-%m')
ORDER BY month;

