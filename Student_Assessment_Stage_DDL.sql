/*  
# Description: STUDENT_ASSESSMENT STAGING TABLES DDL STRUCTURES
# Created by: NANDAKUMAR PANDIAN
# Created on: 14/12/2016
# Revision History :
# Latest Modification History on top

# DD/MM/YYYY Who(Network id) Version Modification Details
# 14/12/2016        NANDAKUMAR PANDIAN      01         STUDENT_ASSESSMENT STAGING TABLES DDL STRUCTURES
*/

--Making directory 
--hadoop fs -mkdir /user/<username>/Testdata/Customer_Invoice
--hadoop fs -chmod 777 /user/<username>/Testdata/Customer_Invoice

hadoop fs -mkdir /user/<username>/Testdata/Student_Assessment
hadoop fs -chmod 777 /user/<username>/Testdata/Student_Assessment

--copy files From Local
--hadoop fs -copyFromLocal /cloudera/home/Testdata/Customer_Invoice/ /user/<username>/Testdata/Customer_Invoice
hadoop fs -copyFromLocal /cloudera/home/Testdata/Student_Assessment/ /user/<username>/Testdata/Student_Assessment

--Creating databases
--CREATE DATABASE IF NOT EXISTS CUSTOMER_STG;
--CREATE DATABASE IF NOT EXISTS CUSTOMER_CORE;

CREATE DATABASE IF NOT EXISTS STUDENT_STG;
CREATE DATABASE IF NOT EXISTS STUDENT_CORE;
------------------------------------------------------------------------------------------------------------------
---Creating Hive external Table for Student_Assessment

--01--STUDENT_ASSESSMENT
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.STUDENT_ASSESSMENT (
STUDENT_ID INT,
ASSESSMENT_ID INT,
EXPERIENCED_IND CHAR(1),
CRT_BY_NM STRING,
INS_DT DATE,
UPDT_DT DATE,
EXPR_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Student_Assessment';
------------------------------------------------------------------------------------------------------------------

--02--STUDENT
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.STUDENT (
STUDENT_ID INT,
ADDRESS_ID INT,
FIRST_NAME STRING,
MIDDLE_NAME STRING,
LAST_NAME STRING,
CELL_MOBILE_NUMBER STRING,
EMAIL_ADDRESS STRING,
DATE_FIRST_RENTAL DATE,
DATE_LEFT_UNIVERSITY DATE,
INS_DT DATE,
UPDT_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Student';
------------------------------------------------------------------------------------------------------------------

--03--ASSESSMENTS
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.ASSESSMENTS (
ASSESSMENT_ID  INT,
STUDENT_ID INT,
DATE_ASSMNT_START DATE,
DATE_ASSMNT_END DATE,
ASSMNT_SUMMARY STRING,
ASSMNT_SCORE BIGINT,
RECOMMENDATIONS STRING,
INS_DT DATE,
UPDT_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Assessments';
------------------------------------------------------------------------------------------------------------------

--04--ADDRESS
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.ADDRESS (
ASSESSMENT_ID INT,
LINE_1 STRING,
LINE_2 STRING,
LINE_3 STRING,
CITY STRING,
ZIP_POSTCODE BIGINT,
STATE STRING,
COUNTRY STRING,
INS_DT DATE,
UPDT_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Address';
------------------------------------------------------------------------------------------------------------------

--05--STAFF
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.STAFF (
STAFF_ID  INT,
ADDRESS_ID  INT,
DEPARTMENT_CD STRING,
FIRST_NAME STRING,
MIDDLE_NAME STRING,
LAST_NAME STRING,
GENDER CHAR(8)
PHONE STRING,
EMAIL STRING,
INS_DT DATE,
UPDT_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Staff';
------------------------------------------------------------------------------------------------------------------

--06--STAFF_ROLES
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.STAFF_ROLES (
DEPARTMENT_CODE  STRING,
DEPARTMENT_DESCRIPTION  STRING,
INS_DT DATE,
UPDT_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Staff_Roles';
------------------------------------------------------------------------------------------------------------------

--07--ACHIEVEMENT
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.ACHIEVEMENT (
ACHIEVEMENT_ID  INT,
STUDENT_ID  INT,
ACHIVE_TYP_CD CHAR(4),
DATE_ACHIVE DATE,
ACHIVE_DTLS STRING,
INS_DT DATE,
UPDT_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Achievement';
------------------------------------------------------------------------------------------------------------------

--08-ASSESSMENT_NOTES
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.ASSESSMENT_NOTES (
SESSION_ID   INT,
ASSESSMENT_ID   INT,
STAFF_ID   INT,
DATE_OF_SESSION DATE,
SESSION_NOTES STRING,
INS_DT DATE,
UPDT_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Assessment_Notes';
------------------------------------------------------------------------------------------------------------------

--09--ACHIEVEMENT_TYPE
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.ACHIEVEMENT_TYPE (
ACHIEVE_TYP_CD CHAR(4),
ACHIEVE_TYP_DESC STRING		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Achievement_Type';
------------------------------------------------------------------------------------------------------------------

--10--ACHIEVEMENT_COMMENTS
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.ACHIEVEMENT_COMMENTS (
ASSESSMENT_ID INT,
COMMENT_CD CHAR(4),
INS_DT DATE,
UPDT_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Achievement_Comments';
------------------------------------------------------------------------------------------------------------------

--11--COMMENTS_CATEGORY
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.COMMENTS_CATEGORY (
CATEGORY_CD CHAR(4),
CATEGORY_DESC  STRING		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Comments_Category';
------------------------------------------------------------------------------------------------------------------

--12--COMMENTS_CATEGORY
CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.COMMENTS_CATEGORY (
CATEGORY_CD CHAR(4),
CATEGORY_DESC  STRING		)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Student_Ass/Comments_Category';
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------



