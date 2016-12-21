/*  
# Description: Student_AssessmenttESSMENT STAGING TABLES DDL STRUCTURES
# Created by: NANDAKUMAR PANDIAN
# Created on: 14/12/2016
# Revision History :
# Latest Modification History on top
# DD/MM/YYYY Who(Network id) Version Modification Details
# 14/12/2016        NANDAKUMAR PANDIAN      01         Student_AssessmenttESSMENT STAGING TABLES DDL STRUCTURES
*/

--Making directory 
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment

--------------------------------------
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/student_assessment
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/student
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/assessment
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/address
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/staff
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/staff_role
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/achievement
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/assessment_note
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/achievement_type
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/achievement_comment
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/comment
hadoop fs -mkdir /user/hdfs/Testdata/Student_Assessment/comment_category

hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/Student_Assessment
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/student
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/assessment
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/address
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/staff
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/staff_role
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/achievement
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/assessment_note
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/achievement_type
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/achievement_comment
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/comment
hadoop fs -chmod 777 /user/hdfs/Testdata/Student_Assessment/comment_category

hadoop fs -ls /user/hdfs/Testdata/Student_Assessment/

--copy files From Local
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/STUDENT_ASSESSMENT /user/hdfs/Testdata/Student_Assessment/student_assessment
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/STUDENT /user/hdfs/Testdata/Student_Assessment/student
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/ASSESSMENTS /user/hdfs/Testdata/Student_Assessment/assessment
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/ADDRESS /user/hdfs/Testdata/Student_Assessment/address
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/STAFF /user/hdfs/Testdata/Student_Assessment/staff
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/STAFF_ROLES /user/hdfs/Testdata/Student_Assessment/staff_role
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/ACHIEVMENT /user/hdfs/Testdata/Student_Assessment/achievement
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/ASSESSMENT_NOTES /user/hdfs/Testdata/Student_Assessment/assessment_note
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/ACHIEVMENT_TYPE /user/hdfs/Testdata/Student_Assessment/achievement_type
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/ACHIEVMENT_COMMENTS /user/hdfs/Testdata/Student_Assessment/achievement_comment
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/COMMENTS /user/hdfs/Testdata/Student_Assessment/comment
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/student/COMMENTS_CATEGORY /user/hdfs/Testdata/Student_Assessment/comment_category

hadoop fs -rm /user/hdfs/Testdata/Student_Assessment/staff_role/
hadoop fs -rmdir /user/hdfs/Testdata/Student_Assessment/achievment_comment
--Creating databases
CREATE DATABASE IF NOT EXISTS STUDENT_STG;
CREATE DATABASE IF NOT EXISTS STUDENT_CORE;
------------------------------------------------------------------------------------------------------------------
---Creating Hive external Table for Student_Assessmenttessment

--01--Student_AssessmenttESSMENT

DROP TABLE IF  EXISTS STUDENT_STG.STUDENT_ASSESSMENT;

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
location '/user/hdfs/Testdata/Student_Assessment/student_assessment';
------------------------------------------------------------------------------------------------------------------

--02--STUDENT

DROP TABLE IF  EXISTS STUDENT_STG.STUDENT;

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
location '/user/hdfs/Testdata/Student_Assessment/student';
------------------------------------------------------------------------------------------------------------------

--03--ASSESSMENTS

DROP TABLE IF  EXISTS STUDENT_STG.ASSESSMENTS;

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
location '/user/hdfs/Testdata/Student_Assessment/assessment';
------------------------------------------------------------------------------------------------------------------

--04--ADDRESS

DROP TABLE IF  EXISTS STUDENT_STG.ADDRESS;

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
location '/user/hdfs/Testdata/Student_Assessment/address';
------------------------------------------------------------------------------------------------------------------

--05--STAFF

DROP TABLE IF  EXISTS STUDENT_STG.STAFF;

CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.STAFF (
STAFF_ID  INT,
ADDRESS_ID  INT,
DEPARTMENT_CD STRING,
FIRST_NAME STRING,
MIDDLE_NAME STRING,
LAST_NAME STRING,
GENDER CHAR(8),
PHONE STRING,
EMAIL STRING,
INS_DT DATE,
UPDT_DT DATE,
START_DATE DATE,
END_DATE DATE		 )
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/hdfs/Testdata/Student_Assessment/staff';
------------------------------------------------------------------------------------------------------------------

--06--STAFF_ROLES

DROP TABLE IF  EXISTS STUDENT_STG.STAFF_ROLES;

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
location '/user/hdfs/Testdata/Student_Assessment/staff_role';
------------------------------------------------------------------------------------------------------------------

--07--ACHIEVEMENT

DROP TABLE IF  EXISTS STUDENT_STG.ACHIEVEMENT;

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
location '/user/hdfs/Testdata/Student_Assessment/achievement';
------------------------------------------------------------------------------------------------------------------

--08-ASSESSMENT_NOTES

DROP TABLE IF  EXISTS STUDENT_STG.ASSESSMENT_NOTES;

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
location '/user/hdfs/Testdata/Student_Assessment/assessment_note';
------------------------------------------------------------------------------------------------------------------

--09--ACHIEVEMENT_TYPE

DROP TABLE IF  EXISTS STUDENT_STG.ACHIEVEMENT_TYPE;

CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.ACHIEVEMENT_TYPE (
ACHIEVE_TYP_CD CHAR(4),
ACHIEVE_TYP_DESC STRING	)
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/hdfs/Testdata/Student_Assessment/achievement_type';
------------------------------------------------------------------------------------------------------------------

--10--ACHIEVEMENT_COMMENTS

DROP TABLE IF  EXISTS STUDENT_STG.ACHIEVEMENT_COMMENTS;

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
location '/user/hdfs/Testdata/Student_Assessment/achievement_comment';
------------------------------------------------------------------------------------------------------------------

--11--COMMENT

DROP TABLE IF  EXISTS STUDENT_STG.COMMENT;

CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.COMMENT (
COMMENT_CD CHAR(4),
CATEGORY_CD CHAR(4),
CATEGORY_DESC  STRING )
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/hdfs/Testdata/Student_Assessment/comment';
------------------------------------------------------------------------------------------------------------------

--12--COMMENTS_CATEGORY

DROP TABLE IF  EXISTS STUDENT_STG.COMMENTS_CATEGORY;

CREATE EXTERNAL TABLE IF NOT EXISTS STUDENT_STG.COMMENTS_CATEGORY (
CATEGORY_CD CHAR(4),
CATEGORY_DESC  STRING )
COMMENT 'Data about student assessment details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/hdfs/Testdata/Student_Assessment/comment_category';
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select * from student;