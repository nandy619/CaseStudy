/*  
# Description: CUSTOMER_INVOICE STAGING TABLES DDL STRUCTURES
# Created by: NANDAKUMAR PANDIAN
# Created on: 14/12/2016
# Revision History :
# Latest Modification History on top

# DD/MM/YYYY Who(Network id) Version Modification Details
# 14/12/2016        NANDAKUMAR PANDIAN      01         CUSTOMER_INVOICE STAGING TABLES DDL STRUCTURES
*/

--Making directory 
hadoop fs -mkdir /user/<username>/Testdata/Customer_Invoice
hadoop fs -chmod 777 /user/<username>/Testdata/Customer_Invoice

--hadoop fs -mkdir /user/<username>/Testdata/Student_Assessment
--hadoop fs -chmod 777 /user/<username>/Testdata/Student_Assessment

--copy files From Local
hadoop fs -copyFromLocal /cloudera/home/Testdata/Customer_Invoice/ /user/<username>/Testdata/Customer_Invoice
--hadoop fs -copyFromLocal /cloudera/home/Testdata/Student_Assessment/ /user/<username>/Testdata/Student_Assessment

--Creating databases
CREATE DATABASE IF NOT EXISTS CUSTOMER_STG;
CREATE DATABASE IF NOT EXISTS CUSTOMER_CORE;

--CREATE DATABASE IF NOT EXISTS STUDENT_STG;
--CREATE DATABASE IF NOT EXISTS STUDENT_CORE;

------------------------------------------------------------------------------------------------------------------
---Creating Hive external Table for Customers

--01--CUSTOMER
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.CUSTOMER (
CUSTOMER_ID INT,
CUSTOMER_FIRST_NAME STRING,
CUSTOMER_MIDDLE_NAME STRING,
CUSTOMER_LAST_NAME STRING,
GENDER CHAR(6),
EMAIL_ADDRESS STRING,
LOGIN_NAME STRING,
LOGIN_PASSWORD STRING,
PHONE_NUM INT,
ADDRESS_LINE_1 STRING,
ADDRESS_LINE_2 STRING,
ADDRESS_LINE_3 STRING,
ADDRESS_LINE_4 STRING,
TOWN_CITY STRING,
STATE STRING,
COUNTRY STRING,
COUNTRY_CODE CHAR(3),
CURR_IND CHAR(1),
EXPIRED_DATE DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Customer';
------------------------------------------------------------------------------------------------------------------
--02--ORDERS
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.ORDERS (
ORDER_ID INT,
CUSTOMER_ID INT,
ORDER_ITEM STRING,
VENDOR_NAME STRING,
DATE_ORDER_PLACED DATE,
EXPECTED_DEL_DATE DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Orders';

------------------------------------------------------------------------------------------------------------------
--03--ORDER_ITEM
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.ORDER_ITEM (
ORDER_ITEM_ID INT,
ORDER_ID INT,
ORDER_ITEM STRING,
PRODUCT_ID  INT,
PRODUCT_QTY VARCHAR(8),
EXPR_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Order_Item';

------------------------------------------------------------------------------------------------------------------
--04--PRODUCT
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.PRODUCT (
PRODUCT_ID INT,
SUPPLIER_ID INT,
PRODUCT_TYPE_CD CHAR(4),
PRODUCT_NAME STRING,
PRODUCT_PRICE FLOAT,
PRODUCT_COLOR STRING,
PRODUCT_SIZE CHAR(4),
PRODUCT_DESC STRING,
PRODUCT_AVAILABILITY CHAR(1),
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Product';

------------------------------------------------------------------------------------------------------------------
--05--PRODUCT_TYPE
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.PRODUCT_TYPE (
PRODUCT_TYPE_CD  CHAR(4),
PARENT_PRD_TYP_CD  CHAR(4),
PRD_TYP_DESC STRING,
VAT_RATING CHAR(1),
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Product_Type';

------------------------------------------------------------------------------------------------------------------
--06--INVOICE_LINE_ITEMS
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.INVOICE_LINE_ITEMS (
ORDER_ITEM_ID INT,
INVOICE_NUM INT,
PRODUCT_ID INT,
SHIPPING_ID INT,
PRODUCT_TITLE STRING,
PRODUCT_QTY VARCHAR(8),
DERIVED_PRD_COST FLOAT,
DERIVED_VAT_PYBL FLOAT,
DERIVED_TOT_COST FLOAT,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Invoice_line_items';

------------------------------------------------------------------------------------------------------------------
--07--FINANCIAL_TXN
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.FINANCIAL_TXN (
TRANSACTION_ID INT,
ACCOUNT_ID INT,
INVOICE_NUM INT,
TRANS_TYP_CD CHAR(4),
TRANS_DT DATE,
TRANS_AMT FLOAT,
TRANS_COMMENTS STRING,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Financial_txn';

------------------------------------------------------------------------------------------------------------------
--08--INVOICES
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.INVOICES (
INVOICE_NUM INT,
ORDER_ID INT,
INVOICE_DT INT,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Invoice';

------------------------------------------------------------------------------------------------------------------
--09--ACCOUNTS
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.ACCOUNTS (
ACCOUNT_ID INT,
CUSTOMER_ID INT,
DATE_ACC_OPENED DATE,
ACC_NM INT,
ACC_DTLS STRING,
EXPR_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Account';

------------------------------------------------------------------------------------------------------------------
--10--SUPPLIER_DETAILS
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.SUPPLIER_DETAILS (
SUPPLIER_ID INT,
SUPPLIED_BY STRING,
SUPPLIED_ITEMS STRING,
SUPPLIED_DT_TIME TIMESTAMP,
SUPPLIER_STATUS CHAR(1),
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Supplier_Details';

------------------------------------------------------------------------------------------------------------------
--11--DELIVERY_DETAILS
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.DELIVERY_DETAILS (
DELIVER_ID INT,
PRODUCT_ID INT,
CUSTOMER_ID INT,
DELIVERED_BY STRING,
DELIEVERY_ITEMS STRING,
DELIEVERY_DATE_TIME TIMESTAMP,
DELIEVERY_STATUS CHAR(1),
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Deliver_Details';

------------------------------------------------------------------------------------------------------------------
--12--SHIPPING_DETAILS
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.SHIPPING_DETAILS (
SHIPPING_ID INT,
DELIVER_ID INT,
SHIPPING_TYP_CD CHAR(4),
SHIPPING_TYP_NM STRING,
MODE_OF_SHIPPING STRING,
COURIER_NM STRING,
SHIPPING_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Shipping_Details';

------------------------------------------------------------------------------------------------------------------
--13--TRANSACTION_TYPE
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.TRANSACTION_TYPE (
TRANS_TYP_CD CHAR(4),
TRANS_TYP_DESC STRING,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/<username>/Testdata/Customer_Invoice/Transaction_Type';

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE TABLE mytable ( 
         name string,
         city string,
         employee_id int ) 
PARTITIONED BY (year STRING, month STRING, day STRING) 
CLUSTERED BY (employee_id) INTO 256 BUCKETS

CREATE TABLE new_key_value_store
   ROW FORMAT SERDE "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe"
   STORED AS RCFile
   AS
SELECT (key % 1024) new_key, concat(key, value) key_value_pair
FROM key_value_store
SORT BY new_key, key_value_pair;

CREATE TABLE new_test
row format delimited fields terminated by '|' 
STORED AS RCFile 
AS select * from source where col=1

CREATE TABLE myFlightInfo2008 AS
SELECT Year, Month, DepTime, ArrTime, FlightNum, Origin, Dest FROM FlightInfo2008
WHERE (Month = 7 AND DayofMonth = 3) AND (Origin='JFK' AND Dest='ORD');

CREATE [EXTERNAL] TABLE [IF NOT EXISTS] db_name.]table_name
  [COMMENT 'table_comment']
  [WITH SERDEPROPERTIES ('key1'='value1', 'key2'='value2', ...)]
  [
   [ROW FORMAT row_format] [STORED AS ctas_file_format]
  ]
  [LOCATION 'hdfs_path']
  [TBLPROPERTIES ('key1'='value1', 'key2'='value2', ...)]
  [CACHED IN 'pool_name' [WITH REPLICATION = integer] | UNCACHED]
  
   CREATE TABLE T (key int, value string) 
 PARTITIONED BY (ds string, hr int) AS
 SELECT key, value, ds, hr+1 hr1 
   FROM srcpart 
   WHERE ds is not null 
   And hr>10;
   
   SET hive.exec.dynamic.partition.mode=nonstrict;

CREATE TABLE T (key int, value string) 
PARTITIONED BY (ds string, hr int);

INSERT OVERWRITE TABLE T PARTITION(ds, hr) 
SELECT key, value, ds, hr+1 AS hr 
   FROM srcpart 
   WHERE ds is not null 
   And hr>10;


