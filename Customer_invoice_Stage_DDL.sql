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
hadoop fs -ls /
hadoop fs -mkdir /user/hdfs/Testdata
hadoop fs -chmod 777 /user/hdfs/Testdata

hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice
hadoop fs -rm /user/hdfs/Testdata/Customer_Invoice/
--------------------------------------
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/customer
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/account
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/delievery_detail
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/financial_txn
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/invoice_line_item
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/invoice
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/order_item
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/order
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/product
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/product_type
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/shipping_detail
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/supplier_detail
hadoop fs -mkdir /user/hdfs/Testdata/Customer_Invoice/transaction_type

hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/customer
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/account
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/delievery_detail
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/financial_txn
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/invoice_line_item
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/invoice
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/order_item
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/order
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/product
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/product_type
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/shipping_detail
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/supplier_detail
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/transaction_type

hadoop fs -ls /user/hdfs/Testdata/Customer_Invoice/

--copy files From Local
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/CUSTOMERS /user/hdfs/Testdata/Customer_Invoice/customer
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/ACCOUNTS /user/hdfs/Testdata/Customer_Invoice/account
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/DELIEVERY_DETAILS /user/hdfs/Testdata/Customer_Invoice/delievery_detail
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/FINANCIAL_TXNS /user/hdfs/Testdata/Customer_Invoice/financial_txn
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/INVOICE_LINE_ITEMS /user/hdfs/Testdata/Customer_Invoice/invoice_line_item
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/INVOICES /user/hdfs/Testdata/Customer_Invoice/invoice
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/ORDER_ITEMS /user/hdfs/Testdata/Customer_Invoice/order_item
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/ORDERS /user/hdfs/Testdata/Customer_Invoice/order
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/PRODUCT /user/hdfs/Testdata/Customer_Invoice/product
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/PRODUCT_TYPE /user/hdfs/Testdata/Customer_Invoice/product_type
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/SHIPPING_DETAILS /user/hdfs/Testdata/Customer_Invoice/shipping_detail
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/SUPPLIER_DETAILS /user/hdfs/Testdata/Customer_Invoice/supplier_detail
hadoop fs -copyFromLocal /home/cloudera/Testing/Testdata/customer/TRANSACTION_TYPE /user/hdfs/Testdata/Customer_Invoice/transaction_type

hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/customer/CUSTOMERS
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/account/ACCOUNTS
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/delievery_detail/DELIEVERY_DETAILS
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/financial_txn/FINANCIAL_TXNS
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/invoice_line_item/INVOICE_LINE_ITEMS
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/invoice/INVOICES
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/order_item/ORDER_ITEMS
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/order/ORDERS
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/product/PRODUCT
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/product_type/PRODUCT_TYPE
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/shipping_detail/SHIPPING_DETAILS
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/supplier_detail/SUPPLIER_DETAILS
hadoop fs -chmod 777 /user/hdfs/Testdata/Customer_Invoice/transaction_type/TRANSACTION_TYPE

hadoop fs -ls /user/hdfs/Testdata/Customer_Invoice/

--Creating databases
CREATE DATABASE IF NOT EXISTS CUSTOMER_STG;
CREATE DATABASE IF NOT EXISTS CUSTOMER_CORE;

------------------------------------------------------------------------------------------------------------------
---Creating Hive external Table for Customers

--01--CUSTOMER

DROP TABLE CUSTOMER_STG.CUSTOMER;
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
location '/user/hdfs/Testdata/Customer_Invoice/customer/';
------------------------------------------------------------------------------------------------------------------
--02--ORDERS
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.ORDER (
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
location '/user/hdfs/Testdata/Customer_Invoice/order';

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
location '/user/hdfs/Testdata/Customer_Invoice/order_item';

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
location '/user/hdfs/Testdata/Customer_Invoice/product';

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
location '/user/hdfs/Testdata/Customer_Invoice/product_type';

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
location '/user/hdfs/Testdata/Customer_Invoice/invoice_line_item';

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
location '/user/hdfs/Testdata/Customer_Invoice/financial_txn';

------------------------------------------------------------------------------------------------------------------
--08--INVOICES
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.INVOICE (
INVOICE_NUM INT,
ORDER_ID INT,
INVOICE_DT INT,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/hdfs/Testdata/Customer_Invoice/invoice';

------------------------------------------------------------------------------------------------------------------
--09--ACCOUNTS

DROP TABLE CUSTOMER_STG.ACCOUNT;
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.ACCOUNT (
ACCOUNT_ID INT,
CUSTOMER_ID INT,
DATE_ACC_OPENED DATE,
ACC_NM STRING,
ACC_DTLS STRING,
EXPR_DT DATE,
START_DATE DATE,
END_DATE DATE		)
COMMENT 'Data about customer online purchases details'
ROW FORMAT DELIMITED  FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
location '/user/hdfs/Testdata/Customer_Invoice/account';

------------------------------------------------------------------------------------------------------------------
--10--SUPPLIER_DETAILS
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.SUPPLIER_DETAIL (
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
location '/user/hdfs/Testdata/Customer_Invoice/supplier_detail';

------------------------------------------------------------------------------------------------------------------
--11--DELIVERY_DETAILS
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.DELIVERY_DETAIL (
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
location '/user/hdfs/Testdata/Customer_Invoice/deliver_detail';

------------------------------------------------------------------------------------------------------------------
--12--SHIPPING_DETAILS
CREATE EXTERNAL TABLE IF NOT EXISTS CUSTOMER_STG.SHIPPING_DETAIL (
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
location '/user/hdfs/Testdata/Customer_Invoice/shipping_detail';

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
location '/user/hdfs/Testdata/Customer_Invoice/transaction_type';
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
select count(*) from customer
union all
select count(*) from account
union all
select count(*) from delivery_detail
union all
select count(*) from financial_txn
union all
select count(*) from invoice
union all
select count(*) from invoice_line_items
union all
select count(*) from order
union all
select count(*) from order_item
union all
select count(*) from product
union all
select count(*) from product_type
union all
select count(*) from shipping_detail
union all
select count(*) from supplier_detail
union all
select count(*) from transaction_type;

select * from customer;


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


