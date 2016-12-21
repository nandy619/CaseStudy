/*  
# Description: TWITTER LIVE STREAMING STAGE LOAD BY USING FLUME
# Created by: NANDAKUMAR PANDIAN
# Created on: 19/12/2016
# Revision History :
# Latest Modification History on top

# DD/MM/YYYY Who(Network id) Version Modification Details
# 19/12/2016        NANDAKUMAR PANDIAN      01         TWITTER LIVE STREAMING STAGE LOAD BY USING FLUME
*/


--Step 1--Download Flume_Source from below website 
https://github.com/cloudera/cdh-twitter-example
--Then place the flume-sources folder to anywhere in cloudera VM from your local download folder
cd /home/cloudera/Desktop
ls -ltr
cd /home/cloudera/Desktop/flume-sources
ls -ltr /home/cloudera/Desktop/flume-sources
--Download the mvn package
cd /home/cloudera/Desktop/flume-sources
mvn package
--Once mvn package download is completed then just ensure whether target folder is created or not 
ls -ltr /home/cloudera/Desktop/flume-sources/target

--Step 2--set up the config file in the below directory 
/etc/flume-ng/conf/

--Rough work
rm /etc/flume-ng/conf/flume.conf
cp /home/cloudera/FlumeTutorial/apache-flume-1.4.0-bin/conf/flume.conf /etc/flume-ng/conf/
gedit /etc/flume-ng/conf/flume.conf

--setp 3:
cp /home/cloudera/Desktop/flume-sources/target/flume-sources-1.0-SNAPSHOT.jar /usr/lib/flume-ng/lib/ 

--Step 4:Run the final command 

cd /etc/flume-ng/conf/
flume-ng agent --conf conf --conf-file flume.conf --name TwitterAgent -Dflume.root.logger=INFO,console

--step 5: To view the twitter streamed data in the target hadoop directory

hadoop fs -ls /user/Flume/twitter_data/

hadoop fs -rm /user/Flume/twitter_data/backup/

hadoop fs -cat /user/Flume/twitter_data/FlumeData.1476642831382 | head -10

--step 6 : To load the data to hive tables from hadoop

Download hive-serdes-1.0-SNAPSHOT.jar  from https://github.com/cloudera/cdh-twitter-example
add jar /usr/lib/hive/lib/hive-serdes-1.0-SNAPSHOT.jar 

hadoop fs -rm /user/Flume/twitter_data/backup/

hadoop fs -ls /user/Flume/twitter_data/backup
hadoop fs -ls /user/Flume/twitter_data/backup

hadoop fs -mkdir /user/Flume/twitter_data/backup;
hadoop fs -chmod 777 /user/Flume/twitter_data/backup


hadoop fs -mv /user/Flume/twitter_data/FlumeData.1477938459406 /user/Flume/twitter_data/backup;
hadoop fs -mv /user/Flume/twitter_data/FlumeData.1477938576700 /user/Flume/twitter_data/backup;

hadoop fs -ls /user/Flume/twitter_data/backup/

create database FLUME_STG;
create database FLUME_CORE;

drop table IF  EXISTS FLUME_STG.FLUME_CUSTOMER_STUDENT;

CREATE EXTERNAL TABLE FLUME_STG.FLUME_CUSTOMER_STUDENT (
  id BIGINT,
  created_at STRING,
  source STRING,
  favorited BOOLEAN,
  retweeted_status STRUCT<
    text:STRING,
    user:STRUCT<screen_name:STRING,name:STRING>,
    retweet_count:INT>,
  entities STRUCT<
    urls:ARRAY<STRUCT<expanded_url:STRING>>,
    user_mentions:ARRAY<STRUCT<screen_name:STRING,name:STRING>>,
    hashtags:ARRAY<STRUCT<text:STRING>>>,
  text STRING,
  user STRUCT<
    screen_name:STRING,
    name:STRING,
    friends_count:INT,
    followers_count:INT,
    statuses_count:INT,
    verified:BOOLEAN,
    utc_offset:INT,
    time_zone:STRING>,
  in_reply_to_screen_name STRING
) ROW FORMAT SERDE 'com.cloudera.hive.serde.JSONSerDe'
LOCATION '/user/Flume/twitter_data/backup';

select * from FLUME_STG.FLUME_CUSTOMER_STUDENT limit 2;
select count(*) from FLUME_STG.FLUME_CUSTOMER_STUDENT ;

select id,count(id) from FLUME_STG.FLUME_CUSTOMER_STUDENT  group by id;
