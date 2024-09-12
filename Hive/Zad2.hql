SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.max.dynamic.partitions = 2000;
SET hive.exec.max.dynamic.partitions.pernode = 500;
set hive.mapred.mode=nonstrict;

USE nowakpawe;

DROP TABLE IF EXISTS temp_terro PURGE;

CREATE TEMPORARY EXTERNAL TABLE IF NOT EXISTS temp_terro (
  	id						INT,
    eventid		            INT,
    country         		STRING,
    country_txt 			STRING,
    region   				STRING,
    region_txt   			STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/nowakpawe/dataset'
TBLPROPERTIES ('skip.header.line.count'='1');

DROP TABLE IF EXISTS terro PURGE;

CREATE TABLE terro (
    country_txt         	STRING)
COMMENT 'nameses'
PARTITIONED BY(
    region_txt              STRING)
CLUSTERED BY(country_txt) INTO 4 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE terro PARTITION(region_txt)
    SELECT s.country_txt, s.region_txt
	FROM temp_terro s;
	
DROP TABLE IF EXISTS newtab PURGE;

CREATE TABLE newtab AS SELECT country_txt, region_txt,  COUNT(*) as incidents FROM terro GROUP BY country_txt, region_txt;

SELECT q3.region_txt, q3.country_txt, q3.incidents FROM(
  SELECT q2.region_txt, t1.country_txt, t1.incidents, q2.median FROM(
	  SELECT region_txt, percentile(cast(incidents as BIGINT), 0.5) as median FROM newtab GROUP BY region_txt) AS q2 JOIN newtab t1 ON (t1.region_txt = q2.region_txt)) AS q3 WHERE incidents < median