CREATE TABLE cust_partition(
designation STRING,
email STRING,
first_name STRING,
gender STRING,
id INT,
last_name STRING,
phone STRING
)
PARTITIONED BY
(country STRING)
STORED AS ORC
TBLPROPERTIES
('hive.exec.dynamic.partition.mode'='nonstrict',
'hive.exec.dynamic.partition'='true',
'orc.compress'='SNAPPY');