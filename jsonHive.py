#simple pyspark program to store json data into hive table

import json
from pyspark.sql import HiveContext
from pyspark import SparkContext
sc = SparkContext()
hiveContext = HiveContext(sc)

#setting the parameters for hive partitioning mode
hiveContext.setConf("hive.exec.dynamic.partition","true")
hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

jsonDf = hiveContext.read.json("file:///efs/home/ps900191/pyspark_prjct/Load_HiveTBL_from_Json/jsondata.txt")

jsonDf.show() 
#storing the data in hive table
#below code is to store the data in partitoned table, here I am just storing in non-partitoned table
#jsonDf.write.mode("append").partitionBy('country').insertInto("dev_internal_cmmp.cust_partition")

jsonDf.write.format("orc").saveAsTable("dev_internal_cmmp.cust_partition1")
