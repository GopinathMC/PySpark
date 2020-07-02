#simple pyspark program to store json data into hive table

import json
from pyspark.sql import HiveContext
from pyspark import SparkContext
sc = SparkContext()
hiveContext = HiveContext(sc)

#setting the parameters for hive partitioning mode
hiveContext.setConf("hive.exec.dynamic.partition","true")
hiveContext.setConf("hive.exec.dynamic.partition.mode","nonstrict")

jsonDf = hiveContext.read.json("file:..")

jsonDf.show() 
#storing the data in hive table
#below code is to store the data in partitoned table, here I am just storing in non-partitoned table

jsonDf.write.format("orc").saveAsTable("")
