#simple program to load data from text file & register it as temporary table and perform sparksql operation

from pyspark import SparkContext
from pyspark.sql import SQLContext,Row

sc = SparkContext()
sqlContext = SQLContext(sc)

rdd1 = sc.textFile("file:///efs/home/ps900191/pyspark_prjct/Load_HiveTBL_from_txtFile/sample_data.txt")

header = rdd1.first()
data = rdd1.filter(lambda x : x!=header)
data1 = data.map(lambda x : x.split(','))
col = header.split(',')
df1 = sqlContext.createDataFrame(data1,schema = col)

print("Here is the data frame created from text file!!")
df1.show()

df1.registerTempTable("pyspahive")

#doing some sparkSQL operations

result1 = sqlContext.sql("select concat(first_name,last_name) as NAME,regexp_replace(cast(phone as string),'^0+','') as NUMBER,email,city from pyspahive")
result1.show()