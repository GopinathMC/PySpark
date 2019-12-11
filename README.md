# PyProjects

#simple wordcount problem using pyspark and register it as dataFrame

import sys
from pyspark import SQLContext,SparkContext
sc = SparkContext()
sqlContext = SQLContext(sc)
words = sc.textFile("file:///efs/home/ps900191/test2")
lines = words.flatMap(lambda line : line.split(" "))
wordcount = lines.map(lambda word : (word,1))
aggrcount = wordcount.reduceByKey(lambda a,b : a+b)
df1 = sqlContext.createDataFrame(aggrcount)
print("Toatl words in the files are {}".format(df1.count()))
print("Here is the words and its counts!!")
df1.show()
