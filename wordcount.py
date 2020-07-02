#simple wordcount problem using pyspark and register it as dataFrame

import sys
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
words = sc.textFile("file:///2")
lines = words.flatMap(lambda line : line.split(" "))
wordcount = lines.map(lambda word : (word,1))
aggrcount = wordcount.reduceByKey(lambda a,b : a+b)
df1 = sqlContext.createDataFrame(aggrcount,schema = ("words","count"))
print("Toatl words in the files are {}".format(df1.count()))
print("Here is the words and its counts!!")
df1.show()
