#1 read multiple csv file
from pyspark.sql.functions import col,window,desc
staticDF = spark.read.format('csv').option('inferschema','true').option('header','true').load('file:///efs/home/ps900191/pyspark_prjct/DataBricks/Data/RetailData/*.csv*)
df4 = staticDF.selectExpr('CustomerID','(Quantity * UnitPrice) as TotalCost','InvoiceDate').groupBy(col('CustomerID'),window('InvoiceDate','1 day')).sum('TotalCost').sort(desc('CustomerID')) #window function grouping the total cost with 1 day irrespective of time of that day
df1 = flightData.groupBy('DEST_COUNTRY_NAME').sum('count').withColumnRenamed('sum(count)','DestTotal').sort(desc('DestTotal'))

#sparkStreaming
streamDF = spark.readStream.schema(staticSchema).option('maxFilesPerTrigger',1).option('header','true').format('csv').load('file:///efs/home/ps900191/pyspark_prjct/DataBricks/Data/RetailData/*.csv*')#create streaming dataframe
#streamDF.isStreaming => True, means its straming check
purchasePerHr = streamDF.selectExpr('CustomerID','(Quantity * UnitPrice) as TotalCost','InvoiceDate').groupBy(col('CustomerID'),window('InvoiceDate','1 day')).sum('TotalCost').sort(desc('CustomerID'))#apply business logic
purchasePerHr.writeStream.format('memory').queryName('cust_purchase').outputMode('complete').start()#streaming Action, stores data in in-memory table, here 'cust_purchase'
df2 = spark.sql('select * from cust_purchase')

#define schema manually
from pyspark.sql.types import StructType,StructField,StringType,LongType
mySchema = StructType([StructField('Destination',StringType(),True),StructField('Origination',StringType(),True),StructField('Count',LongType(),False,metadata={'hello':'world'})])
flight = spark.read.format('json').schema(mySchema).load('file:///efs/home/ps900191/pyspark_prjct/DataBricks/Data/FlightData/json/flightData.json')

#selectExpr(helpful for doing aggregation operations compared to select statement)
flight.selectExpr('*','(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry').show()

#literals -> assign some constant value to df using literals
from pyspark.sql.functions import lit
flight.select('*',lit(1).alias('const')).show()

#adding/rename/drop/cast column(withColumn('colname',expression())),(withColumnRenamed('oldName','newName')),drop('','')
flight.withColumn('sameCountry',expr('DEST_COUNTRY_NAME == ORIGIN_COUNTRY_NAME')).show()
flight.withColumnRenamed('count','totalServices').show()
flight.drop('count','totalServices').show()
flight.withColumn("countstr",col('count').cast('String')).select('DEST_COUNTRY_NAME','ORIGIN_COUNTRY_NAME','countstr').printSchema()#casting
flight.filter(col('DEST_COUNTRY_NAME')=='Egypt').show()#filter
flight.where("DEST_COUNTRY_NAME=='Egypt'").show()#where
flight.select('DEST_COUNTRY_NAME','ORIGIN_COUNTRY_NAME').distinct().show()#distinct

#to make spark case sensitive
set spark.sql.caseSensitive true

#sorting 'asc_nulls_first','desc_nulls_last'
flight.orderBy(expr('count desc')).limit(5).show()

#repartition
flightPar=flight.repartition(5,col('DEST_COUNTRY_NAME'))#full shuffling the data across cluster based on country column
flight.rdd.getNumPartitions()#to find total partiton for that dataframe
#coalesce
flightPar.coalesce('2')#combine the partitions into 2 partition, avoids full shuffling, but data won't be distributed equally across cluster compare to repartition

#boolean
countryf = col('DEST_COUNTRY_NAME').contains('United States')
countf = col('count') > 10
USflight = flight.where(countryf | countf)
US = flight.withColumn('USFlights',countryf & countf)

#numbers
totalQty = pow(col('UnitPrice')*col('Quantity'),2)+5 #from pyspark.sql.functions import pow
retailOri = retail.select(col('CustomerID'),totalQty.alias('OriginalQty'))
select *,POWER((UnitPrice*Quantity),2.0)+5 as OriginalQty from dfTable #SparkSql
df = df.select(round(lit('2.5')),bround(lit('2.5'))) #from pyspark.sql.functions import lit,round,bround ans: 3(round),2(bround)

#pearson correlation(-1 to 1), 1 means both are linearly corelated, and -1 means inversly related, 0 means no relation between two
from pyspark.sql.functions import corr
corrr = retailOri.select(corr('Quantity','UnitPrice'))

#regular expressions
retailexp = retail.select(regexp_exp(col('Description'),'BLUE|ADVENT|WHITE','color'))
retailext = retail.select(regexp_extract(col('Description'),'(BLUE|ADVENT)',1))

#date&Time
from pyspark.sql.functions import current_date,current_timestamp,date_add,date_sub,datediff,months_between,to_date
df = spark.range(5).withColumn('Today',current_date()).withColumn('now',current_timestamp())
dfdiff = df.withColumn('weekAgo',date_sub(col('Today'),7)).withColumn('weekLater',date_add(col('Today'),7)).select('*')
df = df.withColumn('diff',datediff(c1,c2))
dateFormat = 'yyyy-dd-MM'
dff = spark.range(1).withColumn('dates',to_date(lit("2019-20-12"),dateFormat)).select('dates')
dff = spark.range(1).withColumn('dates',to_timestamp(lit("2019-20-12"),dateFormat)).select('dates')

#with nulls
df.na.drop('all')#If all values in rows are null then that row will be dropped
df.na.drop('any')#IF any of the value is null that row will be dropped
df.na.drop('all',subset=['c1','c2'])
#fill
fillALlCols = {'StockCode':5,'Description':'It is null'}
df.na.fill(fillALlCols)
#replace
df.replace('[]',['Its null'],'Description')
complexDf = retail.select(struct('InvoiceNo','UnitPrice').alias('Complex'))

#split
complexDf = retail.select(split('Description',' ').alias('Complex'))
comp = complexDf.select(expr('Complex[o]')).show()
complexDf = retail.select(size(split('Description',' ')).alias('Complex'))
complexDf = retail.select(array_contains(split('Description',' ')).alias('Complex'))
#explode
exploded = complexDf.withColumn('explodes',explode(col('Complex'))).select('Complex','explodes')
#create_map
mapped = retail.select(create_map(expr('StockCode'),expr('UnitPrice')).alias('getPrice'))

#jsonfrom pyspark.sql.funcions import to_json,from_json
jsonDff = retail.selectExpr('(InvoiceNo,UnitPrice) as merged').select(to_json(expr('merged'))).alias('js')
jsonToNorm = jsonDff.select(from_json(expr('js'),schema)).alias('norm')

#UDF
#create udf using python/scala
"""
     Scala UDF is always faster than python UDF,since scala directly runs on JVM but for python, spark starts a python process in worker then serialize each data into
	 python understandable then compute row by row finally return to JVM and spark, serializing the data to python is too tedious:(
"""
def power3(x):
    return x**3
#register this UDF as spark data frame function
from pyspark.sql.functions import udf
powerOf3 = udf(power3)#nomalUDF --> Spark UDF
df = spark.range(10).toDf('num')
#pass the df to the function
df3 = df.select(powerOf3(col('num')))
#register the UDF as spark SQL(effective compared to above since this UDFcan be used in sql or across language)
spark.udf.register('pow3',power3,DoubleType())

#hiveUDF
SparkSession.builder().enableHiveSupport()
create temporary function/permanent as my_fun as 'com.organization.hive.udf.functionName'
