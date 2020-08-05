#1 read multiple csv file
from pyspark.sql.functions import col,window,desc
staticDF = spark.read.format('csv').option('inferschema','true').option('header','true').load('file:/*.csv*)
df4 = staticDF.selectExpr('CustomerID','(Quantity * UnitPrice) as TotalCost','InvoiceDate').groupBy(col('CustomerID'),window('InvoiceDate','1 day')).sum('TotalCost').sort(desc('CustomerID')) #window function grouping the total cost with 1 day irrespective of time of that day
df1 = flightData.groupBy('DEST_COUNTRY_NAME').sum('count').withColumnRenamed('sum(count)','DestTotal').sort(desc('DestTotal'))

#sparkStreaming
streamDF = spark.readStream.schema(staticSchema).option('maxFilesPerTrigger',1).option('header','true').format('csv').load('fa/*.csv*')#create streaming dataframe
#streamDF.isStreaming => True, means its straming check
purchasePerHr = streamDF.selectExpr('CustomerID','(Quantity * UnitPrice) as TotalCost','InvoiceDate').groupBy(col('CustomerID'),window('InvoiceDate','1 day')).sum('TotalCost').sort(desc('CustomerID'))#apply business logic
purchasePerHr.writeStream.format('memory').queryName('cust_purchase').outputMode('complete').start()#streaming Action, stores data in in-memory table, here 'cust_purchase'
df2 = spark.sql('select * from cust_purchase')

#define schema manually
from pyspark.sql.types import StructType,StructField,StringType,LongType
mySchema = StructType([StructField('Destination',StringType(),True),StructField('Origination',StringType(),True),StructField('Count',LongType(),False,metadata={'hello':'world'})])
flight = spark.read.format('json').schema(mySchema).load('file.json')

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

#aggregation
df.groupBy('col1').agg(expr(avg(c2)),expr(stddev_pop(c3))).show()

#window functions(rank,dense_rank)
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank,rank,max
Windowspec = Window.partitionBy('CustomerID','InvoiceDate').orderBy(desc('Quantity')).rowsBetween(Window.unboundedPreceding,Window.currentRow)
maxPurchase = max(col('Quantity')).over(Windowspec)
quantityDRank = dense_rank().over(Windowspec)
quantityRank = rank().over(Windowspec)
retailnew = retail.where('CustomerID is not null').orderBy('CustomerID').select(col('CustomerID'),col('InvoiceDate'),col('Quantity'),maxPurchase.alias('MaxPurchaseQuantity'),quantityDRank.alias('Dense'),quantityRank.alias('QRank'))
""" OUTPUTvfor window function, 
CustomerID|        InvoiceDate|Quantity|MaxPurchaseQuantity|Dense|QRank|
+----------+-------------------+--------+-------------------+-----+-----+
|   12347.0|2010-12-07 14:57:00|      36|                 36|    1|    1|
|   12347.0|2010-12-07 14:57:00|      30|                 36|    2|    2|
|   12347.0|2010-12-07 14:57:00|      24|                 36|    3|    3|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|      12|                 36|    4|    4|
|   12347.0|2010-12-07 14:57:00|       6|                 36|    5|   17|
|   12347.0|2010-12-07 14:57:00|       6|                 36|    5|   17|"""

#partitioning and bucketing while writing df(Bucketing only for Spark managed table)
csvFile.write.format('PARQUET').mode('overwrite').partitionBy('country').save('hdfs:nameservice/prd/product/fibi_ibp')
csvFile.write.format('PARQUET').mode('overwrite').bucketBy(10,'country').saveAsTable('BucketTable')#if you give .saveAsTable('BucketTable'), it will be stored in default hive location (i.e)/user.hive/warehouse
csvFile.write.option('maxRecordsPerFile',5000).format('ORC').save('hdfs:')#can specify the number of records in file




#########################################################################RDD operations#############################################################

rd2 = rd1.keyBy(lambda x:x.upper()[1])#to get key value pairs
rd2.collect()
[('H', 'Dhoni'), ('A', 'Watson'), ('A', 'Raina'), ('A', 'Jaduu')]
rd3 = rd2.mapValues(lambda x:x.upper()) #mapvalues to some functions

#broadcast variable, it shares variable/rdd to all executors so that serializable operation is prevented across nodes which results better speed/performance
a = {'Gopi':'Bat','Nath','Bowl'}
bc = sc.broadcast(a)#broadcasted to all executors
bc.value#to see data
word = sc.parallelize(['Gopi','Nath','Arul'])
rdd1 = word.map(lambda x : (x,bc.value.get(x,0)))#[('Gopi', 'Bat'), ('Nath', 'Bowl'), ('Arul', 0)]
rdsort = rdd1.sortBy(lambda x:x[1])#[('Arul', 0), ('Gopi', 'Bat'), ('Nath', 'Bowl')]

#accumulators a mutable variable that can be updated through series of transformations and send it to driver node more efficiently
flight = spark.read.format('csv').option('header','true').option('inferschema','true').load('fa/c')
acChina = sc.accumulator(0)#accumulator variable created i.e.acChina
def accuChi(x):
     dest = x['DEST_COUNTRY_NAME']
     orig = x['ORIGIN_COUNTRY_NAME']
     if(dest=='China' or orig=='China'):
         acChina.add(x['count'])
rd11 = flight.foreach(lambda x : accChi(x))
acChina.value #1692



###################################################advanced operations DF###########################################################################
finalDF.printSchema()#[rdid,ordqty,orddt,MRPType]
from pyspark.sql.functions import max as SparkMax
finz = finalDF.groupBy('MRPType').pivot('orddt').agg(max(col('ordqty')))[MRPType,'2020-01-29','2019-01-30'......]
columns = ["v"+str(i) for i in range(1,len(finz.columns)-1)] #[v1,v2,v3,v4......]
columns.insert(0,'MRP') #[MRP,v1,v2......]
finz = finz.toDF(*columns) #changing the columns names[MRP,v1,v2...]
#to perform operaion between adjacent cols
finzDiff = finz
for i in range(1,len(finz.columns)-1):
    finzDiff = finz.withColumn(columns(i),col(columns(i))-col(columns(i+1)))
	
#read table from MySQL
orders = (spark
             .read
             .format("jdbc")
             .option("url", "jdbc:mysql://localhost/retail_db")
             .option("driver", "com.mysql.jdbc.Driver")
             .option("dbtable", "orders")
             .option("user", "root")
             .option("password", "cloudera")
             .load())



			 
#####################################################PANDAS############################################################
import pandas as pd
file = 'D:/...'
df = pd.read_csv(file,sep=',',header=0,index_col=False,names=None)#read csv file
df.columns.tolist #list column names
df['sales_rep'].value_counts() #return map with value in key and its count in value
df1 = df.drop(columns=['zcmodeid','zcustprdstatus']) #drop columns

#read excel file and convert it as spark df
#prerequist pip install xlrd
pddf = pd.read_excel('/dbfs/filename',sheet_name='CDPUPLOAD',inferSchema='True')
sparkDf = spark.createDataFrame(pddf)

#filtering
mask = (df['prdid'].isin([100001442,100127884])) & (df['soldtoid']==1500000473)
print(df[mask])

#locating
print(df.loc[2,['prdid','custid']])

#apply UDF
def calendar_year(x,y):
    x=str(x)
    if y in ['June','July','August','September','October','November','December']:
       ans =  'FY'+x[2:4]+str((int(x[2:4])+1))
    else:
        ans = 'FY'+str((int(x[2:4])-1))+x[2:4]
    return ans

recharge_new['fy'] = recharge_new.apply(lambda x : calendar_year(x['calendar_year'],x['financial_month']),axis=1)
print(recharge_new.head(10))


######################################################THEORY SPARK ########################################################
Modes:
1. Cluster Mode:
2. Refer github.com/spark-examples/pyspark-examples for more codes
