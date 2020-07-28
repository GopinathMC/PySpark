from pyspark.sql.functions import *

col = ['KEYFIGUREDATE','CUSTID','PRDID','ZBUDGQTY']

df = spark.read.format('csv').option('header','true').option('inferschema','true').load('file:///efs...')

unpivoteddf = df.select('SoldID','ProdID',expr("stack(3,'Jun',Jun,'Jul',Jul,'Aug',Aug) as (Keyfiguredate,qty)"))

opDF = unpivoteddf.select('Keyfiguredate','SoldID','ProdID','qty')
fnDF = opDF.withColumn('keyfigureDate',when(col('KeyfigureDate')=='Jun','20200601').when(col('KeyfigureDate')=='Jul','20200701').when(col

('KeyfigureDate')=='Aug','20200801')).select(col('KeyfigureDate'),col('SoldID'),col('ProdID'),col('qty'))

#read hive table
custProdDF = spark.read.table('prd_product_fibi_ibp.md_customerproduct').select(col('custid'),col('soldto'),col('prdid'))

restDF = fnDF.join(custDF,fnDF['SoldID']==custDF['soldto']).select(col('KeyfigureDate'),col('shipto_soldto'),col('ProdID'),col('qty'))

windowSpec = window.partitionBy('prdid','soldtoid').orderBy('prdid')
custProdT = custProdDF.withColumn('repeat_times',row_number().over(windowSpec)).withColumn('divide_value',max('repeat_times').over(windowSpec))

finalDF = fnDF.join(custProdT,[fnDF['ProdID']==custProdT['prdid'],fn['SoldID']==custProdT['soldtoid']],how='left').withColumn('qtyC',regexp_replace(col

('qty'),",","").cast(IntegerType())).withColumn('valuedivided'),col('qtyC')/col('divide_value')))

#writing
finalDF.coalesce(1).write.format('csv').option('header','true').option('inferschema','true').mode('overwrite').save('file:///efs...')
