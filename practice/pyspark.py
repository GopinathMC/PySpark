from pyspark.sql.types import *
from pyspark.sql.functions import col,udf,avg,sum,min,max,desc,round

#below is the way of registering the UDF in pyspark
# @udf(returnType=DecimalType())
# def normalize_series(s):
#     return (s - s.min()) / (s.max() - s.min())
#second method
#normalizeUDF = udf(lambda z:normalize_series(z),DecimalType())

df = spark.read.format('csv').option('header','true').load('folder/path/best_match_jul2.csv')
df = df.withColumn('cust_prop', col('purchases')/col('customer_tot_items'))
df = df.withColumn('tk_prop', col('item_count')/col('takeaway_total_items'))
df = df.withColumn('product_prop', col('cust_prop')*col('tk_prop'))
sdf = df.groupBy('takeaway_name').agg(sum('product_prop').alias('product_prop'))# for multiple column simply seperate with commo
#multi_agg = df.groupBy('takeaway_name').agg(sum('product_prop').alias('total'),avg('product_prop').alias('average'))
#sdf.select('takeaway_name').distinct().count(), get distinct takeaway count
sdf = sdf.repartition(10,col('takeaway_name')) # repartition it based on takeaway since after groupby aggregation there were 200 partitions for small data
fdf = df.join(sdf.withColumnRenamed('product_prop','product_prop_sum'), on=['takeaway_name'], how='inner')
fdf = fdf.dropDuplicates((['takeaway_name']))
#fdf = fdf.withColumn('product_prop_sum_n',normalize_series(col('product_prop_sum'))) call UDF, this is not working in spark since s.min is pointless in spark
prop_min = fdf.agg(min(col('product_prop_sum'))).head()[0]
prop_max = fdf.agg(max(col('product_prop_sum'))).head()[0]
fdf = fdf.withColumn('normalised',(col('product_prop_sum')-prop_min) / (prop_max - prop_min))
fdf = fdf.withColumn('value', (col('takeaway_wait_grp')*1.5) +  (col('takeaway_min_order_grp')*1.25) + (col('normalised')*1.25) )
fdf = fdf.withColumn('value', (col('value') * (col('takeaway_rating') / 5)) ).sort(desc('value'))
fdf = fdf.withColumn('takeaway_rating',col('takeaway_rating').cast(DecimalType(3,2)))
#fdf = fdf.withColumn('takeaway_rating',round(col('takeaway_rating'),2)) for rounding



##############################################################PANDAS EQUIVALENT CODE##############################################################


df['cust_prop'] = df['purchases']/df['customer_tot_items']
df['tk_prop'] = df['item_count']/df['takeaway_total_items']
df['product_prop'] = df['cust_prop']*df['tk_prop']
sdf = df.groupby(['takeaway_name']).agg({'product_prop': 'sum'}).reset_index()
fdf = pd.merge(left=df, right=sdf, on=['takeaway_name'],how='inner')
fdf['product_prop_y'] = normalize_series(fdf['product_prop_y'])
fdf['value'] = (fdf['takeaway_wait_grp']*1.5)+(fdf['takeaway_min_order_grp']*1.25)+(fdf['product_prop_y']*1.25)
fdf['value'] = fdf['value'] * (fdf['takeaway_rating']/5)
fdf = fdf.sort_values(by=['value'],ascending=False)
