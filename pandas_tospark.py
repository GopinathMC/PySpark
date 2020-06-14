import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,StringType
import pandas as pd
from impala.dbapi import connect
from impala.util import as_pandas
IMPALA_HOST = "peanut-impala.cargill.com"
IMPALA_PORT = 21050
IMPALA_USER = "ps329181@AP.CORP.CARGILL.COM"
IMPALA_PASSWORD = "Welc0me@1"
conn = connect(host=IMPALA_HOST,port=IMPALA_PORT,use_ssl=True,ca_cert=None,auth_mechanism='PLAIN',user=IMPALA_USER,password=IMPALA_PASSWORD)
cursor = conn.cursor()
cursor.execute('Select * from prd_product_china_anf.i_submission_schedule;')
df_pd = as_pandas(cursor)

spark = SparkSession.builder.appName('pandasToSparkDF').getOrCreate()

mySchema = StructType([StructField('uomtoid',StringType(),True),StructField('uomtodescr',StringType(),True),StructField('business_unit',StringType(),True)])

df = spark.createDataFrame(df_pd,schema=mySchema)

