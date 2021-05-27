from pyspark.sql import SparkSession,Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json
from schema import *
from delta.tables import DeltaMergeBuilder, DeltaTable
from pyspark.sql.utils import AnalysisException



def processor(kinesisDF,schema):
    df = kinesisDF.withColumn("data", col("data").cast("string")).select("data")
    df = df.withColumn("data", from_json(df.data,schema))
    df_new = df.select('data.row.*','data.type')
    ad_window = Window.partitionBy('id').orderBy(col('updated_at').desc())
    df_new = df_new.withColumn('rn', row_number().over(ad_window)).filter('rn==1').drop('rn')
    fnDF = df_new.withColumn('__deleted',when(col('type') == 'DeleteRowsEvent','true').otherwise('false'))
    fnDF = fnDF.withColumn("order_date",to_date(col("order_placed_on"),"yyyy-MM-dd"))
    return fnDF


def get_delta_table(path):
    try:
        dt= DeltaTable.forPath(spark,path)
    except AnalysisException as e:
        if('doesn\'t exist;' in str(e).lower() or 'is not a delta table.' in str(e).lower()):
            print("Error Occured due to : "+str(e))
            return None
        else:
            raise e
    return dt


def upsertToOIB(microBatchOutputDF, batchId):
    schema = get_schema('table1')
    filDF = processor(microBatchOutputDF,schema)
    delDF = filDF.filter(col('__deleted')=='true')
    upsDF = filDF.filter(col('__deleted')=='false')
    oibdeltaTable = get_delta_table(oibpath) #expensive
    if not oibdeltaTable:
        upsDF.write.format('delta').mode('overwrite').partitionBy('order_date').save(oibpath)

    else:

        if(upsDF.rdd.isEmpty()==False):
          try:
              #oibdeltaTable.alias("t").merge(upsDF.alias("s"),"s.id = t.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
              merge_builder: DeltaMergeBuilder = (
                                    oibdeltaTable.alias("t").merge(source=upsDF.alias("s"),condition="s.id = t.id")
              )
              merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
          except Exception as e:
              print("Error Occured due to : "+str(e))
              raise e
        else:
          pass

        if(delDF.rdd.isEmpty()==False):
          idtoDel=delDF.select('id').collect()
          idList = [int(row.id) for row in idtoDel]
          try:
              oibdeltaTable.delete(col("id").isin(idList))
          except Exception as e:
              print("Error Occured due to : "+str(e))
        else:
           pass



def upsertToOI(microBatchOutputDF, batchId):
    schema  = get_schema('table2')
    filDF = processor(microBatchOutputDF,schema)
    delDF = filDF.filter(col('__deleted')=='true')
    upsDF = filDF.filter(col('__deleted')=='false')
    oideltaTable = get_delta_table(oipath) #expensive
    if not oideltaTable:
        upsDF.write.format('delta').mode('overwrite').partitionBy('order_date').save(oipath)
    else:
        if(upsDF.rdd.isEmpty()==False):
          try:
              #oideltaTable.alias("t").merge(upsDF.alias("s"),"s.id = t.id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
              merge_builder: DeltaMergeBuilder = (
                                    oideltaTable.alias("t").merge(source=upsDF.alias("s"),condition="s.id = t.id")
              )
              merge_builder.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()              
          except Exception as e:
              print("Error Occured due to : "+str(e))
              raise e
        else:
          pass

        if(delDF.rdd.isEmpty()==False):
          idtoDel=delDF.select('id').collect()
          idList = [int(row.id) for row in idtoDel]
          try:
              oideltaTable.delete(col("id").isin(idList))
          except Exception as e:
              print("Error Occured due to : "+str(e))
        else:
          pass


if __name__ == "__main__":

    spark = SparkSession.builder.appName("deltaLake") \
    .config("spark.jars.packages", "io.delta:delta-core_2.11:0.6.1") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.scheduler.mode", "FAIR") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()


    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.access.key', 'key')
    spark.sparkContext._jsc.hadoopConfiguration().set('fs.s3a.secret.key', 'key')

    oibDF = spark.readStream.format("kinesis").option("streamName","kinesis_table1").option("endpointUrl", "https://kinesis.region.amazonaws.com").option("awsAccessKeyId","key").option("awsSecretKey","key").option("startingposition","Latest").load()
    oiDF  = spark.readStream.format("kinesis").option("streamName","kinesis_table2").option("endpointUrl", "https://kinesis.region.amazonaws.com").option("awsAccessKeyId","key").option("awsSecretKey","key").option("startingposition","Latest").load()


    oibpath,oibrefPath,oibcheckPointPath = get_path('table1')
    oipath,oirefPath,oicheckPointPath = get_path('table2')

    #starting the streaming query
    queryOIB = oibDF.writeStream.trigger(processingTime='90 seconds').option('checkpointLocation', oibcheckPointPath).foreachBatch(upsertToOIB).start()
    print("First Query started")
    queryOI  = oiDF.writeStream.trigger(processingTime='60 seconds').option('checkpointLocation', oicheckPointPath).foreachBatch(upsertToOI).start()
    print("Second Query started")

    spark.streams.awaitAnyTermination()
