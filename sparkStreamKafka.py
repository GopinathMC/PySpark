from pyspark.sql.types import BooleanType, IntegerType, StructType, StringType, StructField, TimestampType
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType

schema = (StructType()
  .add("timestamp", TimestampType())
  .add("url", StringType())
  .add("userURL", StringType())
  .add("pageURL", StringType())
  .add("isNewPage", BooleanType())
  .add("geocoding", StructType()
    .add("countryCode2", StringType())
    .add("city", StringType())
    .add("latitude", StringType())
    .add("country", StringType())
    .add("longitude", StringType())
    .add("stateProvince", StringType())
    .add("countryCode3", StringType())
    .add("user", StringType())
    .add("namespace", StringType()))
)

spark.conf.set("spark.sql.shuffle.partitions", sc.defaultParallelism)

kafkaDF = (spark
  .readStream
  .option("kafka.bootstrap.servers", "server1.databricks.training:9092")
  .option("subscribe", "en")
  .format("kafka")
  .load()
)


kafkaCleanDF = (kafkaDF
  .select(from_json(col("value").cast(StringType()), schema).alias("message")) #kafka transmit info like key value pair, we just accession value with above schema
  .select("message.*")
  
goeocodingDF = (kafkaCleanDF
  .filter(col("geocoding.country").isNotNull())
  .select("timestamp", "pageURL", "geocoding.countryCode2", "geocoding.city")
)

countries = spark.read.parquet("/mnt/training/countries/ISOCountryCodes/ISOCountryLookup.parquet")
joinedDF = goeocodingDF.join(countries, goeocodingDF.countryCode2 == countries.alpha2Code)

#writing to delta table
joinedDF
  .writeStream                                   # Write the stream
  .format("delta")                               # Use the delta format
  .partitionBy("countryCode2")                   # Specify a feature to partition on
  .option("checkpointLocation", checkpointPath)  # Specify where to log metadata
  .option("path", joinedPath)                    # Specify the output path
  .outputMode("append")                          # Append new records to the output path
  .queryName(myStreamName)                       # The name of the stream
  .start()                                       # Start the operation
)

joinedDF.stop()
