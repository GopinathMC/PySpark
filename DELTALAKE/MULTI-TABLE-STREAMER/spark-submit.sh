#!/bin/bash

cd ~

spark-submit --packages io.delta:delta-core_2.11:0.6.1,com.amazonaws:aws-java-sdk-pom:1.11.913,org.apache.hadoop:hadoop-aws:2.7.0,org.apache.hadoop:hadoop-common:2.7.0 \
 --jars /home/path/jars/spark-sql-kinesis_2.11-1.2.1_spark-2.4-SNAPSHOT.jar,/home/path/jars/aws-java-sdk-1.11.913.jar,/home/path/jars/hadoop-aws-2.7.0.jar,/home/path/jars/hadoop-common-2.7.0.jar \
 --conf 'spark.serializer=org.apache.spark.serializer.KryoSerializer' --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
  --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
  --conf "spark.delta.logStore.class=org.apache.spark.sql.delta.storage.S3SingleDriverLogStore" \
  --driver-memory 5G --executor-memory 9G --num-executors 4 /home/path/deltalake/table/multi-delta-streamer.py
