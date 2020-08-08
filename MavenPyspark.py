import sys, os, multiprocessing
from pyspark.sql import DataFrame, DataFrameStatFunctions, DataFrameNaFunctions
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as sFn
from pyspark.sql.types import *
from pyspark.sql.types import Row
  # ------------------------------------------
  # Note: Row() in .../pyspark/sql/types.py
  # isn't included in '__all__' list(), so
  # we must import it by name here.
  # ------------------------------------------

num_cpus = multiprocessing.cpu_count()        # Number of CPUs for SPARK Local mode.
os.environ.pop('SPARK_MASTER_HOST', None)     # Since we're using pip/pySpark these three ENVs
os.environ.pop('SPARK_MASTER_POST', None)     # aren't needed; and we ensure pySpark doesn't
os.environ.pop('SPARK_HOME',        None)     # get confused by them, should they be set.
os.environ.pop('PYTHONSTARTUP',     None)     # Just in case pySpark 2.x attempts to read this.
os.environ['PYSPARK_PYTHON'] = sys.executable # Make SPARK Workers use same Python as Master.
os.environ['JAVA_HOME'] = '/usr/lib/jvm/jre'  # Oracle JAVA for our pip/python3/pySpark 2.4 (CDH's JRE won't work).
JARS_IVE_REPO = '/home/jdoe/SPARK.JARS.REPO.d/'

# ======================================================================
# Maven Coordinates for JARs (and their dependencies) needed to plug
# extra functionality into Spark 2.x (e.g. Kafka SQL and Streaming)
# A one-time internet connection is necessary for Spark to autimatically
# download JARs specified by the coordinates (and dependencies).
# ======================================================================
spark_jars_packages = ','.join(['org.apache.spark:spark-streaming-kafka-0-10_2.11:2.4.0',
                                'org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.0',])
# ======================================================================
spark_conf = SparkConf()
spark_conf.setAll([('spark.master', 'local[{}]'.format(num_cpus)),
                   ('spark.app.name', 'myApp'),
                   ('spark.submit.deployMode', 'client'),
                   ('spark.ui.showConsoleProgress', 'true'),
                   ('spark.eventLog.enabled', 'false'),
                   ('spark.logConf', 'false'),
                   ('spark.jars.repositories', 'file:/' + JARS_IVE_REPO),
                   ('spark.jars.ivy', JARS_IVE_REPO),
                   ('spark.jars.packages', spark_jars_packages), ])

spark_sesn            = SparkSession.builder.config(conf = spark_conf).getOrCreate()
spark_ctxt            = spark_sesn.sparkContext
spark_reader          = spark_sesn.read
spark_streamReader    = spark_sesn.readStream
spark_ctxt.setLogLevel("WARN")
