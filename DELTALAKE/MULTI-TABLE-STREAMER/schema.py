from pyspark.sql.types import *

def get_schema(table):
    if(table == 'table1'):
        schema = StructType([
    	        StructField('row',StructType([
        	StructField('col1',IntegerType(),True),
        	StructField('col2',StringType(),True),
        	StructField('col3',StringType(),True),
        	StructField('col4',StringType(),True),
        	StructField('col5',IntegerType(),True),
        	StructField('col6',StringType(),True),
        	StructField('col7',StringType(),True),
        	StructField('col8',StringType(),True),
        	StructField('col9',StringType(),True),
        	StructField('col10',StringType(),True),
        	StructField('col11',StringType(),True),
        	StructField('col12',TimestampType(),True),
        	StructField('col13',StringType(),True),
        	StructField('col14',StringType(),True),
        	StructField('col15',TimestampType(),True),
        	StructField('col16',TimestampType(),True)]),True),
    	        StructField('type',StringType(),True)])
        return schema
    elif(table == 'table2'):
        schema = StructType([
    	        StructField('row',StructType([
        	StructField('col1',IntegerType(),True),
        	StructField('col2',StringType(),True),
        	StructField('col3',StringType(),True),
        	StructField('col4',StringType(),True),
        	StructField('col5',IntegerType(),True),
        	StructField('col6',StringType(),True),
        	StructField('col7',StringType(),True),
        	StructField('col8',StringType(),True),
        	StructField('col9',StringType(),True),
        	StructField('col10',StringType(),True),
        	StructField('col11',StringType(),True),
        	StructField('col12',TimestampType(),True),
        	StructField('col13',StringType(),True),
        	StructField('col14',StringType(),True),
        	StructField('col15',TimestampType(),True),
        	StructField('col16',TimestampType(),True)]),True),
    	        StructField('type',StringType(),True)])
        return schema
    else:
        return "Please pass the correct table name"

#ORDER_INFO_BACKUP
def get_path(table):
    path = 's3://bucket/DELTA-LAKE-EMR/TABLES/'+table+'/'
    refPath = 's3://bucket/DELTA-LAKE-EMR/REFFILES/'+table+'/'
    checkPointPath = 's3://bucket/DELTA-LAKE-EMR/CHECK_POINT_DIR/'+table+'/'
    return path,refPath,checkPointPath
