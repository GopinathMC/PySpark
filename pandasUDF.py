#Types:
#1. Series to Series

import pandas as pd
from pyspark.sql.functions import pandas_udf

df=spark.range(10)

@pandas_udf('long')
def plus_one(s: pd.Series) -> pd.Series:
    return s+1
    
df_1 = sf.withColumn('id+1',plus_one('id'))
