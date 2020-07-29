#Types:

#1. Series to Series(pd.Series -> pd.Series)
import pandas as pd
from pyspark.sql.functions import pandas_udf

df=spark.range(10)

@pandas_udf('long')
def plus_one(s: pd.Series) -> pd.Series:
    return s+1
    
df_1 = sf.withColumn('id+1',plus_one('id'))

#2. Iterator series to Iterator series(Iterator[pd.Series] -> Iterator[pd.Series] -> Iterator[pd.Series])
#takes singles column, lenghth of input and output should be same
from typing import Iterator
@pandas_udf('long')
def plus_one(iterator: Iterator[pd.Series]) -> Iterator[pd.Series]:
    return map(lambda s : s+1,iterator)
