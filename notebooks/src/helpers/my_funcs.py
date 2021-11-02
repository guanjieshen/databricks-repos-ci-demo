from pyspark.sql import dataframe
from pyspark.sql.functions import col, round


def roundValue(df: dataframe, targetCol:str, sigdig: int) -> dataframe:
    return df.withColumn("rounded_val", round(targetCol, sigdig)).drop(col(targetCol)).withColumnRenamed("rounded_val",targetCol)

