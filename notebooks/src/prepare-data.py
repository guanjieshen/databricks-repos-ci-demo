# Databricks notebook source
# DBTITLE 1,Imports
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from sklearn.datasets import load_boston
import pandas as pd

# COMMAND ----------

# DBTITLE 1,Functions
def get_data() -> DataFrame:
    bd = load_boston()
    _df = pd.DataFrame(bd.data, columns=bd.feature_names)
    _df["TARGET"] = bd.target
    df = spark.createDataFrame(_df)
    return df


# COMMAND ----------

# DBTITLE 1,Main
df = get_data()
display(df)

# COMMAND ----------

# MAGIC %autoreload 2

# COMMAND ----------

from helpers import my_funcs


output_df = df.transform(lambda df: my_funcs.roundValue(df, "CRIM",3))
display(output_df)

# COMMAND ----------

df.write.format("delta").mode("overwrite").save("dbfs:/databricks-ci-demo/data/boston")
