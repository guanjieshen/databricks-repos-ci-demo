# Databricks notebook source
# DBTITLE 1,Install nutter and it's dependencies
# MAGIC %pip install -U nutter chispa

# COMMAND ----------

# MAGIC %md ### Test `prepare-data.py`

# COMMAND ----------

# DBTITLE 1,Test prepare-data
from runtime.nutterfixture import NutterFixture, tag


class PrepareDataTestFixture(NutterFixture):
    def run_test_name(self):
        dbutils.notebook.run("../src/prepare-data", 600)

    def assertion_test_name(self):
        counter = spark.sql(
            "SELECT COUNT(*) AS total FROM delta.`dbfs:/databricks-ci-demo/data/boston`"
        )
        first_row = counter.first()
        assert first_row[0] > 1


result = PrepareDataTestFixture().execute_tests()
print(result.to_string())

# COMMAND ----------

# MAGIC %md ### Test `Common.py`

# COMMAND ----------

# MAGIC %run ../src/common

# COMMAND ----------

from chispa.dataframe_comparer import *

class CommonTestFixture(NutterFixture):
    
  # we're using Chispa library here to compare the content of the processed dataframe with expected results
  def assertion_upper_columns(self):
    cols = ["col1", "col2", "col3"]
    df = spark.createDataFrame([("abc", "cef", 1)], cols)
    upper_df = upper_columns(df, cols)
    expected_df = spark.createDataFrame([("ABC", "CEF", 1)], cols)
    assert_df_equality(upper_df, expected_df)
      
result = CommonTestFixture().execute_tests()
print(result.to_string())

# COMMAND ----------

is_job = (
    dbutils.notebook.entry_point.getDbutils()
    .notebook()
    .getContext()
    .currentRunId()
    .isDefined()
)
if is_job:
    result.exit(dbutils)

