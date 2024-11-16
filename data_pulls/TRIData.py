# Databricks notebook source
import pandas as pd

# COMMAND ----------

data = pd.read_csv("/Volumes/dev_bricks/files/raw/TRI_processed_1987_to_2020.csv")

# COMMAND ----------

data.head()

# COMMAND ----------

spark.createDataFrame(data.rename(columns={column: column.replace(' ', '') for column in data})).write.format('delta').mode('overwrite').saveAsTable('default.TRI')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from 

# COMMAND ----------

dbutils.fs.ls("/Volumes/dev_bricks/files/raw/")

# COMMAND ----------


