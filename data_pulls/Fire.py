# Databricks notebook source
import geopandas as gpd

# COMMAND ----------

data = gpd.read_file('/Volumes/dev_bricks/files/raw/WFIGS Interagency Fire Perimeters/Perimeters.shp')

# COMMAND ----------

data.head()

# COMMAND ----------

spark.createDataFrame(data).write.format('delta').mode('overwrite').saveAsTable('asdfasdf')

# COMMAND ----------



# COMMAND ----------

data = spark.sql("select * from demographics.seer_annual_county_populations group by county").toPandas()

# COMMAND ----------


