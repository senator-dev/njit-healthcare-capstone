# Databricks notebook source
!pip install geopandas

# COMMAND ----------

import requests
import os
import zipfile
import shutil
from datetime import datetime
import geopandas as gpd

# COMMAND ----------

# MAGIC %md
# MAGIC # Load cbsa

# COMMAND ----------

CBSA_ZIP_URL = "https://www2.census.gov/geo/tiger/TIGER2024/CBSA/tl_2024_us_cbsa.zip"

# COMMAND ----------

response = requests.get(CBSA_ZIP_URL, stream=True)
tmp_file_name = 'tl_2024_us_cbsa.zip'
with open(tmp_file_name, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192): 
        if chunk:
            f.write(chunk)

# COMMAND ----------

tmp_dir = datetime.now().strftime("%Y%m%d%H%M%S")
os.makedirs(tmp_dir)

# COMMAND ----------

with zipfile.ZipFile(tmp_file_name, 'r') as zip_ref:
    zip_ref.extractall(tmp_dir)

# COMMAND ----------

data = gpd.read_file(f"{tmp_dir}/{tmp_file_name.replace('.zip', '.shp')}")

# COMMAND ----------

shutil.rmtree(tmp_dir)
os.remove(tmp_file_name)

# COMMAND ----------

data['geometry'] = data['geometry'].apply(lambda geom: geom.wkt)

# COMMAND ----------

data.head()

# COMMAND ----------

spark.createDataFrame(data).write.format("delta").mode("overwrite").saveAsTable('dev_bricks.tiger.cbsa')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tiger.cbsa

# COMMAND ----------

# MAGIC %md
# MAGIC # Counties

# COMMAND ----------

county_url = 'https://www2.census.gov/geo/tiger/TIGER2024/COUNTY/tl_2024_us_county.zip'

# COMMAND ----------

response = requests.get(county_url, stream=True)
tmp_file_name = 'tl_2024_us_county.zip'
with open(tmp_file_name, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192): 
        if chunk:
            f.write(chunk)

# COMMAND ----------

tmp_dir = datetime.now().strftime("%Y%m%d%H%M%S")
os.makedirs(tmp_dir)

# COMMAND ----------

with zipfile.ZipFile(tmp_file_name, 'r') as zip_ref:
    zip_ref.extractall(tmp_dir)

# COMMAND ----------

data = gpd.read_file(f"{tmp_dir}/{tmp_file_name.replace('.zip', '.shp')}")

# COMMAND ----------

shutil.rmtree(tmp_dir)
os.remove(tmp_file_name)

# COMMAND ----------

data['geometry'] = data['geometry'].apply(lambda geom: geom.wkt)

# COMMAND ----------

spark.createDataFrame(data).write.format("delta").mode("overwrite").saveAsTable('dev_bricks.tiger.county')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tiger.county

# COMMAND ----------

# MAGIC %md
# MAGIC # State

# COMMAND ----------

states_url = r'https://www2.census.gov/geo/tiger/TIGER2024/STATE/tl_2024_us_state.zip'

# COMMAND ----------

response = requests.get(states_url, stream=True)
tmp_file_name = 'tl_2024_us_state.zip'
with open(tmp_file_name, 'wb') as f:
    for chunk in response.iter_content(chunk_size=8192): 
        if chunk:
            f.write(chunk)

# COMMAND ----------

tmp_dir = datetime.now().strftime("%Y%m%d%H%M%S")
os.makedirs(tmp_dir)

# COMMAND ----------

with zipfile.ZipFile(tmp_file_name, 'r') as zip_ref:
    zip_ref.extractall(tmp_dir)

# COMMAND ----------

data = gpd.read_file(f"{tmp_dir}/{tmp_file_name.replace('.zip', '.shp')}")

# COMMAND ----------

shutil.rmtree(tmp_dir)
os.remove(tmp_file_name)

# COMMAND ----------

data['geometry'] = data['geometry'].apply(lambda geom: geom.wkt)

# COMMAND ----------

spark.createDataFrame(data).write.format("delta").mode("overwrite").saveAsTable('dev_bricks.tiger.state')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from tiger.state
