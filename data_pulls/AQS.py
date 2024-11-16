# Databricks notebook source
import pandas as pd
from tqdm import tqdm
import requests
import zipfile
import io

# COMMAND ----------

# MAGIC %md
# MAGIC https://aqs.epa.gov/aqsweb/airdata/download_files.html

# COMMAND ----------

import requests

# COMMAND ----------

data = pd.DataFrame()
for year in tqdm(range(1980, 2025)):
  url = f'https://aqs.epa.gov/aqsweb/airdata/annual_aqi_by_county_{year}.zip'
  try:
    response = requests.get(url)
    with zipfile.ZipFile(io.BytesIO(response.content)) as z:
      for file_name in z.namelist():
        with z.open(file_name) as file:
          tmp = pd.read_csv(file)

        data = pd.concat([data, tmp])
  except Exception as e:
    print(year, e)

# COMMAND ----------

data

# COMMAND ----------

spark.createDataFrame(data.rename(columns={column: column.replace(' ', '_')for column in data})).write.format('delta').mode('overwrite').saveAsTable('annual_aqs_county')
