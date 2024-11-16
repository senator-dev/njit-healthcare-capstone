# Databricks notebook source
# MAGIC %md
# MAGIC https://www.epa.gov/waterdata/waters-geospatial-data-downloads#NationalGeospatialDatasets

# COMMAND ----------

import geopandas as gpd
from tqdm import tqdm

# COMMAND ----------



# COMMAND ----------

db_url = "/Volumes/dev_bricks/files/raw/Beaches OW Linked Dataset/beach_20240229.gpkg"

# COMMAND ----------

layers = ['beach_src_p', 'beach_src_l', 'beach_src_a', 'beach_cip_geo', 'beach_huc12_geo', 'beach_sfid', 'beach_rad_p', 'beach_rad_l', 'beach_rad_a', 'beach_attr', 'beach_cip', 'beach_huc12', 'beach_rad_evt2meta', 'beach_rad_metdata', 'beach_rad_srccit']

# COMMAND ----------

dfs = {}
for layer in tqdm(layers):
  dfs[layer] = gpd.read_file(db_url, layer=layer)

# COMMAND ----------

for layer in layers:
  if not dfs[layer].empty:
    print(layer)

# COMMAND ----------

spark.createDataFrame(dfs['beach_attr']).write.format('delta').mode('overwrite').saveAsTable(f"beach_{'beach_attr'}")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from beach_beach_attr
# MAGIC

# COMMAND ----------

layers = ['attains_au_areas', 'attains_au_lines', 'attains_au_points', 'attains_au_catchments', 'attains_au_attributes', 'attains_au_control', 'attains_au_catchassmnt', 'attains_au_huc12assmnt', 'attains_au_assmnt_parms', 'attains_au_water_types', 'attains_au_meta']

# COMMAND ----------

attains_url = 'https://drive.google.com/file/d/1eZCEO4qyIDrrM-zD3BLk3pRmQeezVQkB/view?usp=drive_link'

# COMMAND ----------

attains = gpd.read_file(attains_url, layer=layers[0])

# COMMAND ----------


import requests
import zipfile
import io


tmp_path = 'attains.gpkg'

response = requests.get(url)

with zipfile.ZipFile(io.BytesIO(response.content)) as z:
  for file_name in z.namelist():
    if file_name.endswith('.gpkg'):
      with open(tmp_path, 'wb') as f:
        f.write(z.read(file_name))

# COMMAND ----------

layers = ['beach_src_p', 'beach_src_l', 'beach_src_a', 'beach_cip_geo', 'beach_huc12_geo', 'beach_sfid', 'beach_rad_p', 'beach_rad_l', 'beach_rad_a', 'beach_attr', 'beach_cip', 'beach_huc12', 'beach_rad_evt2meta', 'beach_rad_metdata', 'beach_rad_srccit']

response = requests.get(url)


tmp_path = 'beach.gpkg'

url = 'https://dmap-data-commons-ow.s3.amazonaws.com/data/beach_20240228_gpkg.zip'

response = requests.get(url)

with zipfile.ZipFile(io.BytesIO(response.content)) as z:
  for file_name in z.namelist():
    if file_name.endswith('.gpkg'):
      with open(tmp_path, 'wb') as f:
        f.write(z.read(file_name))

# COMMAND ----------



# COMMAND ----------

dfs = {}
for layer in layers:
  dfs[layer] = gpd.read_file(tmp_path, layer=layer)

# COMMAND ----------

dfs['beach_src_l']

# COMMAND ----------

# MAGIC %md
# MAGIC BEACH Program Data

# COMMAND ----------


response = requests.get(url)

with zipfile.ZipFile(io.BytesIO(response.content)) as z:
  for file_name in z.namelist():
    if file_name.endswith('.gpkg'):
      with open(tmp_path, 'wb') as f:
        f.write(z.read(file_name))

# COMMAND ----------


