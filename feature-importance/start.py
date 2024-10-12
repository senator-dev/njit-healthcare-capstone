# Databricks notebook source
import folium
import pandas as pd
import requests
import geopandas as gpd
from shapely.wkt import loads
import numpy as np

# COMMAND ----------

q = """
select dc.*, tc.geometry 
from default.cancer_incidence dc
left join tiger.cbsa tc
on dc.msa = tc.NAME
"""

lung_cancer_incidence = spark.sql(q).toPandas()
lung_cancer_incidence['geometry'] = lung_cancer_incidence['geometry'].apply(lambda x: loads(x) if x is not None else np.nan) # why so slow?
lung_cancer_incidence = gpd.GeoDataFrame(lung_cancer_incidence)

# COMMAND ----------


m = folium.Map()
geojson_data = lung_cancer_incidence[lung_cancer_incidence['year'] == '2020'].to_json()
folium.GeoJson(geojson_data).add_to(m)
m

# COMMAND ----------


