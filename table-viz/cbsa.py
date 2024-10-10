# Databricks notebook source
!pip install dash dash-leaflet

# COMMAND ----------

from dash import Dash, html, get_asset_url
import dash_leaflet as dl
import geopandas as gpd
import os

# COMMAND ----------

app = Dash()

data = gpd.GeoDataFrame(spark.sql("select * from tiger.cbsa"))
metro = data[data['LSAD'] == 'M1'].to_file('tmp1.geojson', driver='GeoJSON')
micro = data[data['LSAD'] == 'M2'].to_file('tmp2.geojson', driver='GeoJSON')

app.layout = html.Div([

])

app.run(jupyter_mode='inline')

# COMMAND ----------


