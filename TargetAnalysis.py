# Databricks notebook source
import pandas as pd
import geopandas as gpd
from sklearn.preprocessing import LabelEncoder
import plotly.graph_objects as go
import folium
import numpy as np
from shapely.wkt import loads

# COMMAND ----------

bladder_cancer_incidence = spark.sql("select * from incidence where cancer_site = 'Urinary Bladder'").toPandas()


# COMMAND ----------

bladder_cancer_incidence.head()

# COMMAND ----------

data = bladder_cancer_incidence[['year', 'state', 'age_group', 'count']].groupby(['state', 'year'])['count'].sum().reset_index()

# COMMAND ----------

data.head()

# COMMAND ----------

tiger_states = spark.sql("select * from tiger.state").toPandas()[['STATEFP', 'NAME', 'geometry']].rename(columns={'NAME': 'state'})

# COMMAND ----------

tiger_states.head()

# COMMAND ----------

tiger_counties = spark.sql("select * from tiger.county").toPandas()[['STATEFP', 'COUNTYFP', 'NAME', 'geometry']].rename(columns={'NAME': 'county'})
tiger_counties['geometry'] = tiger_counties['geometry'].apply(loads)
tiger_counties = gpd.GeoDataFrame(tiger_counties, geometry='geometry', crs='EPSG:4326')
tiger_counties['area'] = tiger_counties['geometry'].to_crs(epsg=3857).area * 3.861e-7

# COMMAND ----------

tiger_counties.head()

# COMMAND ----------

q = """
select STATE_FIPS_CODE, COUNTY_FIPS_CODE, YEARS, sum(POPULATION) as Population
from demographics.seer_annual_county_populations
group by STATE_FIPS_CODE, COUNTY_FIPS_CODE, YEARS
"""

county_populations = spark.sql(q).toPandas().rename(columns={'STATE_FIPS_CODE': 'STATEFP', 'COUNTY_FIPS_CODE': 'COUNTYFP', 'YEARS': 'year'})

# COMMAND ----------

county_populations.head()

# COMMAND ----------

county_populations['STATEFP'] = county_populations['STATEFP'].astype(str)
county_populations['COUNTYFP'] = county_populations['COUNTYFP'].astype(str)

tiger_counties['STATEFP'] = tiger_counties['STATEFP'].astype(str)
tiger_counties['COUNTYFP'] = tiger_counties['COUNTYFP'].astype(str)


# COMMAND ----------

merged = county_populations.merge(tiger_counties[['STATEFP', 'COUNTYFP', 'area']], on=['STATEFP', 'COUNTYFP'], how='left')
merged['pop_density'] = merged['Population'] / merged['area']

# COMMAND ----------

def rank_pop_dens(x):
  x['pop_density'] = (x['pop_density'] - x['pop_density'].min()) / (x['pop_density'].max() - x['pop_density'].min())
  return x

# COMMAND ----------

annual_county_population_densities = merged.groupby('year', group_keys=False)[['pop_density', 'COUNTYFP', 'STATEFP', 'year']].apply(rank_pop_dens)

# COMMAND ----------

annual_county_population_densities.head()

# COMMAND ----------

county_states = tiger_counties.merge(tiger_states, on=['STATEFP'], how='left')
county_states['intersection'] = gpd.GeoSeries(county_states['geometry_x'].apply(loads)).intersects(gpd.GeoSeries(county_states['geometry_y'].apply(loads)))

# COMMAND ----------



# COMMAND ----------




# COMMAND ----------

county_populations.head()

# COMMAND ----------



# COMMAND ----------

bladder_cancer_incidence.merge(county_populations, on=['STATE_FIPS_CODE', 'COUNTY_FIPS_CODE', 'YEARS']).head()
