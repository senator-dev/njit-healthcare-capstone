# Databricks notebook source
# MAGIC %md
# MAGIC Source: https://data-nifc.opendata.arcgis.com/search?tags=historic_wildlandfire_opendata%2CCategory

# COMMAND ----------

import geopandas as gpd
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC [IMSR Incident Locations View: Final Occurrence](https://data-nifc.opendata.arcgis.com/datasets/85d3f50b5eee4dcfa48f5e4fb23aa9e1_0/explore)
# MAGIC - A filtered subset of the 'LAST' or 'ONLY' incident locations (its final occurrence) and attributes from the IMSR incident locations (points) provided by the National Interagency Coordination Center (NICC) for the published IMSR PDF documents.
# MAGIC - Documentation on features: https://data-nifc.opendata.arcgis.com/datasets/nifc::imsr-incident-locations-view-final-occurrence/about

# COMMAND ----------

data_IMSR = gpd.read_file('/Volumes/dev_bricks/files/raw/fire/IMSR_Incident_Locations View Final Occurrence/IMSR_Incident_Locations.shp')

# COMMAND ----------

# show all the columns names
print(data_IMSR.columns)

# select relevant columns & show dimensions
df_IMSR = data_IMSR.drop("geometry", axis=1)
print("Shape:", df_IMSR.shape)
df_IMSR.head()

# COMMAND ----------

spark.createDataFrame(df_IMSR).write.format('delta').mode('overwrite').saveAsTable('dev_bricks.fire.IMSR_Incident_Locations_View_Final_Occurrence')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fire.IMSR_Incident_Locations_View_Final_Occurrence

# COMMAND ----------

# MAGIC %md
# MAGIC [Historic Perimeters Combined 2000-2018 GeoMAC](https://data-nifc.opendata.arcgis.com/datasets/ef25d7e8c9f3499ba9e3d8e09606e488_0/explore)
# MAGIC - This geospatial dataset contains wildfire perimeter data between the years of 2000 - 2018 derived from the GeoMAC application.
# MAGIC - Documentation on features: https://data-nifc.opendata.arcgis.com/datasets/nifc::historic-perimeters-combined-2000-2018-geomac/about

# COMMAND ----------

data_HistoricPerimeters = gpd.read_file('/Volumes/dev_bricks/files/raw/fire/Historic Perimeters Combined 2000-2018 GeoMAC/US_HIST_FIRE_PERIMTRS_2000_2018_DD83.shp')

# COMMAND ----------

# show all the columns names
print(data_HistoricPerimeters.columns)

# select relevant columns
df_HistoricPerimeters = data_HistoricPerimeters.drop("geometry", axis=1)
print("Shape:", df_HistoricPerimeters.shape)
df_HistoricPerimeters.head()

# COMMAND ----------

spark.createDataFrame(df_HistoricPerimeters).write.format('delta').mode('overwrite').saveAsTable('dev_bricks.fire.Historic_Perimeters_Combined_2000_2018_GeoMAC')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fire.Historic_Perimeters_Combined_2000_2018_GeoMAC

# COMMAND ----------

# MAGIC %md
# MAGIC [WFIGS Interagency Fire Perimeters](https://data-nifc.opendata.arcgis.com/datasets/5e72b1699bf74eefb3f3aff6f4ba5511_0/explore)
# MAGIC - Best available perimeters for all known wildland fires in the United States. This data set is an ongoing project and as such, may be incomplete.
# MAGIC - Documentation on features: https://data-nifc.opendata.arcgis.com/datasets/nifc::wfigs-interagency-fire-perimeters/about

# COMMAND ----------

data_WFIGS = gpd.read_file('/Volumes/dev_bricks/files/raw/fire/WFIGS Interagency Fire Perimeters/Perimeters.shp')

# COMMAND ----------

# show all the columns names
for i in range(len(data_WFIGS.columns)):
    print(data_WFIGS.columns[i])
# column names are getting cut off

# select & rename relevant columns
cols = ["attr_Conta", "attr_Contr", "attr_Incid", "attr_Disco", "attr_Final", "attr_Inc_2", "attr_Initi", "attr_Ini_1", "attr_POOCo", "attr_POOSt", "attr_Predo", "attr_Pre_1", "attr_Prima", "attr_Secon"]
renameCols = {"attr_Incid": "attr_IncidentSize", "attr_Disco": "attr_DiscoveryAcres", "attr_Final": "attr_FinalAcres", "attr_Inc_2": "attr_IncidentName",
              "attr_Initi": "attr_InitialLatitude", "attr_Ini_1": "attr_InitialLongitude", "attr_POOCo": "attr_POOCounty", "attr_POOSt": "attr_POOState",
              "attr_Predo": "attr_PredominantFuelGroup", "attr_Pre_1": "attr_PredominantFuelModel", "attr_Prima": "attr_PrimaryFuelModel", 
              "attr_Secon": "attr_SecondaryFuelModel", "attr_Conta": "attr_ContainmentDateTime", "attr_Contr": "attr_ControlDateTime"}
df_WFIGS = data_WFIGS[cols]
df_WFIGS = df_WFIGS.rename(columns=renameCols)
print("Shape:", df_WFIGS.shape)
print("Columns: ", df_WFIGS.columns)
df_WFIGS.head()

# COMMAND ----------

spark.createDataFrame(df_WFIGS).write.format('delta').mode('overwrite').saveAsTable('dev_bricks.fire.WFIGS_Interagency_Fire_Perimeters')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fire.WFIGS_Interagency_Fire_Perimeters

# COMMAND ----------

# MAGIC %md
# MAGIC [Wildland Fire Incident Locations](https://data-nifc.opendata.arcgis.com/datasets/b4402f7887ca4ea9a6189443f220ef28_0/explore)
# MAGIC - Point Locations for all wildland fires in the United States reported to the IRWIN system.
# MAGIC - Documentation on features: https://data-nifc.opendata.arcgis.com/datasets/nifc::wildland-fire-incident-locations/about

# COMMAND ----------

data_Wildland = gpd.read_file('/Volumes/dev_bricks/files/raw/fire/Wildland Fire Incident Locations/Incidents.shp')
# getting decoding error, so stopped

# COMMAND ----------

# MAGIC %md
# MAGIC [InterAgencyFirePerimeterHistory All Years View](https://data-nifc.opendata.arcgis.com/datasets/e02b85c0ea784ce7bd8add7ae3d293d0_0/explore)
# MAGIC - Interagency Wildland Fire Perimeter History includes perimeters thru 2023 fires, update complete 8/25/2024. Data maintained by the Wildland Fire Management Research, Development, & Application program data team: wfmrda.datasupport@firenet.gov 
# MAGIC - Documentation on features: https://data-nifc.opendata.arcgis.com/datasets/nifc::interagencyfireperimeterhistory-all-years-view/about

# COMMAND ----------

data_InterAgency = gpd.read_file('/Volumes/dev_bricks/files/raw/fire/InterAgencyFirePerimeterHistory_All_Years_View/InterAgencyFirePerimeterHistory_All_Years_View.shp')

# COMMAND ----------

# show all the columns names
print(data_InterAgency.columns)

# select relevant columns
df_InterAgency = data_InterAgency.drop("geometry", axis=1)
print("Shape:", df_InterAgency.shape)
df_InterAgency.head()

# COMMAND ----------

spark.createDataFrame(df_InterAgency).write.format('delta').mode('overwrite').saveAsTable('dev_bricks.fire.InterAgencyFirePerimeterHistory_All_Years_View')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from fire.InterAgencyFirePerimeterHistory_All_Years_View

# COMMAND ----------

# MAGIC %md
# MAGIC [InFORM Fire Occurrence Data Records](https://data-nifc.opendata.arcgis.com/datasets/60a94840152b4a89bec467a9f052f135_0/explore)
# MAGIC - Incident records for all known fire occurrence in the United States shown as point locations
# MAGIC - Documentation on features: https://data-nifc.opendata.arcgis.com/datasets/nifc::inform-fire-occurrence-data-records/about
# MAGIC
# MAGIC [WEIRD FORMAT IN RAW DATA, SO SKIPPED]

# COMMAND ----------

# MAGIC %md
# MAGIC Historic Geomac Perimeters All Years 2000 2018 gdb
# MAGIC - This geospatial dataset contains wildfire perimeters from 2000 - 2018 derived from the GeoMAC application.
# MAGIC - Documentation on features (limited): https://data-nifc.opendata.arcgis.com/datasets/5b3ff19978be49208d41a9d9a461ecfb/about
# MAGIC
# MAGIC [WEIRD FORMAT IN RAW DATA, SO SKIPPED]
