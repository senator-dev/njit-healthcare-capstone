# Databricks notebook source
# MAGIC %md
# MAGIC https://wonder.cdc.gov/cancer-v2021.HTML

# COMMAND ----------

import pandas as pd
import numpy as np

# COMMAND ----------

data = pd.read_csv('/Volumes/dev_bricks/files/raw/United States and Puerto Rico Cancer Statistics, 1999-2021 Incidence.txt', delimiter='\t')

# COMMAND ----------

# clean data
dropCols = ['Notes', 'Cancer Sites Code', 'States Code', 'Year Code', 'Age Groups Code'] 
renameCols = {'Cancer Sites': 'Diagnosis', 'States': 'State', 'Count':'Number of Incidences'}
renameDiagnosis = {'Urinary Bladder, invasive and in situ': 'Urinary Bladder'}

data = data.drop(columns=dropCols) # Notes: has NaN values until the end, where it has notes on the data; Others: redundant or useless info
data = data.dropna() # removes rows with NaN values (caused by notes)
data['Year'] = data['Year'].astype(int) # converts year from float to int
data = data.rename(columns=renameCols)
data['Diagnosis'] = data['Diagnosis'].replace(renameDiagnosis)
data['Number of Incidences'] = pd.to_numeric(data['Number of Incidences'], errors='coerce')
print(data)
print(f"Shape: {data.shape}")

# COMMAND ----------

spark.createDataFrame(data.rename(columns={column: column.replace(' ', '_') for column in data})).write.format('delta').mode('overwrite').saveAsTable('dev_bricks.demographics.wonder_incidence')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from demographics.wonder_incidence
