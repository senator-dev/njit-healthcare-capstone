# Databricks notebook source
import pandas as pd
from tqdm import tqdm

# COMMAND ----------

# MAGIC %md
# MAGIC https://seer.cancer.gov/popdata/

# COMMAND ----------

colspecs = [(0, 4), (4, 6), (6, 8), (8, 11), (11, 13), (13, 14), (14, 15), (15, 16), (16, 18), (18, 27)]
cols = ['YEARS', 'STATE_ABBREVIATION', 'STATE_FIPS_CODE', 'COUNTY_FIPS_CODE', 'REGISTRY', 'RACE', 'ORIGIN', 'SEX', 'AGE_GROUP', 'POPULATION']
data = pd.read_fwf('https://seer.cancer.gov/popdata/yr1969_2022.19ages/us.1969_2022.19ages.adjusted.txt.gz', colspecs=colspecs, compression='gzip', header=None)
data.columns = cols

# COMMAND ----------

data.head()

# COMMAND ----------

registry_map = {
    '01': 'San Francisco-Oakland SMSA',
    '02': 'Connecticut',
    '20': 'Detroit (Metropolitan)',
    '21': 'Hawaii',
    '22': 'Iowa',
    '23': 'New Mexico',
    '25': 'Seattle (Puget Sound)',
    '26': 'Utah',
    '27': 'Atlanta (Metropolitan)',
    '29': 'Alaska',
    '31': 'San Jose-Monterey',
    '33': 'Arizona',
    '35': 'Los Angeles',
    '37': 'Rural Georgia',
    '41': 'California excluding SF/SJM/LA',
    '42': 'Kentucky',
    '43': 'Louisiana',
    '44': 'New Jersey',
    '47': 'Georgia excluding Atlanta/Rural Georgia',
    '61': 'Idaho',
    '62': 'New York',
    '63': 'Massachusetts',
    '99': 'Registry for non-SEER area'
}

data['REGISTRY'] = data['REGISTRY'].apply(lambda x: registry_map[str(x).zfill(2)])

race_map_ge1965 = {
    '1': 'White',
    '2': 'Black',
    '3': 'Other'
}

race_map_ge1990 = {
    '1': 'White',
    '2': 'Black',
    '3': 'American Indian/Alaska Native',
    '4': 'Asian or Pacific Islander'
}

data['RACE'] = data.apply(lambda x: race_map_ge1965[str(x['RACE'])] if x['YEARS'] <= 1990 else race_map_ge1990[str(x['RACE'])], axis=1)


origin_map = {
    '0': 'Non-Hispanic',
    '1': 'Hispanic',
    '9': 'Not applicable in 1969+ W,B,O files'
}

data['ORIGIN'] = data['ORIGIN'].apply(lambda x: origin_map[str(x)])


sex_map = {
    '1': 'Male',
    '2': 'Female'
}

data['SEX'] = data['SEX'].apply(lambda x: sex_map[str(x)])


age_map = {
  "00": '0 years',
  '01': '1-4 years',
  '02': '5-9 years',
  '03': '10-14 years',
  '04': '15-19 years',
  '05': '20-24 years',
  '06': '25-29 years',
  '07': '30-34 years',
  '08': '35-39 years',
  '09': '40-44 years',
  '10': '45-49 years',
  '11': '50-54 years',
  '12': '55-59 years',
  '13': '60-64 years',
  '14': '65-69 years',
  '15': '70-74 years',
  '16': '75-79 years',
  '17': '80-84 years',
  '18': '85+ years',
}

data['AGE_GROUP'] = data['AGE_GROUP'].apply(lambda x: age_map[str(x).zfill(2)])

# COMMAND ----------

# DBTITLE 1,.
data.head()

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists demographics.seer_annual_county_populations;
# MAGIC
# MAGIC create table demographics.seer_annual_county_populations (
# MAGIC   YEARS int,
# MAGIC   STATE_ABBREVIATION string,
# MAGIC   STATE_FIPS_CODE int,
# MAGIC   COUNTY_FIPS_CODE int,
# MAGIC   REGISTRY string,
# MAGIC   RACE string,
# MAGIC   ORIGIN string,
# MAGIC   SEX string,
# MAGIC   AGE_GROUP string,
# MAGIC   POPULATION int
# MAGIC )

# COMMAND ----------

# data too large must be written in batches
data.shape[0]

# COMMAND ----------

for i in tqdm(range(0, data.shape[0] - 500_000, 500_000)):
  spark.createDataFrame(data.iloc[i: i + 500_000, :]).createOrReplaceTempView('tmp')
  spark.sql("insert into demographics.seer_annual_county_populations select * from tmp")

# COMMAND ----------


