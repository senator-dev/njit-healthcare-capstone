# Databricks notebook source
from utils import download_one, URL, create_driver
import pandas as pd

# COMMAND ----------

BUCKET = 2

# COMMAND ----------

driver = create_driver()

# COMMAND ----------

data = spark.sql("select * from air_quality_data_base").toPandas()
data = data[data['bucket'].isin([6, 7, 8])]
data = data.iloc[1035 + 941:, :]

# COMMAND ----------

failed = []
tmp = pd.DataFrame()
for flag, (i, row) in enumerate(data.iterrows()):
    print(f"Processing {flag} . . .", end=' ')
    print(f"CBSA: {row['cbsa']}, Pollutant: {row['pollutant']}, Year: {row['year']}", end=' ')
    try:
        df = download_one(row['pollutant'], row['year'], row['cbsa'], driver)
        df['site_latitude'] = pd.to_numeric(df['site_latitude'], errors='coerce')
        df['site_longitude'] = pd.to_numeric(df['site_longitude'], errors='coerce')

        df['site_id'] = pd.to_numeric(df['site_id'], errors='coerce')
        df['poc'] = pd.to_numeric(df['poc'], errors='coerce')
        df['value'] = pd.to_numeric(df['value'], errors='coerce')
        df['daily_aqi_value'] = pd.to_numeric(df['daily_aqi_value'], errors='coerce')
        df['Daily Obs Count'] = pd.to_numeric(df['Daily Obs Count'], errors='coerce')
        df['percent_complete'] = pd.to_numeric(df['percent_complete'], errors='coerce')
        df['aqs_parameter_code'] = pd.to_numeric(df['aqs_parameter_code'], errors='coerce')
        df['method_code'] = pd.to_numeric(df['method_code'], errors='coerce')
        df['cbsa_code'] = pd.to_numeric(df['cbsa_code'], errors='coerce')
        df['state_fips_code'] = pd.to_numeric(df['state_fips_code'], errors='coerce')
        df['county_fips_code'] = pd.to_numeric(df['county_fips_code'], errors='coerce')


        tmp = pd.concat([tmp, df])
        print(f"Tmp Size: {tmp.shape[0]}", end=' ')
        if tmp.shape[0] > 10_000:
            print('Writting to table . . .', end=' ')
            spark.createDataFrame(tmp).createOrReplaceTempView("new_insert")
            spark.sql(f"""
                insert into default.air_pollutants
                select * from new_insert          
            """)
            tmp = pd.DataFrame()
        print('Finished')

    except Exception as err:
        print('Failed')
        failed.append([flag, i, err])
        driver.quit()
        driver = create_driver()
        driver.get(URL)
    print()


    

# COMMAND ----------

failed

# COMMAND ----------

driver.quit()

# COMMAND ----------



# COMMAND ----------


