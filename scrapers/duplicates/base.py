# Databricks notebook source
# MAGIC %md
# MAGIC # **!!!DO NOT RUN THIS IF YOU DO NOT KNOW WHAT YOU ARE DOING!!!**

# COMMAND ----------

from itertools import product
import pandas as pd
import numpy as np

# COMMAND ----------


cbsa_name_values = {
    'Albany-Schenectady-Troy, NY': '10580',
    'Albuquerque, NM': '10740',
    'Anchorage, AK': '11260',
    'Atlanta-Sandy Springs-Roswell, GA': '12060',
    'Austin-Round Rock, TX': '12420',
    'Bakersfield, CA': '12540',
    'Baltimore-Columbia-Towson, MD': '12580',
    'Baton Rouge, LA': '12940',
    'Beaver Dam, WI': '13180',
    'Birmingham-Hoover, AL': '13820',
    'Bishop, CA': '13860',
    'Bismarck, ND': '13900',
    'Boise City, ID': '14260',
    'Boston-Cambridge-Newton, MA-NH': '14460',
    'Bowling Green, KY': '14540',
    'Bozeman, MT': '14580',
    'Bridgeport-Stamford-Norwalk, CT': '14860',
    'Buffalo-Cheektowaga-Niagara Falls, NY': '15380',
    'Burlington-South Burlington, VT': '15540',
    'Canton-Massillon, OH': '15940',
    'Champaign-Urbana, IL': '16580',
    'Charleston, WV': '16620',
    'Charlotte-Concord-Gastonia, NC-SC': '16740',
    'Cheyenne, WY': '16940',
    'Chicago-Naperville-Elgin, IL-IN-WI': '16980',
    'Chico, CA': '17020',
    'Cincinnati, OH-KY-IN': '17140',
    'Cleveland-Elyria, OH': '17460',
    'Colorado Springs, CO': '17820',
    'Columbia, SC': '17900',
    'Columbus, OH': '18140',
    'Corning, NY': '18500',
    'Dallas-Fort Worth-Arlington, TX': '19100',
    'Davenport-Moline-Rock Island, IA-IL': '19340',
    'Dayton, OH': '19380',
    'Denver-Aurora-Lakewood, CO': '19740',
    'Des Moines-West Des Moines, IA': '19780',
    'Detroit-Warren-Dearborn, MI': '19820',
    'Durango, CO': '20420',
    'El Centro, CA': '20940',
    'El Paso, TX': '21340',
    'Erie, PA': '21500',
    'Eureka-Arcata-Fortuna, CA': '21700',
    'Evansville, IN-KY': '21780',
    'Fairbanks, AK': '21820',
    'Fort Collins, CO': '22660',
    'Fresno, CA': '23420',
    'Gettysburg, PA': '23900',
    'Grand Junction, CO': '24300',
    'Grand Rapids-Wyoming, MI': '24340',
    'Greeley, CO': '24540',
    'Hartford-West Hartford-East Hartford, CT': '25540',
    'Helena, MT': '25740',
    'Houston-The Woodlands-Sugar Land, TX': '26420',
    'Indianapolis-Carmel-Anderson, IN': '26900',
    'Jackson, MS': '27140',
    'Jackson, WY-ID': '27220',
    'Jacksonville, FL': '27260',
    'Johnstown, PA': '27780',
    'Kansas City, MO-KS': '28140',
    'Knoxville, TN': '28940',
    'Laredo, TX': '29700',
    'Las Vegas-Henderson-Paradise, NV': '29820',
    'Little Rock-North Little Rock-Conway, AR': '30780',
    'Los Angeles-Long Beach-Anaheim, CA': '31080',
    'Louisville/Jefferson County, KY-IN': '31140',
    'Manchester-Nashua, NH': '31700',
    'Memphis, TN-MS-AR': '32820',
    'Miami-Fort Lauderdale-West Palm Beach, FL': '33100',
    'Milwaukee-Waukesha-West Allis, WI': '33340',
    'Minneapolis-St. Paul-Bloomington, MN-WI': '33460',
    'Modesto, CA': '33700',
    'Napa, CA': '34900',
    'Nashville-Davidson--Murfreesboro--Franklin, TN': '34980',
    'New Haven-Milford, CT': '35300',
    'New Orleans-Metairie, LA': '35380',
    'New York-Newark-Jersey City, NY-NJ-PA': '35620',
    'Ogden-Clearfield, UT': '36260',
    'Oklahoma City, OK': '36420',
    'Olympia-Tumwater, WA': '36500',
    'Omaha-Council Bluffs, NE-IA': '36540',
    'Orlando-Kissimmee-Sanford, FL': '36740',
    'Philadelphia-Camden-Wilmington, PA-NJ-DE-MD': '37980',
    'Phoenix-Mesa-Scottsdale, AZ': '38060',
    'Pittsburgh, PA': '38300',
    'Ponce, PR': '38660',
    'Port Angeles, WA': '38820',
    'Portland-South Portland, ME': '38860',
    'Portland-Vancouver-Hillsboro, OR-WA': '38900',
    'Providence-Warwick, RI-MA': '39300',
    'Provo-Orem, UT': '39340',
    'Raleigh, NC': '39580',
    'Reno, NV': '39900',
    'Richmond, VA': '40060',
    'Riverside-San Bernardino-Ontario, CA': '40140',
    'Roanoke, VA': '40220',
    'Rochester, NY': '40380',
    'Rutland, VT': '40860',
    'Sacramento--Roseville--Arden-Arcade, CA': '40900',
    'St. Louis, MO-IL': '41180',
    'Salinas, CA': '41500',
    'Salt Lake City, UT': '41620',
    'San Antonio-New Braunfels, TX': '41700',
    'San Diego-Carlsbad, CA': '41740',
    'San Francisco-Oakland-Hayward, CA': '41860',
    'San Jose-Sunnyvale-Santa Clara, CA': '41940',
    'San Juan-Carolina-Caguas, PR': '41980',
    'Santa Maria-Santa Barbara, CA': '42200',
    'Santa Rosa, CA': '42220',
    'Scranton--Wilkes-Barre--Hazleton, PA': '42540',
    'Seattle-Tacoma-Bellevue, WA': '42660',
    'Sioux Falls, SD': '43620',
    'Springfield, MA': '44140',
    'Stockton-Lodi, CA': '44700',
    'Tallahassee, FL': '45220',
    'Tampa-St. Petersburg-Clearwater, FL': '45300',
    'Torrington, CT': '45860',
    'Tucson, AZ': '46060',
    'Tulsa, OK': '46140',
    'Urban Honolulu, HI': '46520',
    'Vallejo-Fairfield, CA': '46700',
    'Vernal, UT': '46860',
    'Virginia Beach-Norfolk-Newport News, VA-NC': '47260',
    'Waco, TX': '47380',
    'Washington-Arlington-Alexandria, DC-VA-MD-WV': '47900',
    'Wheeling, WV-OH': '48540',
    'Worcester, MA-CT': '49340'
}

pollutant_values = {
    'CO': '42101',
    'Pb': "12128','14129','85129",
    'NO2': '42602',
    'Ozone': '44201',
    'PM10': '81102',
    'PM2.5': "88101','88502",
    'SO2': '42401'
}
years = list(range(1980, 2025))

# COMMAND ----------

data = pd.DataFrame(list(product(cbsa_name_values.keys(), pollutant_values.keys(), years)), columns=['cbsa', 'pollutant', 'year'])

# COMMAND ----------

data.shape[0]

# COMMAND ----------

n_buckets = int(data.shape[0] / 2666)
data['bucket'] = np.random.permutation(data.shape[0]) % n_buckets

# COMMAND ----------

data['bucket'].nunique()

# COMMAND ----------

data['index'] = list(range(data.shape[0]))

# COMMAND ----------

# MAGIC %sql drop table if exists air_quality_data

# COMMAND ----------

spark.createDataFrame(data).write.mode('overwrite').saveAsTable('air_quality_data_base')

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC drop table if exists air_pollutants;
# MAGIC
# MAGIC create table air_pollutants (
# MAGIC   date date,
# MAGIC   source string,
# MAGIC   site_id int,
# MAGIC   poc int,
# MAGIC   value float,
# MAGIC   units string,
# MAGIC   daily_aqi_value int,
# MAGIC   local_site_name string,
# MAGIC   daily_obs_count int,
# MAGIC   percent_complete float,
# MAGIC   aqs_parameter_code int,
# MAGIC   aqs_parameter_description string,
# MAGIC   method_code int,
# MAGIC   cbsa_code int,
# MAGIC   cbsa_name string,
# MAGIC   state_fips_code int,
# MAGIC   state string,
# MAGIC   county_fips_code int,
# MAGIC   county string,
# MAGIC   site_latitude float,
# MAGIC   site_longitude float
# MAGIC );

# COMMAND ----------


