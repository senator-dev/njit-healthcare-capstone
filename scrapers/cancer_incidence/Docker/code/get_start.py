from selenium.webdriver.common.by import By
import re
import time
import os
import numpy as np
import pandas as pd
from itertools import product
import concurrent.futures
from utils import URL, create_driver, MSA_SELECT_ID, MSA_OPTION_ID, YEAR_SELECT_ID, SEX_SELECT_ID, AGE_GROUP_SELECT_ID, ETHNICITY_SELECT_ID, RACE_SELECT_ID, CANCER_SITES_SELECT_ID, FOOTER_BUTTONS_CLASS, SEND_BUTTON_VALUE, select_msa_option

def get_select_options(select_id, driver):
    select = driver.find_element(By.ID, select_id)
    options = select.find_elements(By.XPATH, "./*")
    value_options = {option.get_property('value'): option for option in options}
    name_values = {re.sub(r'\(\d+\)', '', option.text).strip(): option.get_property('value') for option in options}
    return name_values 


driver = create_driver()
driver.get(URL) 

select_msa_option(driver)
msa_name_values = get_select_options(MSA_SELECT_ID, driver)
year_name_values = get_select_options(YEAR_SELECT_ID, driver)
sex_name_values = get_select_options(SEX_SELECT_ID, driver)
age_group_name_values = get_select_options(AGE_GROUP_SELECT_ID, driver)
ethnicity_name_values = get_select_options(ETHNICITY_SELECT_ID, driver)
race_name_values = get_select_options(RACE_SELECT_ID, driver)
cancer_site_name_values = get_select_options(CANCER_SITES_SELECT_ID, driver)

driver.quit()

msa_name_values.pop('The United States')
year_name_values.pop("All Years")
sex_name_values.pop("All Sexes")
age_group_name_values.pop("All Ages")
ethnicity_name_values.pop("All Ethnicities")
race_name_values.pop("All Races")
cancer_site_name_values = {"Gallbladder": cancer_site_name_values["Gallbladder"], "Lung and Bronchus": cancer_site_name_values["Lung and Bronchus"]}

data = pd.DataFrame(list(product(msa_name_values, year_name_values, sex_name_values, age_group_name_values, ethnicity_name_values, race_name_values, cancer_site_name_values)), columns=['msa', 'year', 'sex', 'age_group', 'ethnicity', 'race', 'cancer_site'])
data['code.msa'] = data['msa'].apply(lambda x: msa_name_values[x])
data['code.year'] = data['year'].apply(lambda x: year_name_values[x])
data['code.sex'] = data['sex'].apply(lambda x: sex_name_values[x])
data['code.age_group'] = data['age_group'].apply(lambda x: age_group_name_values[x])
data['code.ethnicity'] = data['ethnicity'].apply(lambda x: ethnicity_name_values[x])
data['code.race'] = data['race'].apply(lambda x: race_name_values[x])
data['code.cancer_site'] = data['cancer_site'].apply(lambda x: cancer_site_name_values[x])
n_buckets = int(data.shape[0] / 47)
data['bucket'] = np.random.permutation(data.shape[0]) % n_buckets

for bucket in data['bucket'].unique():
    data[data['bucket'] == bucket].to_csv(f'csvs/{bucket}.csv')

