from selenium.webdriver.common.by import By
from utils import URL, create_driver, MSA_SELECT_ID, MSA_OPTION_ID, YEAR_SELECT_ID, SEX_SELECT_ID, AGE_GROUP_SELECT_ID, ETHNICITY_SELECT_ID, RACE_SELECT_ID, CANCER_SITES_SELECT_ID, FOOTER_BUTTONS_CLASS, SEND_BUTTON_VALUE, select_msa_option
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

DESELECT_DICT = {
    MSA_SELECT_ID: "10900",
    YEAR_SELECT_ID: "2002",
    SEX_SELECT_ID: "F",
    AGE_GROUP_SELECT_ID: "15-19",
    ETHNICITY_SELECT_ID: "2186-5",
    RACE_SELECT_ID: "2054-5",
    CANCER_SITES_SELECT_ID: "20030"
}

def select_send(driver):
    footer_buttons = driver.find_element(By.CLASS_NAME, FOOTER_BUTTONS_CLASS)
    send_button = footer_buttons.find_element(By.XPATH, f'//*[@value="{SEND_BUTTON_VALUE}"]')
    send_button.click()

def select_option(select_id, option_value, driver):
    select = driver.find_element(By.ID, select_id)
    option = driver.find_element(By.XPATH, f'//*[@value="{option_value}"]')
    deselect_option = driver.find_element(By.XPATH, f'//*[@value="{DESELECT_DICT[select_id]}"]')

    select.click()
    deselect_option.click()
    option.click()


def one_download(row, driver):
    try:
        msa, year, sex, age_group, ethnicity, race, cancer_site = row
        select_msa_option(driver)
        select_option(MSA_SELECT_ID, msa, driver)
        select_option(YEAR_SELECT_ID, year, driver)
        select_option(SEX_SELECT_ID, sex, driver)
        select_option(AGE_GROUP_SELECT_ID, age_group, driver)
        select_option(ETHNICITY_SELECT_ID, ethnicity, driver)
        select_option(RACE_SELECT_ID, race, driver)
        select_option(CANCER_SITES_SELECT_ID, cancer_site, driver)
        select_send(driver)

        try:
            driver.find_element(By.ID, 'error-messages') 
            count = 0
        except Exception as err:
            table = driver.find_element(By.CLASS_NAME, 'response-form')
            count = int(table.find_element(By.TAG_NAME, 'td').text)

        driver.get(URL)
        return count
    except Exception as err:
        return -1

def download_multiple(bucket):
    print(f'Processing {bucket}')
    df = pd.read_csv(f'csvs/{bucket}.csv')
    driver = create_driver()
    driver.get(URL)
    code_cols = ['code.msa', 'code.year', 'code.sex', 'code.age_group', 'code.ethnicity', 'code.race', 'code.cancer_site']
    df['count'] = df[code_cols].apply(one_download, driver=driver, axis=1)
    df.drop(columns=code_cols).to_csv(f'outputs/{bucket}.csv')
    driver.quit()
    print(f'Finished {bucket}')


buckets  = [int(file.split('.')[0]) for file in os.listdir('csvs')]

with ThreadPoolExecutor(max_workers=48) as executor:
    futures = {executor.submit(download_multiple, bucket): bucket for bucket in buckets}
    for future in as_completed(futures):
        bucket = futures[future]
        try:
            future.result()  # Wait for the bucket processing to complete
        except Exception as err:
            print(f'Bucket {bucket} generated an exception: {err}')