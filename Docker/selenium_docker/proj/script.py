from utils import create_driver, MSA_SELECT_ID, MSA_OPTION_ID, YEAR_SELECT_ID, SEX_SELECT_ID, AGE_GROUP_SELECT_ID, ETHNICITY_SELECT_ID, RACE_SELECT_ID, CANCER_SITES_SELECT_ID, FOOTER_BUTTONS_CLASS, SEND_BUTTON_VALUE, URL, DESELECT_DICT
from selenium.webdriver.common.by import By
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd

inputs_path = os.sys.argv[1]
outputs_path = os.sys.argv[2]


def select_msa_option(driver):
    msa_option = driver.find_element(By.ID, MSA_OPTION_ID)
    msa_option.click()

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
        print(' | '.join(map(lambda x: f"{str(x)[:8]:>8}", row)), end=' --> ', flush=True)
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
        print('Success \u263a', flush=True)
        return count
    except Exception as error:
        print(error)
        print('Failed  \u2639', flush=True)
        return -1


def download_multiple(bucket):
    print(f"{'#' * 40} Processing bucket {bucket} {'#' * 40} ", flush=True)
    try:
        driver = create_driver()
        data = pd.read_csv(os.path.join(inputs_path, f'{bucket}.csv'))
        driver.get(URL)
        data['count'] = data[['code.msa', 'code.year', 'code.sex', 'code.age_group', 'code.ethnicity', 'code.race', 'code.cancer_site']].apply(one_download, driver=driver, axis=1)
        data.to_csv(os.path.join(outputs_path, f'{bucket}_counted.csv'))
        print(f"{'#' * 40} Finished bucket {bucket} {'#' * 40} ", flush=True)
    except Exception as error:
        print(f"{'#' * 40} Failed bucket {bucket} {'#' * 40} ", flush=True)


buckets = [file.split('.')[0] for file in os.listdir(inputs_path)]

with ThreadPoolExecutor(max_workers=46) as executor:
    futures = {executor.submit(download_multiple, bucket): bucket for bucket in buckets}
    for future in as_completed(futures):
        bucket = futures[future]
        try:
            future.result() 
        except Exception as err:
            print(f'Bucket {bucket} generated an exception: {err}', flush=True)

