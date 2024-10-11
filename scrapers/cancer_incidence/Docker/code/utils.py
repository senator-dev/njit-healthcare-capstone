from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.common.by import By

URL = "https://wonder.cdc.gov/cancer-v2021.HTML"


def create_driver():
    chrome_options = Options()
    service = Service()
    chrome_options.add_argument("--headless")  
    chrome_options.add_argument("--disable-gpu")  
    chrome_options.add_argument("--window-size=1920,1080")  
    chrome_options.add_argument("--disable-extensions")  
    chrome_options.add_argument("--no-sandbox")  
    chrome_options.add_argument("--disable-dev-shm-usage") 
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

MSA_SELECT_ID = "SD198.V3"
MSA_OPTION_ID = "RO_locationD198.V3"
YEAR_SELECT_ID = "SD198.V1"
SEX_SELECT_ID = "SD198.V9"
AGE_GROUP_SELECT_ID = "SD198.V5"
ETHNICITY_SELECT_ID = "SD198.V6"
RACE_SELECT_ID = "SD198.V4"
CANCER_SITES_SELECT_ID = "SD198.V8"
FOOTER_BUTTONS_CLASS = "footer-buttons"
SEND_BUTTON_VALUE = "Send"


def select_msa_option(driver):
    msa_option = driver.find_element(By.ID, MSA_OPTION_ID)
    msa_option.click()