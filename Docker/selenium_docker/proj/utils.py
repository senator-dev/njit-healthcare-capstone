from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
import os
import numpy as np
import pandas as pd
import concurrent.futures


URL = "https://wonder.cdc.gov/cancer-v2021.HTML"


def create_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")  
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    service = Service()
    driver = webdriver.Chrome(options=chrome_options, service=service)
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


DESELECT_DICT = {
    MSA_SELECT_ID: "10900",
    YEAR_SELECT_ID: "2002",
    SEX_SELECT_ID: "F",
    AGE_GROUP_SELECT_ID: "15-19",
    ETHNICITY_SELECT_ID: "2186-5",
    RACE_SELECT_ID: "2054-5",
    CANCER_SITES_SELECT_ID: "20030"
}
