import pandas as pd
from itertools import product
import requests
import os
import zipfile


def years() -> list:
    return list(range(1980, 2025))

def categories() -> dict:
    return {
        "44201": "Ozone",
        "42401": "SO2",
        "42101": "CO",
        "42602": "NO2",
        "88101": "PM2.5 FRM/FEM Mass",
        "88502": "PM2.5 non FRM/FEM Mass",
        "81102": "PM10 Mass",
        "86101": "PMc Mass",
        "SPEC": "PM2.5 Speciation",
        "PM10SPEC": "PM10 Speciation",
        "WIND": "Winds (Resultant)",
        "TEMP": "Temperature",
        "PRESS": "Barometric Pressure",
        "RH_DP": "RH and Dewpoint",
        "HAPS": "HAPs",
        "VOCS": "VOCs",
        "NONOxNOy": "NONOxNOy",
        "LEAD": "Lead",
    }


def inputs(categories: dict, years: list) -> pd.DataFrame:
    return pd.DataFrame(product(categories.keys(), years), columns=['Category Code', 'Year'])


def _download_zip(name, zip_dir):
    url  = f"https://aqs.epa.gov/aqsweb/airdata/daily_{name}.zip"
    response = requests.get(url, stream=True, headers={
        'user-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    })
    response.raise_for_status()

    zip_path = os.path.join(zip_dir, f"{name}.zip")
    with open(zip_path, 'wb') as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)

    return zip_path

def zips(inputs: pd.DataFrame, zip_dir: str) -> pd.Series:
    
    return (inputs['Category Code'] + '_' + inputs['Year'].astype(str)).apply(_download_zip, zip_dir=zip_dir)


def _unzip(zip_path, unzip_dir, categories):
    tmp = os.path.basename(zip_path)[:-len('.zip')].split('_')
    if len(tmp) > 2:
        category, year = '_'.join(tmp[:-1]), tmp[-1]
    else:
        category, year = tmp
    category = categories[category]
    unzip_name = f"{category}_{year}.csv".replace('/', '-')
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            file_name = zip_ref.namelist()[0]  
            zip_ref.extract(file_name, unzip_dir)
            os.rename(os.path.join(unzip_dir, file_name), os.path.join(unzip_dir, unzip_name))

        return os.path.join(unzip_dir, unzip_name)
    except Exception as err:
        print('Error')
        return ''

def unzips(zips: pd.Series, categories: dict, unzip_dir: str) -> pd.Series:
    return zips.apply(_unzip, unzip_dir=unzip_dir, categories=categories)


def _aggregate(path, write_dir):
    if not path:
        return None
    csv = pd.read_csv(path)
    formatted_csv = csv.groupby(['State Code', 'State Name',  'County Code', 'County Name'])['Observation Count'].sum().reset_index()
    formatted_csv.to_csv(os.path.join(write_dir, os.path.basename(path)))
    return os.path.join(write_dir, os.path.basename(path))

def formatted_csvs(unzips: pd.Series, formatted_dir: str) -> pd.Series:
    return unzips.apply(_aggregate, write_dir=formatted_dir)



def total(formatted_csvs: pd.Series) -> pd.DataFrame:
    return pd.concat(formatted_csvs.apply(pd.read_csv).to_list())


# dtype = {
#     "State Code": str,
#     "County Code": str,
#     "Site Num": str,
#     "Parameter Code": str,
#     "POC": int,
#     "Latitude": float,
#     "Longitude": float,
#     "Datum": str,
#     "Parameter Name": str,
#     "Sample Duration": str,
#     "Pollutant Standard": str,
#     "Date Local": np.datetime64,
#     "Units of Measure": str,
#     "Event Type": str,
#     "Observation Count": int,
#     "Observation Percent": float,
#     "Arithmetic Mean": float,
#     "1st Max Value": int,
#     "1st Max Hour": int,
#     "AQI": int,
#     "Method Code": str,
#     "Method Name": str,
#     "Local Site Name": str,
#     "Address": str,
#     "State Name": str,
#     "County Name": str,
#     "CBSA Name": str,
#     "Date of Last Change": np.datetime64
# }

