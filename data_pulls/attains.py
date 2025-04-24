import pandas as pd
import requests
import zipfile
import geopandas as gpd


def zipped_file(url: str, zip_path: str) -> str:
    response = requests.get(url, stream=True)
    response.raise_for_status()

    with open(zip_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    
    return zip_path

def unzipped_file(zipped_file: str, unzip_path: str) -> str:
    with zipfile.ZipFile(zipped_file, 'r') as zip_ref:
        file_name = zip_ref.namelist()[0]
        return zip_ref.extractall(unzip_path)


def attains_au_areas(unzipped_file: str) -> pd.DataFrame:
    return gpd.read_file(unzipped_file, s='attains_au_areas')



def attains_au_lines(unzipped_file: str) -> pd.DataFrame:
    return gpd.read_file(unzipped_file, s='attains_au_lines')


def attains_au_points(unzipped_file: str) -> pd.DataFrame:
    return gpd.read_file(unzipped_file, s='attains_au_points')


def attains_au_catchments(unzipped_file: str) -> pd.DataFrame:
    return gpd.read_file(unzipped_file, s='attains_au_catchments')


def attains_au_attributes(unzipped_file: str) -> pd.DataFrame:
    return gpd.read_file(unzipped_file, s='attains_au_attributes')


def attains_au_control(unzipped_file: str) -> pd.DataFrame:
    return gpd.read_file(unzipped_file, s='attains_au_control')


def attains_au_catchassmnt(unzipped_file: str) -> pd.DataFrame:
    return gpd.read_file(unzipped_file, s='attains_au_catchassmnt')


def attains_au_huc12assmnt(unzipped_file: str) -> pd.DataFrame:
    return gpd.read_file(unzipped_file, s='attains_au_huc12assmnt')


def attains_au_assmnt_parms(unzipped_file: str) -> pd.DataFrame:
    return gpd.read_file(unzipped_file, s='attains_au_water_types')


def attains_au_meta(unzipped_file: str) -> pd.DataFrame:
    return gpd.read_file(unzipped_file, s='attains_au_meta')