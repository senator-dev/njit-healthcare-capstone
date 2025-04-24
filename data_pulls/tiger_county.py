import pandas as pd
import zipfile
import requests


def zipped_file(county_url: str, zip_path: str) -> str:
    response = requests.get(county_url, stream=True)
    response.raise_for_status()

    with open(zip_path, "wb") as file:
        for chunk in response.iter_content(chunk_size=8192):
            file.write(chunk)
    
    return zip_path



def unzipped_file(zipped_file: str, unzip_path: str) -> str:
    with zipfile.ZipFile(zipped_file, 'r') as zip_ref:
        return zip_ref.extractall(unzip_path)

