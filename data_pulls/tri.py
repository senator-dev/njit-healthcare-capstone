import requests
import pandas as pd
import os

def years() -> pd.Series:

    return pd.Series(list(range(1987, 2024)))


def inputs(years: pd.Series) -> pd.Series:
    return years.apply(lambda x: f"https://data.epa.gov/efservice/downloads/tri/mv_tri_basic_download/{x}_US/csv")


def _download_csv(url, download_dir):
    name = url.split('/')[-2].split('_')[0] + '.csv'
    path = os.path.join(download_dir, name)

    response = requests.get(url)
    response.raise_for_status()

    with open(path, "wb") as file:
        file.write(response.content)

    return path


def csvs(inputs: pd.Series, download_dir: str) -> pd.Series:
    return inputs.apply(_download_csv, download_dir=download_dir)

    