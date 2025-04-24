import pandas as pd

from typing import Callable


def _AgeFormatter(value):
    return {
        "00": '0 years',
        '01': '1-4 years',
        '02': '5-9 years',
        '03': '10-14 years',
        '04': '15-19 years',
        '05': '20-24 years',
        '06': '25-29 years',
        '07': '30-34 years',
        '08': '35-39 years',
        '09': '40-44 years',
        '10': '45-49 years',
        '11': '50-54 years',
        '12': '55-59 years',
        '13': '60-64 years',
        '14': '65-69 years',
        '15': '70-74 years',
        '16': '75-79 years',
        '17': '80-84 years',
        '18': '85-90 years',
        '19': '90+ years'
    }[str(value).zfill(2)]


def _SexFormatter(value):
    return {
        '1': 'Male',
        '2': 'Female'
    }[str(value)]

def _OriginFormatter(value):
    return {
        '0': 'Non-Hispanic',
        '1': 'Hispanic',
        '9': 'Not applicable in 1969+ W,B,O files'
    }[str(value)]

def _RaceFormatter(value, year):
    if year < 1990:
        return {
            '1': 'White',
            '2': 'Black',
            '3': 'Other'
        }[str(value)]
    else:
        return {
            '1': 'White',
            '2': 'Black',
            '3': 'American Indian/Alaska Native',
            '4': 'Asian or Pacific Islander'
        }[str(value)]



def _RegistryFormatter(value):
    return {
        '01': 'San Francisco-Oakland SMSA',
        '02': 'Connecticut',
        '20': 'Detroit (Metropolitan)',
        '21': 'Hawaii',
        '22': 'Iowa',
        '23': 'New Mexico',
        '25': 'Seattle (Puget Sound)',
        '26': 'Utah',
        '27': 'Atlanta (Metropolitan)',
        '29': 'Alaska',
        '31': 'San Jose-Monterey',
        '33': 'Arizona',
        '35': 'Los Angeles',
        '37': 'Rural Georgia',
        '41': 'California excluding SF/SJM/LA',
        '42': 'Kentucky',
        '43': 'Louisiana',
        '44': 'New Jersey',
        '47': 'Georgia excluding Atlanta/Rural Georgia',
        '61': 'Idaho',
        '62': 'New York',
        '63': 'Massachusetts',
        '99': 'Registry for non-SEER area'
    }[str(value).zfill(2)]

def SEERS(url: str) -> pd.DataFrame:
    colspecs = [(0, 4), (4, 6), (6, 8), (8, 11), 
    (11, 13), (13, 14), (14, 15), (15, 16), (16, 18), (18, 27)]
    cols = ['YEARS', 'STATE_ABBREVIATION', 'STATE_FIPS_CODE', 'COUNTY_FIPS_CODE', 'GAP', 'RACE', 'ORIGIN', 'SEX', 'AGE_GROUP', 'POPULATION']
    data = pd.read_fwf(url, colspecs=colspecs, compression='gzip', header=None)
    data.columns = cols
    return data

# def registry(SEERS: pd.DataFrame, registry_map: Callable=_RegistryFormatter) -> pd.Series:
#     return SEERS['REGISTRY'].apply(registry_map)


def race(SEERS: pd.DataFrame, race_map: Callable=_RaceFormatter) -> pd.Series:
    return SEERS[['RACE', 'YEARS']].apply(lambda x: race_map(x['RACE'], x['YEARS']), axis=1)


def origin(SEERS: pd.DataFrame, origin_map: Callable=_OriginFormatter) -> pd.Series:
    return SEERS['ORIGIN'].apply(origin_map)

def sex(SEERS: pd.DataFrame, sex_map: Callable=_SexFormatter) -> pd.Series:
    return SEERS['SEX'].apply(sex_map)

def age(SEERS: pd.DataFrame, age_map: Callable=_AgeFormatter) -> pd.Series:
    return SEERS['AGE_GROUP'].apply(age_map)



def SEERSFormatted(SEERS: pd.DataFrame) -> pd.DataFrame:
    return SEERS[['STATE_FIPS_CODE', 'COUNTY_FIPS_CODE', 'YEARS', 'POPULATION']]