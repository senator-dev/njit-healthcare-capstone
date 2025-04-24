
import pandas as pd
import numpy as np


def incidence(incidence_path: str) -> pd.DataFrame:
    _incidence = pd.read_csv(incidence_path, delimiter='\t')
    _incidence['Count'] = _incidence['Count'].replace('Suppressed', np.nan).replace('Missing', np.nan).astype(float)
    return _incidence


def bladder_incidence(incidence: pd.DataFrame) -> pd.DataFrame:
    return incidence[incidence['Cancer Sites'] == 'Urinary Bladder, invasive and in situ'].drop(columns='Cancer Sites')


def lung_incidence(incidence: pd.DataFrame) -> pd.DataFrame:
    return incidence[incidence['Cancer Sites'] == 'Lung and Bronchus'].drop(columns='Cancer Sites')