from pathlib import Path

import pandas as pd
import numpy as np
from functools import partial, reduce

from cod_prep.downloaders import get_cause_map, get_cod_ages, add_location_metadata
from cod_prep.claude.formatting import finalize_formatting
from cod_prep.utils.formatting import ages
from cod_prep.utils import print_log_message, report_if_merge_fail
from cod_prep.claude.configurator import Configurator

CONF = Configurator()


def convert_to_one_death_col(df):

    df.loc[df['maternal_deaths'] != 0, 'deaths'] = 0
    non_death_columns = [col for col in df.columns if 'deaths' not in col]
    non_maternal_deaths = df[non_death_columns + ['deaths']]
    non_maternal_deaths['cause'] = 'cc_code'
    maternal_deaths = df[non_death_columns + ['maternal_deaths']]
    maternal_deaths.rename(columns={'maternal_deaths': 'deaths'}, inplace=True)
    maternal_deaths['cause'] = 'maternal'
    df = pd.concat([non_maternal_deaths, maternal_deaths])

    return df


def merge_nids(df):
    dhs_nid = pd.read_csv(
        'FILEPATH'
    )
    dhs_nid = dhs_nid[['country', 'NID']].drop_duplicates()
    assert not dhs_nid['country'].duplicated().values.any()
    dhs_nid.rename(columns={'country': 'iso3', 'NID': 'nid'}, inplace=True)
    df = df.merge(dhs_nid, on='iso3', how='left')
    assert df['nid'].notnull().values.all()

    return df
    

def set_cod_columns(df):
    df['data_type_id'] = 5
    df['representative_id'] = 1
    df['site'] = ''
    df['code_system_id'] = 177
    df['sex_id'] = 2

    return df


def process_dhs_03(df):
    df = convert_to_one_death_col(df)
    df = merge_nids(df)
    df = set_cod_columns(df)

    return df


