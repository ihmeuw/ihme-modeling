import pandas as pd
import sklearn as sk
import numpy as np
from sklearn.linear_model import LinearRegression
import time
from multiprocessing import Pool

from covid_correction_helper_functions import *
from covid_correction_settings import get_covid_correction_settings

from cod_prep.claude.claude_io import get_claude_data
from cod_prep.downloaders.engine_room import get_package_map
from db_queries import get_population
from db_queries import get_location_metadata
from db_queries import get_cause_metadata

def prep_data_for_regressions(df, SETTINGS=get_covid_correction_settings()):
    for year in range(2020,SETTINGS['MAX_YEAR_FOR_RANGE']):
        df[f'diff_from_expected_{year}'] = df[f'{year}'] - df[f'imputed_{year}']
    
    long_df = pd.DataFrame()
    for year in range(2020,SETTINGS['MAX_YEAR_FOR_RANGE']):
        tmp = df[['location_id','super_region_name','age_group','age_group_id','cause_id','sex_id',f"diff_from_expected_{year}",f'{year}']]
        tmp = tmp.rename(columns={f"diff_from_expected_{year}":"diff_from_expected_deaths"})
        tmp = tmp.rename(columns={f"{year}":"deaths"})
        tmp['year_id'] = year
        long_df = pd.concat([long_df, tmp])

    df = long_df.copy()
    df = df[~df['diff_from_expected_deaths'].isnull()]
    pop = get_population(
        release_id=9, 
        year_id=[2020,2021],
        location_id=list(df['location_id'].unique()),
        sex_id=[1,2],
        age_group_id=list(df.age_group_id.unique())
    )
    df = merge_n_check_left(df, pop)
    df['change_from_cfac_rate'] = (df['diff_from_expected_deaths'] / df['population']) * 100000
    df = merge_n_check_left(df, locs[['location_id','iso3']])
    covid_df = get_ihme_covid_results(SETTINGS)
    df = merge_n_check_left(df, covid_df)
    df['covid_rate'] = (df['val'] / df['population']) * 100000
    df = df[['location_id','super_region_name','iso3','age_group','age_group_id','sex_id','year_id','cause_id',
             'change_from_cfac_rate','covid_rate','deaths','diff_from_expected_deaths','population']]

    return df

if __name__ == "__main__":    

    SETTINGS = get_covid_correction_settings()

    locs = pd.read_csv(SETTINGS['LOCATIONS_WITH_2020_VR'])
    locs = locs.query("level == 3")
    iso3s = get_list_of_active_iso3s()
    df = get_and_format_claude_data_for_predictions(iso3s, retain_age_group_id=True, claude_phase="redistribution")
    df = create_features(df,retain_age_group_id=True, normalize_data=False)
    df = prep_data_for_regressions(df)
    df = df.query("deaths >= 0")
    df.to_csv(SETTINGS['DATAFRAME_WITH_COVID_AND_MOBILITY_post_rd'], index=False)
    check_and_make_dir('REGRESSION_DATA_FOLDER')
    for iso3 in df.iso3.unique():
        tmp = df[(
            (df['iso3'] == iso3)
        )]
        tmp.to_csv(SETTINGS['REGRESSION_DATA_ISO_CAUSE_post_rd'].format(iso3, "all"), index=False)

    for iso3 in ["IND"]:
        tmp = df[(
            (df['iso3'] == iso3)
        )]
        tmp.to_csv(SETTINGS['REGRESSION_DATA_ISO_CAUSE_post_rd'].format(iso3, "all"), index=False)

