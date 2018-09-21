import pandas as pd
import numpy as np
import sys
import os
import random

from getpass import getuser

import sdg_utils.draw_files as dw
import sdg_utils.queries as qry

from multiprocessing import Pool

def age_standardize(df, popdf, awdf, locsdf, pop_level='most_detailed'):
    # Assign location for population
    if pop_level != 'most_detailed':
        df = df.merge(locsdf[['location_id', 'L3_loc']],
                      how = 'left')
        df = df.rename(index = str, columns = {'L3_loc' : 'pop_loc'})
        df['pop_loc'] = df.pop_loc.astype(int)
    else:
        df['pop_loc'] = df['location_id']
    popdf = popdf.rename(index = str, columns = {'location_id' : 'pop_loc'})

    # Combine sexes
    df = df.merge(popdf, how = 'left')
    df = pd.concat(
            [
                df[['location_id', 'year_id', 'age_group_id', 'sex_id', 'population']],
                df[dw.DRAW_COLS].apply(
                    lambda x: x * df['population']
                )
            ],
            axis=1
        )
    df['sex_id'] = 3
    df = df.groupby(['location_id', 'year_id', 'age_group_id', 'sex_id'], as_index = False)[dw.DRAW_COLS + ['population']].sum()

    # Age standardize
    df = df.merge(awdf, how='left')
    df = pd.concat(
            [
                df[['location_id', 'year_id', 'age_group_id', 'sex_id']],
                df[dw.DRAW_COLS].apply(
                    lambda x: (x / df['population']) * df['age_group_weight_value']
                )
            ],
            axis=1
        )
    df['age_group_id'] = 27
    df = df.groupby(['location_id', 'year_id', 'age_group_id', 'sex_id'], as_index = False)[dw.DRAW_COLS].sum()

    # Add relevant variables
    df['measure_id'] = 6
    df['metric_id'] = 3

    return df[dw.HIV_GROUP_COLS + dw.DRAW_COLS]

def scale_forecast(past_df, future_df, scale_year=2016):
    scalar_df = past_df.loc[past_df.year_id == scale_year].set_index(['location_id']).sort_index()[dw.DRAW_COLS] / \
                future_df.loc[future_df.year_id == scale_year].set_index(['location_id']).sort_index()[dw.DRAW_COLS]
    scalar_df = scalar_df.reset_index()
    scalar_df = scalar_df.merge(future_df[dw.HIV_GROUP_COLS])

    future_df = scalar_df.set_index(dw.HIV_GROUP_COLS).sort_index()[dw.DRAW_COLS] * \
                future_df.set_index(dw.HIV_GROUP_COLS).sort_index()[dw.DRAW_COLS]
    future_df = future_df.reset_index()

    return future_df.loc[future_df.year_id > scale_year]


def load_location_file(iso):
    df = pd.read_csv(os.path.join(dw.HIV_DIR, iso + '.csv'))
    df = df.query('year_id >= 1990 and year_id <= 2030 and variable == "Incidence"')
    df['run_num'] = 'draw_' + (df['run_num'] - 1).astype(str)
    df = pd.pivot_table(df,
                        values='value',
                        index=['location_id', 'year_id', 'age_group_id', 'sex_id'],
                        columns='run_num')
    df = df.reset_index()
    return df

if __name__ == '__main__':
    # Supplementary datasets
    print('Collecting supplementary datasets')
    age_weights = qry.get_age_weights(4)
    age_weights.loc[age_weights.age_group_id.isin([30, 31, 32, 235]), 'age_group_id'] = 21
    age_weights = age_weights.groupby(['age_group_id'], as_index=False)['age_group_weight_value'].sum()

    gbd_popdf = qry.get_pops()
    gbd_popdf.loc[gbd_popdf.age_group_id.isin([30, 31, 32, 235]), 'age_group_id'] = 21
    gbd_popdf = gbd_popdf.groupby(['location_id', 'year_id', 'age_group_id', 'sex_id'], as_index=False)['population'].sum()

    wpp_popdf = pd.read_csv('FILEPATH/wpp2015_to2063.csv')
    wpp_popdf = wpp_popdf.loc[wpp_popdf.year_id >= 2016]
    wpp_popdf = wpp_popdf.rename(index=str, columns={'pop':'population'})

    locsdf = qry.get_sdg_reporting_locations()
    locsdf['L3_loc'] = [loc[3] for loc in locsdf.path_to_top_parent.str.split(',').tolist()]

    # Compile all countries
    print('Fetching location-specific datasets')
    pool = Pool(15)
    dfs = pool.map(load_location_file, locsdf['ihme_loc_id'].values)
    pool.close()
    pool.join()
    fulldf = pd.concat(dfs)

    # Make age-standardized
    print('Age-standardizing')
    pastdf = age_standardize(fulldf.loc[fulldf.year_id <= 2016],
                             gbd_popdf, age_weights, locsdf, pop_level='most_detailed')
    futuredf = age_standardize(fulldf.loc[fulldf.year_id >= 2016],
                               wpp_popdf, age_weights, locsdf, pop_level='national')

    # Scale forecast
    print('Scaling forecasts')
    futuredf = scale_forecast(pastdf, futuredf, scale_year=2016)

    # Save past
    print('Saving')
    try:
        if not os.path.exists(dw.HIV_OUTDIR):
            os.makedirs(dw.HIV_OUTDIR)
    except OSError:
        pass
    pastdf.to_hdf(dw.HIV_OUTDIR + "/inc_clean.h5",
                  key="data",
                  format="table", data_columns=['location_id', 'year_id'])

    # Save forecast
    try:
        if not os.path.exists(dw.FORECAST_DATA_DIR + "/hiv/" + dw.HIV_VERS):
            os.makedirs(dw.FORECAST_DATA_DIR + "/hiv/" + dw.HIV_VERS)
    except OSError:
        pass
    futuredf.to_hdf(dw.FORECAST_DATA_DIR + "/hiv/" + dw.HIV_VERS + "/inc_clean.h5",
                    key="data",
                    format="table", data_columns=['location_id', 'year_id'])
