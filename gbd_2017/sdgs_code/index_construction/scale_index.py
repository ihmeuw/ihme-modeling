import pandas as pd
import sys
import math
import numpy as np
import os
from scipy.stats import gmean
from scipy.special import (logit, expit)
from scipy import stats
import time
from getpass import getuser

sys.path.append(SDG_REPO)
import sdg_utils.draw_files as dw
INDICATOR_ID_COLS = ['indicator_id', 'location_id', 'year_id', 'level']
import feather
from db_queries import get_location_metadata as glm
locmeta = glm(location_set_id=35, gbd_round_id=5)
country_list = list(locmeta[locmeta.level == 3]['location_id'])

SDG_VERS = dw.SDG_VERS
draw_cols = ['draw_{}'.format(i) for i in range(1000)]
indicator_map = "FILEPATH"
hrh_past = "FILEPATH" 
hrh_future = "FILEPATH"

def clean_compiled_indicators(df):
    """Merge with indicator metadata and filter to desired locations and years to get invert column (for indicator inverting (Lower number better score indicators))

    Filters to SDG reporting locations and SDG reporting years.

    Parameters
    ----------
    df : pandas DataFrame
        Expected to contain dw.INDICATOR_ID_COLS and draw_cols

    Returns
    -------
    out : pandas DataFrame
        Contains additional columns invert and scaling.
    """

    # replace disaster with multi year moving average
#     if 1019 in df['indicator_id'].unique():
#         df = multi_year_avg(df, 1019)


    indic_table2 = pd.read_csv(indicator_map)[['indicator_id', 'scaling','forecasts', 'invert']]
    indic_table2 = indic_table2[indic_table2.forecasts == 1].drop('forecasts', axis = 1)    
    df = df.merge(indic_table2[['indicator_id', 'scaling','invert']].drop_duplicates(), how='left')
    # filter to sdg reporting years (move this to global in config file)
    sdg_years = range(1990, 2031)
    df = df.loc[df['year_id'].isin(sdg_years)]
    # make sure each id column is an integer
    for id_col in INDICATOR_ID_COLS:
        df[id_col] = df[id_col].astype(int)
    return df


def scale_infinite(df):
    """Scale infinitely scaled indicators"""
    df = df.copy()
    dfs = []
    indicator_list = list(set(df.indicator_id)) #goes through each indicator in a loop (infinites)
    for indicator in indicator_list:
        print 'starting indicator: {}'.format(indicator)
        df_subset = df[df.indicator_id == indicator]
        for col in draw_cols:
            vector = df_subset[col]
            vector = vector.apply(lambda x: 0 if x<1.1e-10 else x) #apply an adjusted zero function.  This will add a constant based on below's equation so that no values will be zeroes in log space.
            if len(vector[vector == 0]) > 0:
                subvector = vector[vector>0]
                constant = np.median(subvector) / ((np.median(subvector)/ subvector.quantile(0.25))**2.9)
                df_subset[col] = np.log(vector + constant)
            else:
                df_subset[col] = np.log(vector)
        df_subset.loc[:, draw_cols] = (df_subset.loc[:, draw_cols] - df_subset.loc[df_subset.location_id.isin(country_list), draw_cols].quantile(0.025)) / (df_subset.loc[df_subset.location_id.isin(country_list), draw_cols].quantile(0.975) - df_subset.loc[df_subset.location_id.isin(country_list), draw_cols].quantile(0.025))
        df_subset.loc[:, draw_cols] = df_subset.loc[:, draw_cols].clip(lower = 0)
        df_subset.loc[:, draw_cols] = df_subset.loc[:, draw_cols].clip(upper = 1)
        dfs.append(df_subset)
    print 'infinites are done'
    df = pd.concat(dfs, ignore_index=True)
    return df

def conflict_fix(df, indicator_id = 1031):
    avg_ind_df = df.loc[(df['indicator_id'] == indicator_id) & df['year_id'].isin(range(2025,2030))]
    df = df.loc[((df['indicator_id'] != 1031) | (df['year_id'] != 2027))]
    avg_ind_df = avg_ind_df.groupby(['indicator_id','location_id','level'],as_index=False)[draw_cols].mean()
    avg_ind_df['year_id'] = 2027    
    final_df = pd.concat([df,avg_ind_df],ignore_index = True)
    final_df = final_df.sort_values(['indicator_id','location_id','year_id'])
    return final_df

def multi_year_avg(df, indicator_id, window=10, time = 'future'):
    """Calculate a moving average for an indicator.

    Functionally used for the disaster indicator so that it can be less noisy.

    Replaces values for the given indicator with the five year moving average
    for that indicator. Requires all years from 1990-2015 to be present for
    that indicator.
sor
    Parameters
    ----------
    df : pandas DataFrame
        Expected to contain dw.INDICATOR_ID_COLS and dw.DRAW_COLS

    Returns
    -------
    df : pandas DataFrame
        Contains the same columns, with less years for the given indicator.

    """
    avg_ind_df = df.loc[(df['indicator_id'] == indicator_id)]
    df = df.loc[df['indicator_id'] != indicator_id]
    # make sure single years are available for calculation or this is probably
    #  going to be worthless
    assert set(avg_ind_df.year_id.unique()) == set(range(1990, 2031)), \
        'Needed years for calculation are not present.'
    if str.lower(time) == 'both':
        time_range = range(1990, 2031)        
    elif str.lower(time) == 'future':
        time_range = range(2018,2031)
    excluded = avg_ind_df[~((avg_ind_df.year_id.isin(time_range))& (avg_ind_df.level == 3))]
    avg_dfs = []
    avg_dfs.append(excluded)
    for year in time_range:
        yrange = year - np.arange(window)
        avg_df = avg_ind_df.loc[avg_ind_df.year_id.isin(yrange) & (avg_ind_df.level == 3)]
        # get lag weights
        avg_df['weight'] = (window + avg_df['year_id'] - max(yrange)) / sum(np.arange(window) + 1)
        avg_df['year_id'] = year

        # calc lag-distributed rate
        avg_df = pd.concat([
                            avg_df[INDICATOR_ID_COLS],
                            avg_df[draw_cols].apply(lambda x: x * avg_df['weight'])
                           ],
                           axis=1)
        avg_df = avg_df.groupby(
            INDICATOR_ID_COLS,
            as_index=False
        )[draw_cols].sum()
        avg_dfs.append(avg_df)
    avg_df = pd.concat(avg_dfs)
    df = df.append(avg_df, ignore_index=True)
    return df


def scale_proportions(df):
    """Scale proportionally scaled indicators."""
    df = df.copy()
    dfs = []
    indicator_list = list(set(df.indicator_id))#goes through each indicator in a loop (proportions)
    for indicator in indicator_list:
        print 'starting indicator: {}'.format(indicator)
        df_subset = df[df.indicator_id == indicator]
        for col in draw_cols:
            vector = df_subset[col]
            vector = vector.apply(lambda x: 0 if x<1.1e-10 else x) #adjust so that zero values do not create negative infinity values within logit space
            df_subset[col] = vector        
        df_subset.loc[:, draw_cols] = df_subset.loc[:, draw_cols].clip(upper = 1)
        df_subset.loc[:, draw_cols] = (df_subset.loc[:, draw_cols] - df_subset.loc[df_subset.location_id.isin(country_list), draw_cols].quantile(0.025)) / (df_subset.loc[df_subset.location_id.isin(country_list), draw_cols].quantile(0.975) - df_subset.loc[df_subset.location_id.isin(country_list), draw_cols].quantile(0.025))
        #adjust values so that proportions are capped within 0 to 1.  Proportions will go over when the draw values are lower or greater than the 2.5th and 97.5th percentile respectively.
        df_subset.loc[:, draw_cols] = df_subset.loc[:, draw_cols].clip(lower = 0)
        df_subset.loc[:, draw_cols] = df_subset.loc[:, draw_cols].clip(upper = 1)
        dfs.append(df_subset)
    print 'proportions are done'
    df = pd.concat(dfs, ignore_index=True)
    return df

def scale_indicators(df, zero_replace_method='fixed'):

    # draw_cols = [col for col in df.columns if "draw_" in col]
    # id_cols = [col for col in df.columns if "draw_" not in col]
    """Scale indicators, differently for proportion vs infinite.""" #Currently a,b is listed as proportions, and c is listed as rates.  Change based on whichever column represents the proportions v. indicators


    props = df.loc[(df.scaling == "a")| (df.scaling == 'b')] #may change "Scaling column in future"
    rates = df.loc[df.scaling == "c"]
    inf_scaled = scale_infinite(rates)
    props_scaled = scale_proportions(props)
    df = pd.concat([inf_scaled, props_scaled], ignore_index=True)
    df = df.apply(lambda row: row.fillna(row[draw_cols].mean()), axis=1)
    max_val = df[draw_cols].values.max()
    min_val = df[draw_cols].values.min()  
    #check if values are within bounds.  This will also check if any NAs are introduced
    max_val = df[draw_cols].values.max()
    # df['max_val'] = df[draw_cols].max(axis=1)
    # max_val = df.drop(draw_cols, axis=1)
    # feather.write_dataframe(max_val, "/share/scratch/projects/sdg/proportion_vals.feather")

    min_val = df[draw_cols].values.min()
    assert max_val <= 1, \
        'The scaled values should not be greater than 1: {}'.format(max_val)
    assert df[draw_cols].values.min() >= 0, \
        'The scaled values should not be less than 0: {}'.format(min_val)

    # invert so that 1 is good, 0 is bad
    df.loc[:, draw_cols] = df.loc[:, draw_cols].apply(lambda x: abs(df['invert'] - x))

    # get rid of invert column cause its WORTHLESS NOW
    df = df[INDICATOR_ID_COLS + draw_cols ]
    return df

def scale_indicators(sdg_version = SDG_VERS, hrh_separate = True):
    """scales all indicators in unscaled dataframe"""
    path = "{idd}/gbd2017/".format(idd=dw.INDICATOR_DATA_DIR)
    input_path = path + "all_indicators_unscaled_v{}.feather".format(sdg_version)
    output_path = path + "all_indicators_scaled_v{}.feather".format(sdg_version)
    df = pd.read_feather(input_path)
    df = multi_year_avg(df, 1019)
    df = conflict_fix(df)
    df = clean_compiled_indicators(df)    
    if hrh_separate:
        df = df[df.indicator_id != 1096]
        new_df = scale_indicators(df, zero_replace_method='fixed')
        hrh_future = pd.read_feather(hrh_future)
        hrh_past = pd.read_feather(hrh_past)
        hrh = pd.concat([hrh_past, hrh_future], ignore_index = True)
        hrh = hrh[INDICATOR_ID_COLS + draw_cols]
        new_df = pd.concat([new_df,hrh], ignore_index = True)
    else:
        new_df = scale_indicators(df, zero_replace_method='fixed')
    new_df.to_feather(out_file)
  

