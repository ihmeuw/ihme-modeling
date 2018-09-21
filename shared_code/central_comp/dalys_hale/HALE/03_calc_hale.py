from __future__ import division

import argparse
import numpy as np
import pandas as pd
from HALE_summary import calc_summary

def old_Tx(df):
    if df['age_group_id'] == 24:
        return df['Tx']
    return df['nLx']

def adjust_Lx(df):
    adj_Lx = df['nLx'] * (1 - df['yld_rate'])
    return adj_Lx

def adjust_Tx(df):
    if ~np.isnan(df['tmp_Tx']):
        return df['tmp_Tx']
    else:
        return df['adj_Tx']

def calc_hale(df):
    hale = df['adj_Tx'] / df['lx']
    return hale

def run_hale_calc(hale_tmp, hale_dir, lt_tmp, yld_tmp, location):
    index_cols = ['sex_id', 'age_group_id', 'year_id', 'location_id', 'draw']

    lt_draws = pd.read_csv('{lt_tmp}/{location}_draws.csv'.format(
            lt_tmp=lt_tmp, location=location))
    yld_draws = pd.read_csv('{yld_tmp}/{location}_draws.csv'.format(
            yld_tmp=yld_tmp, location=location))
    combo_draws = yld_draws.merge(lt_draws, on=index_cols)

    ages = range(5, 21) + range(30, 33) + [28, 235]
    combo_draws = combo_draws.loc[combo_draws['age_group_id'].isin(ages)]
    combo_draws.loc[combo_draws['age_group_id'] == 28, 'age_group_id'] = 4
    combo_draws.loc[combo_draws['age_group_id'] == 30, 'age_group_id'] = 21
    combo_draws.loc[combo_draws['age_group_id'] == 31, 'age_group_id'] = 22
    combo_draws.loc[combo_draws['age_group_id'] == 32, 'age_group_id'] = 23
    combo_draws.loc[combo_draws['age_group_id'] == 235, 'age_group_id'] = 24

    # Start actual math
    combo_draws['nLx'] = combo_draws.apply(old_Tx, axis=1)
    combo_draws['adj_Lx'] = combo_draws.apply(adjust_Lx, axis=1)
    combo_draws['adj_Tx'] = None
    for age in range(24, 3, -1):
        temp = combo_draws.loc[combo_draws['age_group_id'] >= age]
        temp = temp.groupby(['year_id', 'sex_id', 'draw',
                             'location_id']).sum().reset_index()
        temp['tmp_Tx'] = temp['adj_Lx']
        temp['age_group_id'] = age
        temp = temp[index_cols + ['tmp_Tx']]
        combo_draws = combo_draws.merge(temp, on=index_cols, how='left')
        combo_draws['adj_Tx'] = combo_draws.apply(adjust_Tx, axis=1)
        combo_draws.drop('tmp_Tx', axis=1, inplace=True)
    combo_draws['HALE'] = combo_draws.apply(calc_hale, axis=1)
    # End actual math

    combo_draws.loc[combo_draws['age_group_id'] == 4, 'age_group_id'] = 28
    combo_draws.loc[combo_draws['age_group_id'] == 21, 'age_group_id'] = 30
    combo_draws.loc[combo_draws['age_group_id'] == 22, 'age_group_id'] = 31
    combo_draws.loc[combo_draws['age_group_id'] == 23, 'age_group_id'] = 32
    combo_draws.loc[combo_draws['age_group_id'] == 24, 'age_group_id'] = 235

    combo_long = combo_draws[index_cols + ['HALE']].copy(deep=True)
    combo_wide = combo_long.pivot_table(index=['sex_id', 'age_group_id',
                                               'year_id', 'location_id'],
                                        columns='draw',
                                        values='HALE')
    combo_wide = combo_wide.reset_index()
    combo_wide.rename(columns=(lambda x: 'draw_' + str(x) if
                               str(x).isdigit() else x), inplace=True)
    combo_wide['cause_id'] = 294
    csv_wide = combo_wide.set_index('location_id')
    csv_wide.to_csv('{hale_tmp}/{location}_draws.csv'.format(
            hale_tmp=hale_tmp, location=location))

    summ_cols = ['HALE', 'adj_Lx', 'adj_Tx']
    calc_summary(combo_draws, summ_cols, hale_dir, location)

if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument(
            "--hale_tmp",
            help="HALE tmp directory",
            dUSERt="/PATH",
            type=str)
    parser.add_argument(
            "--hale_dir",
            help="HALE summary directory",
            dUSERt="/PATH",
            type=str)
    parser.add_argument(
            "--lt_tmp",
            help="lt tmp directory",
            dUSERt="/PATH",
            type=str)
    parser.add_argument(
            "--yld_tmp",
            help="yld tmp directory",
            dUSERt="/PATH",
            type=str)
    parser.add_argument(
            "--location",
            help="location",
            dUSERt=62,
            type=int)
    args = parser.parse_args()
    hale_tmp = args.hale_tmp
    hale_dir = args.hale_dir
    lt_tmp = args.lt_tmp
    yld_tmp = args.yld_tmp
    location = args.location

    run_hale_calc(hale_tmp, hale_dir, lt_tmp, yld_tmp, location)
