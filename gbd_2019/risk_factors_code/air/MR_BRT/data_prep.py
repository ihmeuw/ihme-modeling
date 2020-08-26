import sys
import os

import pandas as pd
import numpy as np

sys.path.append(os.path.dirname(__file__))
from globals import *


def load_data(file_name, measure, include_smoking, include_shs, age_adjust=False, linear=False):
    if age_adjust:
        df = pd.read_csv(os.path.join(DATA_DIR, 'age_adjust', f'{file_name}.csv'))
    else:
        df = pd.read_csv(os.path.join(DATA_DIR, f'{file_name}.csv'))

    # split up 0/1/2 variables
    for var in ['cv_counfounding.uncontroled', 'cv_selection_bias']:
        df[var + '_b'] = 0
        df.loc[df[var].isnull(), var + '_b'] = np.nan
        df.loc[df[var] == 2, var + '_b'] = 1
        df.loc[df[var] == 2, var] = 1
        df = df.rename(index=str, columns={var: var + '_a'})

    # drop smoking if not including it this run
    if not include_smoking:
        df = df[df.ier_source != 'AS']
    if not include_shs:
        df = df[df.ier_source != 'SHS']

    # drop variables we can't use
    model_cols = []
    for bc in BIAS_COLS:
        if not bc in list(df):
            # print(f'{bc} not in dataset')
            continue
        df.loc[(df.ier_source == 'AS') & (df[bc].isna()), bc] = 0
        # lose if NAs
        if any(np.isnan(df[bc])):
            # print(f'{bc} contains nans')
            continue
        # lose if singular
        if len(df[bc].unique()) == 1:
            # print(f'{bc} is singular')
            continue
        # lose if same as other column
        for obc in model_cols:
            if abs(np.corrcoef(df[bc], df[obc])[0,1]) > 0.99:
                # print(f'{bc} is redundant {obc}')
                continue
        # if you made it this far, congratulations!
        model_cols += [bc]

    # model cols
    if measure == 'diff':
        mean_var = 'shift'
        se_var = 'shift_se'
    elif measure == 'log_ratio':
        mean_var = 'log_rr'
        se_var = 'log_se'
    if linear:
        df[mean_var] = df[mean_var] / (df['conc'] - df['conc_den'])
        df[se_var] = df[se_var] / (df['conc'] - df['conc_den'])

    # check for NAs in essential vars
    for data_col in ['nid', mean_var, se_var, 'conc_den', 'conc']:
        if len(df.loc[df[data_col].isnull()]) > 0:
            problem_idx_list = df.loc[df[data_col].isnull()].index.tolist()
            problem_idx = ', '.join(problem_idx_list)
            raise ValueError(f'Missing value for {data_col} in index {problem_idx}')

    # fill in vars we won't be using
    if 'median_age_fup' not in list(df):
        df['median_age_fup'] = np.nan
    if 'child' not in list(df):
        df['child'] = np.nan

    # add other air pollution indicator
    df['other_ap'] = 1
    df.loc[df.ier_source == 'OAP', 'other_ap'] = 0

    # format data
    df = df[['nid', 'ier_source', 'median_age_fup', 'child', 'other_ap', mean_var, se_var, 'conc_den', 'conc'] + model_cols]
    df = df.sort_values('nid').reset_index(drop=True)

    # get model inputs
    obs_mean = df[mean_var].values
    obs_std = df[se_var].values
    study_sizes = df.groupby('nid', sort=False).count()[mean_var].tolist()
    N = np.sum(study_sizes)

    return df, model_cols, obs_mean, obs_std, study_sizes, N
