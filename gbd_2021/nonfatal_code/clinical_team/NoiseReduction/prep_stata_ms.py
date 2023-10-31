

import glob
import os
import pandas as pd


def get_pre_nr_ms(base):
    sub_dirs = {
                21: [f'{base}FILEPATH',
                     f'{base}FILEPATH'],
                17: [f'{base}FILEPATH',
                     f'{base}FILEPATH']
               }

    l = []
    for est_id, sub_dirs in sub_dirs.items():
        for v in sub_dirs:
            files = glob.glob(f'{v}/FILEPATH')
            assert len(files) > 40,
            tmp = pd.concat([pd.read_stata(f) for f in files], sort=False)
            tmp['estimate_id'] = est_id
            l.append(tmp)
    assert len(l) == 4
    df = pd.concat(l, sort=False, ignore_index=True)
    return df

def fix_cols(df):

    ab_df = df['acause'].str.split("#", expand=True)
    ab_df.rename(columns={0: 'acause', 1: 'bundle_id'},
                inplace=True)
    df.drop('acause', axis=1, inplace=True)
    df = pd.concat([df, ab_df], axis=1)

    int_cols = ['nid', 'age_start', 'bundle_id']
    for c in int_cols:
        df[c] = pd.to_numeric(df[c], downcast='integer', errors='raise')
    return df

def standard_col_names(df):

    rd = {'NID': 'nid', 'sex': 'sex_id', 'year': 'year_id',
          'age': 'age_start', 'cf_raw': 'ms_mean'}
    df.rename(columns=rd, inplace=True)
    return df

def remove_cols(df):
    drops = ['iso3', 'list', 'national', 'source', 'source_type',
             'subdiv', 'region', 'source_label', 'acause',
             'cf_corr', 'cf_rd', 'cf_final']
    df.drop(drops, axis=1, inplace=True)
    return df


def write_csv_for_python_nr(run_id):

    # VARS
    base = FILEPATH
    file_path = f"{base}FILEPATH"

    if os.path.exists(file_path):
        print("The NR file exists, doing nothing")
        pass
    else:
        print("The NR file doesn't exist, prepping it")
        # FUNCTIONS
        df = get_pre_nr_ms(base)
        df = standard_col_names(df)
        df = fix_cols(df)
        df = remove_cols(df)

        # OUTPUT
        df.to_csv(file_path, index=False)

    return
