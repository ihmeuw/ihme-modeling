import pandas as pd
import numpy as np
import argparse

from chronos.interpolate import interpolate

outdir = 'FILEPATH'

draws = []
for i in range(1000):
     draws.append('draw_{}'.format(i))

age_wgt_df = pd.read_csv('FILEPATH/ag_weights.csv')
age_wgt_df = age_wgt_df[age_wgt_df.age_group_id != 28]

def year_chunk_interp(start_year, end_year, locid, measid, meid):
    df = interpolate(
        gbd_id_type = 'modelable_entity_id', 
        gbd_id=meid, 
        source='epi', 
        measure_id=[measid], 
        location_id=locid, 
        reporting_year_start=start_year, 
        reporting_year_end=end_year, 
        status='best')
    if start_year != 1980:
        df = df[df.year_id != start_year]
    return df

def make_summary(df):
    df['mean_value'] = df[draws].mean(axis=1)
    df['lower_value'] = df[draws].quantile(0.025, axis=1)
    df['upper_value'] = df[draws].quantile(0.975, axis=1)
    df.drop(draws, axis=1, inplace=True)
    df = df[['location_id', 'year_id', 'age_group_id', 'sex_id', 'mean_value', 
        'lower_value', 'upper_value']]
    df['year_id'] = df['year_id'].astype(int)
    return df

def age_standardize(df):
    weight = age_wgt_df.copy()
    merged = df.merge(weight, how='outer', on='age_group_id')
    merged['mean_value'] = merged['mean_value'] * merged['age_group_weight_value']
    merged['lower_value'] = merged['lower_value'] * merged['age_group_weight_value']
    merged['upper_value'] = merged['upper_value'] * merged['age_group_weight_value']
    merged.drop('age_group_weight_value', axis=1, inplace=True)
    merged = merged.groupby(['location_id', 'year_id', 'sex_id']).sum()
    merged = merged.reset_index()
    merged['age_group_id'] = 27
    return merged

def add_cov_cols(df, covid, covname):
    df['covariate_id'] = covid
    df['covariate_name_short'] = covname
    return df

def run_interpolations(locid, measid, meid, covid, covname):
    year_sets = ((1980, 1995), (1995, 2000), (2000, 2005), (2005, 2010), (2010, 2017))
    dfs = []
    for (y1, y2) in year_sets:
        df = year_chunk_interp(y1, y2, locid, measid, meid)
        df = make_summary(df)
        df = age_standardize(df)
        df = add_cov_cols(df, covid, covname)
        dfs.append(df)
    dfs = pd.concat(dfs)
    dfs = dfs[['location_id', 'year_id', 'age_group_id', 'sex_id', 
        'covariate_id', 'covariate_name_short', 'mean_value', 'lower_value', 
        'upper_value']]
    dfs.to_csv(outdir + '{}/{}.csv'.format(meid, locid), index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("locid", help="location id to use", type=int)
    parser.add_argument("measid", help="measure ID to use", type=int)
    parser.add_argument("meid", help="modelable entity ID to use", type=int)
    parser.add_argument("covid", help="covariate ID to use", type=int)
    parser.add_argument("covname", help="covariate name", type=str)    
    args = parser.parse_args()
    run_interpolations(args.locid, args.measid, args.meid, args.covid, args.covname)