#import reshape
import argparse
import numpy as np
import pandas as pd
from transmogrifier.draw_ops import get_draws
from glob import glob

# Setting file locations
indir = "{FILEPATH}"
exp_dir = "{FILEPATH}"
tmrel_dir = "{FILEPATH}"
merge_cols = ['location_id', 'year_id', 'age_group_id', 'sex_id']
ages = [{AGE GROUP IDS}]
# For each location...
def get_tmrel(location):
    tmrel_appended = []
    for a in ages:
        cf_data = pd.read_hdf('{FILEPATH}',
                              where='location_id==%s & age_group_id==%s' % (location, a))
        age_df = pd.read_csv('/{FILEPATH}/priors/age_group_table.csv')
        cf_merged = pd.merge(left=cf_data, right=age_df, how='left', left_on='age_group_id', right_on='age_group_id')
        cf_merged.rename(columns={'age_group_years_start': 'gbd_age_start', 'age_group_years_end': 'gbd_age_end'}, inplace=True)
        tmrel_appended.append(cf_merged)
    tmrel_appended=pd.concat(tmrel_appended, axis=0)
    tmrel_appended.to_csv('%s/tmrel_%s.csv' % (tmrel_dir, location))
    return tmrel_appended

def get_obs_shift(location):
    anemia_files = glob("%s/%s_*.h5" % (indir, location))
    dfs = []
    for f in anemia_files:
        df = pd.read_hdf(f)
        df = df[(df.subtype == 'nutrition_iron') & (df.draw == 0)]
        df.drop(['draw', 'subtype', 'mild', 'moderate', 'severe', 'prior_hb_shift', 'prior_subtype_prop'],
                axis=1,
                inplace=True
                )
        df.fillna(0, inplace=True)
        num = df._get_numeric_data()
        num[num < 0] = 0
        dfs.append(df)
    dfs_appended = pd.concat(dfs, axis=0)
    assert dfs_appended.shape==(276, 5), "df has incorrect dimensions"
    return dfs_appended


def merge_subtypes(location):
    prev_dir = '{FILEPATH}'
    mild = pd.read_hdf('%s/{MODELABLE ENTITY ID/{MEASURE ID}_%s.h5' % (prev_dir, location))
    mild.drop('subtype', axis=1, inplace=True)
    mild.rename(columns={'draw_%s' % d: 'mild_%s' % d for d in range(1000)}, inplace=True)
    moderate = pd.read_hdf('%s/{MODELABLE ENTITY ID/{MEASURE ID}_%s.h5' % (prev_dir, location))
    moderate.drop('subtype', axis=1, inplace=True)
    moderate.rename(columns={'draw_%s' % d: 'moderate_%s' % d for d in range(1000)}, inplace=True)
    severe = pd.read_hdf('%s/{MODELABLE ENTITY ID/{MEASURE ID}_%s.h5' % (prev_dir, location))
    severe.drop('subtype', axis=1, inplace=True)
    severe.rename(columns={'draw_%s' % d: 'severe_%s' % d for d in range(1000)}, inplace=True)
    merged = pd.merge(left=mild, right=moderate, how='left',
                      left_on=merge_cols, right_on=merge_cols)
    merged = pd.merge(left=merged, right=severe, how='left',
                      left_on=merge_cols, right_on=merge_cols)
    assert merged.shape == (276, 3004), "merge failed"
    return merged

def calculate_total(df):
    for i in range(0, 1000):
        df['total_%s' % i] = df['mild_%s' % i] + df['moderate_%s' % i] + df['severe_%s' % i]
    cols = [c for c in df.columns if ((c[0:4] != 'mild') & (c[0:8] != 'moderate') & (c[0:6] != 'severe'))]
    return df[cols]


def multiply_by_shift(location, df):
    obs = get_obs_shift(location)
    merged = pd.merge(left=df, right=obs, how='left',
                      left_on=merge_cols, right_on=merge_cols)
    for i in range(0, 1000):
        merged['total_%s' % i] = merged['total_%s' % i] * merged['observed_hb_shift']
    return merged


def subtract_from_counterfactual(shiftprev_df, tmrel_df):
    df = pd.merge(left=shiftprev_df, right=tmrel_df, how='left',
                  left_on=merge_cols, right_on=merge_cols)
    df.drop(['gbd_age_start', 'gbd_age_end', 'observed_hb_shift'], axis=1, inplace=True)
    for i in range(0, 1000):
        df['exp_%s' % i] = df['tmrel_%s' % i] - df['total_%s' % i]
    cols = [c for c in df.columns if ((c[0:5] != 'tmrel') & (c[0:5] != 'total'))]
    expdf = df[cols]
    return expdf

def add_stdevs(df, location):
    stdevs = get_draws('modelable_entity_id', {MODELABLE ENTITY ID}, 'dismod', location_ids=location)
    renames = {'draw_%s' % d: 'exp_%s' % d for d in range(1000)}
    stdevs.rename(columns=renames, inplace=True)
    stdevs = stdevs.drop(['measure_id', 'location_id', 'modelable_entity_id', 'model_version_id'], axis=1)
    stdevs['risk'] = 'nutrition_iron'
    stdevs['parameter'] = 'sd'
    stdevs = stdevs[stdevs['age_group_id'].isin(ages)]
    df = pd.concat([df, stdevs], axis=0)
    df.to_csv('%s/exp_%s.csv' % (exp_dir, location))

def get_exposure(location):
    tmrel_df = get_tmrel(location)
    st_df = merge_subtypes(location)
    total = calculate_total(st_df)
    shiftprev_df = multiply_by_shift(location, total)
    cf_subtract_df = subtract_from_counterfactual(shiftprev_df, tmrel_df)
    add_stdevs(cf_subtract_df, location)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("location", help="location to write", type=str)
    args = parser.parse_args()
    get_exposure(args.location)