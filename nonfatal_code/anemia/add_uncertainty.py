import pandas as pd
import argparse
import numpy as np
import re

from hierarchies import dbtrees
from transmogrifier.draw_ops import get_draws

lt = dbtrees.loctree(None, {LOCATION SET ID})
locs = [l.id for l in lt.leaves()]
yids = [{YEAR IDS}]
sids = [{SEX IDS}]
agids = [{AGE GROUP IDS}]

indir = '{FILEPATH}'
outdir = '{FILEPATH}'
uncertainty_map = pd.read_csv(
    '/{FILEPATH}/priors/uncertainty_map.csv')

def grab_meid_instructions(meid):
    """returns the row corresponding to relevant modelable
     entity ID from map as a dictionary with column names as keys"""
    df = uncertainty_map[uncertainty_map.out_meid==meid]
    dt = df.to_dict(orient='records')
    dt = dt[0]
    return dt

def env_meid_instructions(instructions):
    if instructions['severity'] == 'mild':
        env_meid = {MODELABLE ENTITY ID}
    elif instructions['severity'] == 'moderate':
        env_meid = {MODELABLE ENTITY ID}
    elif instructions['severity'] == 'severe':
        env_meid = {MODELABLE ENTITY ID}
    return env_meid

def filter_residuals(instructions):
    residual = False
    if instructions['in_meid'] == 'RESIDUAL':
        residual = True
    return residual

def determine_msid(instructions):
    if instructions['use_incidence'] == 1:
        msid = {MEASURE ID}
    else:
        msid = {MEASURE ID}
    return msid

def get_subtype_draws(meid, loc, msid):
    df = get_draws('modelable_entity_id',
                   meid,
                   'dismod',
                   location_ids=loc,
                   year_ids=yids,
                   sex_ids=sids,
                   age_group_ids=agids,
                   measure_ids=msid,
                   gbd_round_id={GBD ROUND ID})
    df = df.drop(['modelable_entity_id', 'model_version_id', 'measure_id'], axis=1)
    renames = {'draw_%s' % d: 'st_%s' % d for d in range(1000)}
    df.rename(columns=renames, inplace=True)
    draws = [col for col in list(df) if col.startswith('st')]
    df['mean'] = df[draws].mean(axis=1)
    for col in df[draws]:
        df[col] = df[col]/df['mean']
    df.fillna(value=1, inplace=True)
    df = df.drop(['mean'], axis=1)
    return df

def get_envelope_draws(meid, loc):
    df = get_draws('modelable_entity_id',
                   meid,
                   'dismod',
                   location_ids=loc,
                   year_ids=yids,
                   sex_ids=sids,
                   age_group_ids=agids,
                   gbd_round_id={GBD ROUND ID})
    df = df.drop(['modelable_entity_id', 'model_version_id', 'measure_id'], axis=1)
    renames = {'draw_%s' % d: 'env_%s' % d for d in range(1000)}
    df.rename(columns=renames, inplace=True)
    draws = [col for col in list(df) if col.startswith('env')]
    df['mean'] = df[draws].mean(axis=1)
    for col in df[draws]:
        df[col] = df[col]/df['mean']
    df.fillna(value=1, inplace=True)
    df= df.drop(['mean'], axis=1)
    return df

def merge_env_st(env_meid, st_meid, loc, msid):
    env = get_envelope_draws(env_meid, loc)
    st = get_subtype_draws(st_meid, loc, msid)
    merged = pd.merge(left=env, right=st, how='left',
                      left_on=['location_id', 'year_id', 'age_group_id', 'sex_id'],
                      right_on=['location_id', 'year_id', 'age_group_id', 'sex_id'])

    for i in range(0, 1000):
        merged['draw_%s' % i] = merged['env_%s' % i] * merged['st_%s' % i]

    cols = [c for c in merged.columns if ((c[0:2] == 'st') | (c[0:3] == 'env'))]
    merged.drop(cols, axis=1, inplace=True)
    return merged

def create_raw_output_mean(meid, loc, ndraws):
    df = pd.read_hdf('%s/%s/{MEASURE ID}_%s.h5' %(indir, str(meid), str(loc)))
    draws = ['draw_%s' %d for d in range(ndraws)]
    df['mean'] = df[draws].mean(axis=1, skipna=True)
    draw_cols = [col for col in list(df) if col.startswith('draw')]
    df = df.drop(draw_cols, axis=1)
    df.fillna(0, inplace=True)
    num = df._get_numeric_data()
    num[num < 0] = 0
    return df

def make_uncertain_draws(meid, ndraws):
    instructions = grab_meid_instructions(meid)
    env_meid = env_meid_instructions(instructions)
    residual = filter_residuals(instructions)
    msid = determine_msid(instructions)
    draw_colnames = ['draw_%s' %i for i in range(0, 1000)]
    other_colnames = ['age_group_id', 'location_id', 'year_id', 'sex_id', 'subtype']
    acceptable_names = draw_colnames + other_colnames
    for loc in locs:
        if residual:
            uncertain_df = get_envelope_draws(env_meid, loc)
            renames = {'env_%s' % d: 'draw_%s' % d for d in range(1000)}
            uncertain_df.rename(columns=renames, inplace=True)
        else:
            uncertain_df = merge_env_st(env_meid, int(instructions['in_meid']), loc, msid)
        out_mean = create_raw_output_mean(meid, loc, ndraws)
        uncertain_draws = pd.merge(left=uncertain_df, right=out_mean, how='left',
                                   left_on=['location_id', 'year_id', 'age_group_id', 'sex_id'],
                                   right_on=['location_id', 'year_id', 'age_group_id', 'sex_id'])
        for i in range(0, 1000):
            uncertain_draws['draw_%s' %i] = uncertain_draws['draw_%s' %i] * uncertain_draws['mean']
        for i in list(uncertain_draws):
            if i not in acceptable_names:
                uncertain_draws.drop(i, axis=1, inplace=True)
        uncertain_draws.fillna(0, inplace=True)
        uncertain_draws.to_hdf('%s/%s/{MEASURE ID}_%s.h5' %(outdir, str(meid), str(loc)),
                               'draws',
                               mode='w',
                               data_columns=['location_id', 'year_id', 'age_group_id', 'sex_id'],
                               format = 'table')

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("meid", help="output modelable entity ID", type=int)
    parser.add_argument("ndraws", help="number of draws used for causal attribution run", type=int)
    args = parser.parse_args()
    make_uncertain_draws(args.meid, args.ndraws)