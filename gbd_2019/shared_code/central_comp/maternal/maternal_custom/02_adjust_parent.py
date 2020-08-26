"""
adjust_parent.py takes the relevant codcorrect envelope and scales cause_id 366
to the best late maternal death dismod proportion model, as dictated in
dependency_map_mmr.csv.
"""


import os
import logging
import sys
import pandas as pd

from get_draws.api import get_draws
from db_queries.get_model_results import get_model_results
from db_queries import get_envelope
from gbd import decomp_step as decomp
import maternal_fns

jobname, env_model_vers, out_dir, dep_map_type, decomp_step_id, cluster_dir = sys.argv[1:7]
env_model_vers = int(env_model_vers)
decomp_step_id = int(decomp_step_id)

pull_results = True

# do all the prep work
dep_map = pd.read_csv("dependency_map_%s.csv" % dep_map_type,
                      header=0).dropna(axis='columns', how='all')
step_df = dep_map.loc[dep_map.step == 2].reset_index()

columns = maternal_fns.filter_cols()
columns.remove('measure_id')
index_cols = [col for col in columns if not col.startswith('draw_')]

# logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("maternal_custom.02_adjust_parent")
logger.info("Correcting underreporting for late maternal deaths")

# get list of locations
locs = maternal_fns.get_locations()

# get list of location/years that don't need correction
adjust_df = pd.read_csv(os.getcwd() + '/late_start_years_update.csv',
                        encoding='latin-1')
adjust_df = adjust_df[['location_id', 'year_to_start']]

# get original codem envelope
logger.info("Pulling in codem envelope")
env = get_draws(
    gbd_id=366,
    gbd_id_type='cause_id',
    source='codem',
    version_id=int(env_model_vers),
    location_id=locs,
    age_group_id=list(range(7, 16)),
    decomp_step=decomp.decomp_step_from_decomp_step_id(decomp_step_id),
    gbd_round_id=maternal_fns.GBD_ROUND_ID)
env = env[columns + ['envelope']]
env = env[(env.age_group_id.isin(range(7, 16))) & (env.location_id.isin(locs))]

# get proportion from Late DisMod model, merge on adjust_df, keep only model results
# for the locs/years that require adjustment
draw_cols = ['draw_{}'.format(i) for i in range(1000)]
env[draw_cols] = env[draw_cols].divide(env['envelope'], axis="index")

logger.info("Pulling in late dismod model")
if pull_results:
    logger.info("Pulling from get_model_results")
    prop = get_model_results(
        gbd_team='epi',
        gbd_id=int(step_df.loc[0, 'source_id']),
        measure_id=18,
        location_id=locs,
        sex_id=2,
        location_set_id=35,
        decomp_step=decomp.decomp_step_from_decomp_step_id(decomp_step_id),
        age_group_id=list(range(7, 16)),
        gbd_round_id=maternal_fns.GBD_ROUND_ID)
else:
    logger.info("Pulling from draw files")
    gbd_id = int(step_df.loc[0, 'source_id'])
    dfs = []
    for year in list(range(1980, 2020)):
        df = pd.read_hdf(os.path.join(cluster_dir, str(gbd_id), '{}_2.h5'.format(year)), key='draws')
        dfs.append(df)
    prop = pd.concat(dfs)
    prop['mean'] = prop[draw_cols].mean(axis=1)
    prop.drop(draw_cols, inplace=True, axis=1)

prop_columns = index_cols + ['mean']
prop = prop[prop_columns]
prop['prop'] = 1 / (1 - prop['mean'])
prop.drop('mean', axis=1, inplace=True)

# multiply every location/year that doesn't already have a adj_factor of 1, by
# 1/(1-scaledmean_lateprop)

env = env.merge(adjust_df, on=['location_id'], how='left')
env['adj'] = 1
env.loc[env.year_id >= env.year_to_start, 'adj'] = 0
env.set_index(index_cols, inplace=True)
env.sort_index(inplace=True)

prop.set_index(index_cols, inplace=True)
prop.sort_index(inplace=True)

# subset to columns that need to be adjusted
final = env.join(prop)
final.loc[final['adj'] == 0, 'prop'] = 1

final[draw_cols] = final[draw_cols].multiply(final['prop'], axis="index")

# pull in mortality draws
mort = get_envelope(
    location_id=locs, location_set_id=25,
    age_group_id=list(range(7, 16)), sex_id=2,
    year_id=list(range(1980, 2020)),
    decomp_step=decomp.decomp_step_from_decomp_step_id(decomp_step_id),
    gbd_round_id=maternal_fns.GBD_ROUND_ID)
mort = mort[index_cols + ['mean']]
mort.set_index(index_cols, inplace=True)

final = final.join(mort)
final[draw_cols] = final[draw_cols].multiply(final['mean'], axis="index")

final['cause_id'] = 366
final['measure_id'] = 1

final.reset_index(inplace=True)

logger.info("Exporting to %s" % out_dir)
final.to_hdf('%s/late_corrected_maternal_envelope.h5' % out_dir, key='draws',
             data_columns=[
                 'location_id',
                 'year_id',
                 'age_group_id',
                 'sex_id',
                 'cause_id',
                 'measure_id'],
             format='table', mode='w')
