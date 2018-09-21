import sys
import os
import pandas as pd

from PyJobTools import rlog
import maternal_fns
from transmogrifier.gopher import draws
from db_queries.get_model_results import get_model_results

log_dir, jobname, env_model_vers, out_dir, dep_map_type = sys.argv[1:6]

# do all the prep work
dep_map = pd.read_csv("FILEPATH.csv" % dep_map_type,
                      header=0).dropna(axis='columns', how='all')
step_df = dep_map.ix[dep_map.step == 2].reset_index()
columns = maternal_fns.filter_cols()
columns.remove('measure_id')
index_cols = [col for col in columns if not col.startswith('draw_')]

# logging
rlog.open('%s/%s.log' % (log_dir, jobname))
rlog.log("Correcting for the underreporting of Late Maternal deaths")
rlog.log('out_dir is %s' % out_dir)

# get list of locations
locs = maternal_fns.get_locations()

# get list of location/years that don't need correction
rlog.log('Pulling in adjustment csv')
adjust_df = pd.read_csv('FILEPATH.csv' % (os.getcwd()))
adjust_df = adjust_df[['location_id', 'year_id', 'adj_factor']]

# get original codem envelope
rlog.log("Pulling in codem envelope")
env = draws(gbd_ids={'cause_ids': [366]}, source='codem',
            status=int(env_model_vers))
env = env[columns]
env = env[(env.age_group_id.isin(range(7, 16))) & (env.location_id.isin(locs))]

# get prop from Late dismod model, merge on adjust_df, keep only model results
# for the locs/years that require adjustment
rlog.log("Pulling in late dismod model")
prop = get_model_results(gbd_team='epi',
                         gbd_id=int(step_df.ix[0, 'source_id']),
                         measure_id=18,
                         location_id=-1,
                         location_set_id=35)
prop_columns = index_cols + ['mean']
prop = prop[prop_columns]
prop = prop[(prop.age_group_id.isin(range(7, 16))) &
            (prop.location_id.isin(locs))]
prop['prop'] = 1 / (1 - prop['mean'])
prop.drop('mean', axis=1, inplace=True)

# multiply every location/year that doesn't already have a adj_factor of 1, by
# 1/(1-scaledmean_lateprop)
env = env.merge(adjust_df, on=['location_id', 'year_id'], how='left')

env_not_adjust = env[env.adj_factor == 1]
env_adjust = env[env.adj_factor.isnull()]

env_adjust = env_adjust.merge(prop, on=index_cols)
for i in xrange(1000):
    env_adjust['draw_%s' % i] = env_adjust['draw_%s' % i] * env_adjust['prop']
env_adjust.drop('prop', axis=1, inplace=True)

env = pd.concat([env_not_adjust, env_adjust])
env['cause_id'] = 366
env['measure_id'] = 1

rlog.log("Exporting to %s" % out_dir)
env.to_csv('FILEPATH.csv' % out_dir, index=False)
