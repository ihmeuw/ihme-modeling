
"""

Description: This code simply takes a cause fraction file and takes an
envelope file, and multiplies them together to come up with
estimated deaths to each subtype.

Inputs: 
    year: year of interest
    log_dir: directory for where you want log files to be saved
    jobname: to be used to name the current logging file
    env_model_vers: model vers of envelope from codem or NONE for codcorrect
    source_id: me_id of cause fractions
    target_id: cause_id of saved output draws
    out_dir: directory for where to save final death count datasets

Output: A .csv saved to the directory specified, with final death count
datasets for the year and location specified.
"""

import os
import logging
import sys

import pandas as pd

import maternal_fns
from gbd import decomp_step as decomp
from get_draws.api import get_draws

PULL_RESULTS = False


# get args
jobname, env_model_vers, source_id, target_id, out_dir, decomp_step_id, cluster_dir = sys.argv[1:8]

source_id = int(source_id)
target_id = int(target_id)
decomp_step_id = int(decomp_step_id)

# get list of locations
locations = maternal_fns.get_locations()

# logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("maternal_custom.03_final_deaths_by_subtype")
logger.info("Creating final deaths by subtype")

# set up columns we want to subset
columns = maternal_fns.filter_cols()
columns.remove('measure_id')
index_cols = [col for col in columns if not col.startswith('draw_')]

# read maternal disorders envelope
# CAUSES get multiplied by the Late corrected env from codem
# TIMINGS get multiplied by the CoDcorrect env
logging.info("Reading in envelope draws")
if 'timing' in jobname:
    env = get_draws(
        gbd_id=366,
        gbd_id_type='cause_id',
        gbd_round_id=maternal_fns.GBD_ROUND_ID,
        source='codcorrect',
        measure_id=[1],
        sex_id=[2],
        location_id=locations,
        decomp_step=decomp.decomp_step_from_decomp_step_id(decomp_step_id))
else:
    env = get_draws(
        gbd_id=366,
        gbd_id_type='cause_id',
        source='codem',
        version_id=int(env_model_vers),
        gbd_round_id=maternal_fns.GBD_ROUND_ID,
        decomp_step=decomp.decomp_step_from_decomp_step_id(decomp_step_id))

env = env[env.location_id.isin(locations) & env.age_group_id.isin(range(7, 16))]

# we only want index cols & draws as columns, w multiindex
env = env[columns].set_index(index_cols).sort_index()

# read cfs
logger.info("Reading in cause fraction for modelable entity {}".format(source_id))
if PULL_RESULTS:
    logger.info("Pulling results from get_draws")
    cfs = get_draws(
        gbd_id=source_id,
        gbd_id_type='modelable_entity_id',
        source='epi',
        measure_id=[18],
        sex_id=[2],
        decomp_step=decomp.decomp_step_from_decomp_step_id(decomp_step_id),
        gbd_round_id=maternal_fns.GBD_ROUND_ID)
else:
    logger.info("Pulling results from draw files.")
    dfs = []
    for year in list(range(1980, 2020)):
        df = pd.read_hdf(
            os.path.join(cluster_dir, str(source_id), '{}_2.h5'.format(year)),
            key='draws')
        dfs.append(df)
    cfs = pd.concat(dfs)

cfs = cfs[cfs.location_id.isin(locations)]
# we only want maternal age groups
cfs = cfs[cfs.age_group_id.isin(range(7, 16))]
# we only want index cols & draws as columns, w multiindex
cfs = cfs[columns].set_index(index_cols).sort_index()

# multiply to get final deaths
logger.info("multiplying to get deaths")
final_deaths = cfs * env
final_deaths.reset_index(inplace=True)
final_deaths['measure_id'] = 1

# save
if "timing" in jobname:
    final_deaths['modelable_entity_id'] = target_id
    final_deaths.to_hdf(
        '%s/all_draws.h5' % out_dir,
        'draws',
        format='table',
        mode='w',
        data_columns=[
            'measure_id', 'location_id', 'year_id',
            'age_group_id', 'sex_id'])
else:
    final_deaths['cause_id'] = target_id
    final_deaths.to_hdf(
        '%s/final_deaths_%s.h5' % (out_dir, target_id),
        key='draws',
        data_columns=[
            'location_id', 'year_id', 'age_group_id',
            'sex_id', 'cause_id', 'measure_id'],
        format='table', mode='w')

logger.info('Finished!')
