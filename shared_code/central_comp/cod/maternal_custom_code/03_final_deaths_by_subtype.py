##########################################################################
# Description: This code simply takes a cause fraction file and takes an
# envelope file, and multiplies them together to come up with
# estimated deaths to each subtype.
# Input: year: year of interest
# log_dir: directory for where you want log files to be saved
# jobname: to be used to name the current logging file
# env_model_vers: model vers of envelope from codem or NONE for codcorrect
# source_id: me_id of cause fractions
# target_id: cause_id of saved output draws
# out_dir: directory for where to save final death count datasets
# Output: A .csv saved to the directory specified, with final death count
# datasets for the year and location specified.
##########################################################################

from __future__ import division
import sys
from PyJobTools import rlog

import maternal_fns
from transmogrifier.gopher import draws

log_dir, jobname, env_model_vers, source_id, target_id, out_dir = sys.argv[1:7]

# get list of locations
locations = maternal_fns.get_locations()

# logging
rlog.open('%s/%s.log' % (log_dir, jobname))
rlog.log('out_dir is %s' % out_dir)

# set up columns we want to subset
columns = maternal_fns.filter_cols()
columns.remove('measure_id')
index_cols = [col for col in columns if not col.startswith('draw_')]

# read maternal disorders envelope
# CAUSES get multiplied by the Late corrected env from codem
# TIMINGS get multiplied by the CoDcorrect env
rlog.log("reading in envelope draws")
if 'timing' in jobname:
    env = draws(gbd_ids={'cause_ids': [366]}, source='codcorrect',
                measure_ids=[1], sex_ids=[2], location_ids=locations)
else:
    env = draws(gbd_ids={'cause_ids': [366]}, source='codem', sex_ids=[2],
                status=int(env_model_vers))
env = env[env.location_id.isin(locations)]
# we only want maternal age groups
env = env[env.age_group_id.isin(range(7, 16))]
# we only want index cols & draws as columns, w multiindex
env = env[columns].set_index(index_cols).sort_index()

# read cfs
rlog.log("reading in cfs")
cfs = draws(gbd_ids={'modelable_entity_ids': [source_id]}, source='dismod',
            measure_ids=[18], sex_ids=[2])
cfs = cfs[cfs.location_id.isin(locations)]
# we only want maternal age groups
cfs = cfs[cfs.age_group_id.isin(range(7, 16))]
# we only want index cols & draws as columns, w multiindex
cfs = cfs[columns].set_index(index_cols).sort_index()

# multiply to get final deaths
rlog.log("multiplying to get deaths")
final_deaths = cfs * env
final_deaths.reset_index(inplace=True)
final_deaths['measure_id'] = 1

# save
if "timing" in jobname:
    final_deaths['modelable_entity_id'] = target_id
    final_deaths.to_hdf('FILEPATH' % out_dir,
                        'draws', format='table', mode='w',
                        data_columns=['measure_id', 'location_id', 'year_id',
                                      'age_group_id', 'sex_id'])
else:
    final_deaths['cause_id'] = target_id
    final_deaths.to_csv('FILEPATH.csv' % (out_dir, target_id),
                        index=False, encoding='utf-8')

rlog.log('Finished!')
