##########################################################################
# Author: AUTHOR
# Description: We need to make sure that cause fractions sum to one, both
#   across sub-causes and across sub-times. Here, we proportionately
#   rescale cause fractions for each country-age-year so they sum
#   correctly.
# Note: Since 'late' is both a sub-cause and a sub-time, we first
# rescale subcauses, then 'freeze' the late cause fractions when
# we scale the sub-times (which is after codcorrect is run).
# Input: log_dir: directory for where you want log files to be saved
# jobname: to be used to name the current logging file
# cluster_dir: parent directory where scaled output files will be stored
# year: year of interest
# Output: A .csv saved to the directory specified, with scaled datasets for the
# year and location specified.
##########################################################################
import sys
import os
import pandas as pd
import logging

import maternal_fns
from python_emailer import server, emailer
from get_draws.api import get_draws

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("maternal_custom.01_scale_fractions")
##############################################
# PREP WORK:
# set directories and other preliminary data
##############################################
print('starting job!')

if len(sys.argv) > 1:
    jobname, cluster_dir, year, dep_map_type = sys.argv[1:5]
else:
    jobname = "dismod_cf_correct_1995"
    cluster_dir = "FILEPATH"
    year = 1995
    dep_map_type = "mmr"

year = int(year)

# do all the prep work
locs = maternal_fns.get_locations()
columns = maternal_fns.filter_cols()
index_cols = [col for col in columns if not col.startswith('draw_')]

# get dependency_map
dep_map = pd.read_csv("FILEPATH" % dep_map_type,
                      header=0).dropna(axis='columns', how='all')

# subset dep_map for the step that we're on
if "timing" in jobname:
    step_df = dep_map[(dep_map.step == 4) &
                      (dep_map.source_id != 'codcorrect')]
    held_constant = ['late maternal death']
else:
    step_df = dep_map.ix[dep_map.step == 1]
    held_constant = ['hiv maternal death', 'late maternal death']
if dep_map_type == "mmr":
    source = 'epi'
    measure_id = 19
else:
    source = 'dismod'
    measure_id = 18

# set the ME's held constant
held_constant_me = step_df[step_df.target_note.isin(held_constant)].source_id.values.tolist()

#######################################################################
# STEP 1: FOR EACH CAUSE, EXTRACT FILES, GET SUM BY GROUP + TOTAL SUM
#######################################################################
print('getting data')
logger.info('Getting data')
all_data = {}
held_constant = {}
summed_idx = 0
summed_idx_constant = 0

for index, row in step_df.iterrows():
    target_id = row['target_id']

    # for late, we're always going to pull the measure ID 18 because
    # it is always run in DisMod.
    if row['target_note'] == 'late maternal death' and 'timing' not in jobname:
        measure_id = 18
    
    try:
        subtype_df = get_draws(gbd_id=row['source_id'],
                               gbd_id_type='modelable_entity_id',
                               source=source,
                               measure_id=[measure_id],
                               sex_id=[2],
                               year_id=[year],
                               status='best')
    except:
        subtype_df = pd.read_hdf('%s/%s/%s_2.h5' % (cluster_dir,
                                                    row['source_id'],
                                                    year), 'draws')
    subtype_df = subtype_df.ix[(subtype_df.location_id.isin(locs)) &
                               (subtype_df.age_group_id.isin(list(range(7, 16))))]
    # set all measure IDs to proportion space
    subtype_df['measure_id'] = 18
    subtype_df = subtype_df[columns].set_index(index_cols).sort_index()
    
    if row['source_id'] in held_constant_me:
        held_constant[target_id] = subtype_df.copy(deep=True)

        if summed_idx_constant == 0:
            held_constant['Summed_Constants'] = subtype_df
        else:
            held_constant['Summed_Constants'] = (held_constant['Summed_Constants'] +
                                                 subtype_df)
        summed_idx_constant += 1
    else:
        # save this dataframe, and also sum it to all other subtypes
        all_data[target_id] = subtype_df
        
        # note that we do not include Late_df in the sum of subtimes,
        # or hiv_df in the sum of subcauses to ease calculation later
        if summed_idx == 0:
            all_data['Summed_Subtypes'] = subtype_df
        else:
            all_data['Summed_Subtypes'] = (all_data['Summed_Subtypes'] +
                                           subtype_df)
        summed_idx += 1

#######################################################################
# STEP 2: DIVIDE EACH DATASET BY THE TOTAL SUM TO GET PROPORTIONS
#######################################################################

print('dividing to get proportions')
logger.info('dividing to get proportions')
final_data = {}

complement = held_constant['Summed_Constants'].applymap(lambda x: 1 - x)
Q = all_data['Summed_Subtypes'] / complement

for index, row in step_df.iterrows():
    target_id = row['target_id']
    if row['source_id']in held_constant_me:
        final_data[target_id] = held_constant[target_id]
    else:
        final_data[target_id] = all_data[target_id] / Q
    
    out_dir = '%s/%s' % (cluster_dir, row['target_id'])
    logger.info('saving %s to %s' % (target_id, out_dir))
    output_df = final_data[target_id].copy(deep=True)
    output_df['modelable_entity_id'] = target_id
    output_df.reset_index(inplace=True)
    output_df.to_hdf('%s/%s_2.h5' % (out_dir, year), key='draws',
                     mode='w', format='table', data_columns=['location_id',
                                                             'year_id',
                                                             'age_group_id',
                                                             'sex_id'])

epsilon = 0.00001
summed = sum(final_data.values())
abs_diff = summed.applymap(lambda x: abs(1 - x))
not_right = abs_diff[abs_diff > epsilon].dropna()

if not not_right.empty:
    s = server('SERVER')
    s.set_user('USER')
    s.set_password('PASSWORD')
    s.connect()

    e = emailer(s)
    user = os.environ.get("USER")
    me = 'USER' % user
    e.add_recipient('%s' % me)

    e.set_subject('MATERNAL')
    e.set_body('CAUSE FRACTIONS DO NOT SUM TO one FOR YEAR %s' % year)

    e.send_email()
    s.disconnect()

logger.info('Finished!')
