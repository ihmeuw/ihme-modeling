##########################################################################
# Description: We need to make sure that cause fractions sum to one, both
# across sub-causes and across sub-times. Here, we proportionately
# rescale cause fractions for each country-age-year so they sum
# correctly.
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
from PyJobTools import rlog

import maternal_fns
from python_emailer import server, emailer
from transmogrifier.gopher import draws

##############################################
# PREP WORK:
# set directories and other preliminary data
##############################################
print 'starting job!'

log_dir, jobname, cluster_dir, year, dep_map_type = sys.argv[1:6]

year = int(year)

# logging
rlog.open('%s/%s.log' % (log_dir, jobname))

# do all the prep work
locs = maternal_fns.get_locations()
columns = maternal_fns.filter_cols()
index_cols = [col for col in columns if not col.startswith('draw_')]

# get dependency_map
dep_map = pd.read_csv("FILEPATH.csv" % dep_map_type,
                      header=0).dropna(axis='columns', how='all')

# subset dep_map for the step that we're on
if "timing" in jobname:
    step_df = dep_map[(dep_map.step == 4) &
                      (dep_map.source_id != 'codcorrect')]
    held_constant_me = 376
else:
    step_df = dep_map.ix[dep_map.step == 1]
    held_constant_me = 9015

#######################################################################
# STEP 1: FOR EACH CAUSE, EXTRACT FILES, GET SUM BY GROUP + TOTAL SUM
#######################################################################
print 'getting data'
rlog.log('getting data')
all_data = {}
summed_idx = 0

for index, row in step_df.iterrows():
    target_id = row['target_id']
    try:
        subtype_df = draws(gbd_ids={'modelable_entity_ids':
                                    [row['source_id']]},
                           source='dismod',
                           measure_ids=[18],
                           sex_ids=[2],
                           year_ids=[year])
    except (ValueError, OSError):  # pull data from where interp saves it
        subtype_df = pd.read_hdf('%s/%s/%s_2.h5' % (cluster_dir,
                                                    row['source_id'],
                                                    year), 'draws')
    subtype_df = subtype_df.ix[(subtype_df.location_id.isin(locs)) &
                               (subtype_df.age_group_id.isin(range(7, 16)))]
    subtype_df = subtype_df[columns].set_index(index_cols).sort_index()

    if row['source_id'] == str(held_constant_me):
        held_constant_df = subtype_df.copy(deep=True)
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

print 'dividing to get proportions'
rlog.log('dividing to get proportions')
final_data = {}

# for the 'by time' analysis, we want: (Ante + Intra + Post)/Q + Late = 1,
# So Q = (Ante + Intra + Post)/(1-Late). For HIV, Q = (all subcauses)/(1-HIV)
# Here, we generate Q for use later.
complement = held_constant_df.applymap(lambda x: 1 - x)
Q = all_data['Summed_Subtypes'] / complement

for index, row in step_df.iterrows():
    target_id = row['target_id']
    if row['source_id'] == str(held_constant_me):
        final_data[target_id] = held_constant_df
    else:
        final_data[target_id] = all_data[target_id] / Q

    out_dir = '%s/%s' % (cluster_dir, row['target_id'])
    rlog.log('saving %s to %s' % (target_id, out_dir))
    output_df = final_data[target_id].copy(deep=True)
    output_df['modelable_entity_id'] = target_id
    output_df.reset_index(inplace=True)
    output_df.to_hdf('%s/%s_2.h5' % (out_dir, year), key='draws',
                     mode='w', format='table', data_columns=['location_id',
                                                             'year_id',
                                                             'age_group_id',
                                                             'sex_id'])

# make sure subtypes sum to 1 (ish); send the user an angry email otherwise
epsilon = 0.00001
summed = sum(final_data.itervalues())
abs_diff = summed.applymap(lambda x: abs(1 - x))
not_right = abs_diff[abs_diff > epsilon].dropna()

if not not_right.empty:
    s = server('ADDRESS')
    s.set_user('ADDRESS')
    s.set_password('PASSWORD')
    s.connect()

    e = emailer(s)
    user = os.environ.get("USER")
    me = '%s@uw.edu' % user
    e.add_recipient('%s' % me)

    e.set_body('CAUSE FRACTIONS DO NOT SUM TO one FOR YEAR %s' % year)

    e.send_email()
    s.disconnect()

rlog.log('Finished!')
