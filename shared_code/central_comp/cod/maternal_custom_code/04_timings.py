##########################################################################
# Description: Dalynator outputs deaths draws for the full dalynator maternal
#              envelope, as well as for each of the subcauses. Because late is
#              a subcause that becomes a timing, we divide late by the full
#              env, for each location and year, to get late cause fractions.
#              The output of this will be used as a frozen part of the scaling
#              of all the timings. Later we will save these to the cluster_dir
#              using the modelable_entity_id that we will use to upload the
#              scaled version of this to the epi database
# Input: log_dir: directory for where you want log files to be saved
#        year: year of interest
#        dalynator_dir: directory where dalynator death draw results are stored
#        env_id: cause_id of all maternal disorders
#        late_id: cause_id of the late cause
#        output_dir: directory for where to save Late cause fractions
# Output: A .csv saved to the directory specified, with Late cause fractions
#         for the year and location specified.
##########################################################################

import sys
import maternal_fns
from PyJobTools import rlog

from transmogrifier.gopher import draws

##############################################
# PREP WORK:
# set directories and other preliminary data
##############################################

log_dir, year, env_id, late_id, out_dir = sys.argv[1:6]

year = int(year)
env_id = int(env_id)
late_id = int(late_id)
cause = [env_id, late_id]

# get list of locations
locations = maternal_fns.get_locations()

# set up columns we want to subset
columns = maternal_fns.filter_cols()
index_cols = [col for col in columns if not col.startswith('draw_')]

# logging
rlog.open('FILEPATH.log' % (log_dir, year))
rlog.log('')
rlog.log('Starting to get late cause fractions')

##############################################
# GET LATE CAUSE FRACTIONS:
##############################################
codcorrect_df = draws(gbd_ids={'cause_ids': [env_id, late_id]},
                      source='codcorrect', year_ids=[year], sex_ids=[2],
                      measure_ids=[1])
codcorrect_df['measure_id'] = 1
codcorrect_df = codcorrect_df[codcorrect_df.age_group_id.isin(range(7, 16))]

envelope_df = codcorrect_df[codcorrect_df.cause_id == env_id]
late_df = codcorrect_df[codcorrect_df.cause_id == late_id]

# we only want index_cols and draws as columns
envelope_df = envelope_df[columns].set_index(index_cols).sort()
late_df = late_df[columns].set_index(index_cols).sort()

# calculate late cause fractions
rlog.log('Calculating late cfs for year %s' % year)
late_cfs = late_df / envelope_df

late_cfs.reset_index(inplace=True)
late_cfs['modelable_entity_id'] = late_id
late_cfs['measure_id'] = 18

# save late cause fractions
late_cfs.to_hdf('FILEPATH.h5' % (out_dir, year), key='draws', mode='w',
                format='table', data_columns=['location_id', 'year_id',
                'age_group_id', 'sex_id'])
