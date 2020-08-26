
##########################################################################

##########################################################################

import sys
import logging
import maternal_fns

from get_draws.api import get_draws

##############################################
# PREP WORK:
# set directories and other preliminary data
##############################################

year, env_id, late_id, out_dir = sys.argv[1:5]

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
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("maternal_custom.04_timings")

##############################################
# GET LATE CAUSE FRACTIONS:
##############################################

logger.info("Starting to get late cause fractions.")
codcorrect_df = get_draws(gbd_id=[env_id, late_id], gbd_id_type=['cause_id', 'cause_id'],
	                      source='codcorrect', year_id=[year], sex_id=[2],
	                      measure_id=[1])
codcorrect_df['measure_id'] = 1
codcorrect_df = codcorrect_df[codcorrect_df.age_group_id.isin(range(7, 16))]

envelope_df = codcorrect_df[codcorrect_df.cause_id == env_id]
late_df = codcorrect_df[codcorrect_df.cause_id == late_id]

# we only want index_cols and draws as columns
envelope_df = envelope_df[columns].set_index(index_cols).sortlevel()
late_df = late_df[columns].set_index(index_cols).sortlevel()

# calculate late cause fractions
logger.info('Calculating late cfs for year %s' % year)
late_cfs = late_df / envelope_df

late_cfs.reset_index(inplace=True)
late_cfs['modelable_entity_id'] = late_id
late_cfs['measure_id'] = 18

# save late cause fractions
late_cfs.to_hdf('%s/%s_2.h5' % (out_dir, year), key='draws', mode='w',
                format='table', data_columns=['location_id', 'year_id',
                'age_group_id', 'sex_id'])
