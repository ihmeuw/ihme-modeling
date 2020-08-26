###################################################################################

###################################################################################

import sys
import logging

import numpy as np

import maternal_fns
from chronos.interpolate import interpolate
from gbd import decomp_step as decomp


# create logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("maternal_custom.interp_dismod")

# pull arguments from master script
me_id, decomp_step_id, out_dir = sys.argv[1:4]

me_id = int(me_id)
decomp_step_id = int(decomp_step_id)

# get list of locations
locations = maternal_fns.get_locations()

start_year = 1980
end_year = 2019
yearlist = list(range(1980, end_year + 1))

# call central function to interpolate
logger.info("Calling interpolate the first time.")
interpolate(
    gbd_id_type='modelable_entity_id',
    gbd_id=2519,
    source='epi',
    reporting_year_start=1980,
    reporting_year_end=2019,
    measure_id=18,
    age_group_id=7,
    location_id=101,
    sex_id=2,
    num_workers=45,
    decomp_step=decomp.decomp_step_from_decomp_step_id(decomp_step_id),
    gbd_round_id=maternal_fns.GBD_ROUND_ID)
logger.info("Calling interpolate a second time.")
interp_df = interpolate(
    gbd_id_type='modelable_entity_id',
    gbd_id=me_id,
    source='epi',
    reporting_year_start=start_year,
    reporting_year_end=end_year,
    measure_id=18,
    age_group_id=list(range(7, 16)),
    sex_id=2,
    num_workers=45,
    decomp_step=decomp.decomp_step_from_decomp_step_id(decomp_step_id),
    gbd_round_id=maternal_fns.GBD_ROUND_ID)

draw_cols = ["draw_{}".format(x) for x in range(0, 1000)]
data_cols = ['measure_id', 'location_id', 'year_id', 'age_group_id', 'sex_id']
interp_df = interp_df[data_cols + draw_cols]

for col in data_cols:
    interp_df[col] = interp_df[col].astype(np.int64)

# save each of the files
for year in yearlist:
    logger.info('saving interpolated draws for year %s' % year)
    interp_df.query("year_id==%s" % year).to_hdf('%s/%s_2.h5' % (out_dir, year), key='draws',
                                                 mode='w', format='table',
                                                 data_columns=['location_id', 'year_id', 'age_group_id', 'sex_id'])

logger.info('Finished with Interpolation')
