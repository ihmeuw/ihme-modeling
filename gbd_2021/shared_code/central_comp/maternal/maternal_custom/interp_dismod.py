import sys
import logging

import numpy as np

import maternal_fns
from chronos.interpolate import interpolate
from gbd import decomp_step as decomp
from gbd import estimation_years


# create logger
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("maternal_custom.interp_dismod")

# pull arguments from main script
me_id, decomp_step_id, out_dir = sys.argv[1:4]

me_id = int(me_id)
decomp_step_id = int(decomp_step_id)
gbd_round_id = decomp.gbd_round_id_from_decomp_step_id(decomp_step_id)

# get list of locations
locations = maternal_fns.get_locations(decomp_step_id)

# get years
yearlist = maternal_fns.get_all_years(gbd_round_id)
start_year = yearlist[0]
end_year = yearlist[-1]

# call central function to interpolate
logger.info("Calling interpolate the first time.")
interpolate(
    gbd_id_type='modelable_entity_id',
    gbd_id=2519,
    source='epi',
    reporting_year_start=start_year,
    reporting_year_end=end_year,
    measure_id=18,
    age_group_id=7,
    location_id=101,
    sex_id=2,
    num_workers=45,
    decomp_step=decomp.decomp_step_from_decomp_step_id(decomp_step_id),
    gbd_round_id=decomp.gbd_round_id_from_decomp_step_id(decomp_step_id))
logger.info("Calling interpolate a second time.")
interp_df = interpolate(
    gbd_id_type='modelable_entity_id',
    gbd_id=me_id,
    source='epi',
    reporting_year_start=start_year,
    reporting_year_end=end_year,
    measure_id=18,
    age_group_id=maternal_fns.MATERNAL_AGE_GROUP_IDS,
    sex_id=2,
    num_workers=45,
    decomp_step=decomp.decomp_step_from_decomp_step_id(decomp_step_id),
    gbd_round_id=decomp.gbd_round_id_from_decomp_step_id(decomp_step_id))

draw_cols = ["draw_{}".format(x) for x in range(0, 1000)]
data_cols = ['measure_id', 'location_id', 'year_id', 'age_group_id', 'sex_id']
interp_df = interp_df[data_cols + draw_cols]

for col in data_cols:
    interp_df[col] = interp_df[col].astype(np.int64)

# save each of the files
for year in yearlist:
    logger.info('saving interpolated draws for year %s' % year)
    interp_df.query("year_id==%s" % year).to_hdf(
        '%s/%s_2.h5' % (out_dir, year), key='draws',
        mode='w', format='table',
        data_columns=['location_id', 'year_id', 'age_group_id', 'sex_id'])

logger.info('Finished with Interpolation')
