##########################################################################
# Description: We need to interpolate dismod values between years to get a full
# time series. So we call teh central function
# Input: log_dir: directory for where you want log files to be saved
# jobname: to be used to name the current logging file
# me_id: modelable_entity_id of the model being interpolated
# out_dir: directory where you want the interpolated files to be saved
# start_year_str: first year of interpolation data
# end_year_str: last year of interpolation data
# Output: A .h5 saved to the location specified, with the interpolated
# datasets for the years and countries specified.
##########################################################################

from __future__ import division
import sys

from PyJobTools import rlog
from transmogrifier.draw_ops import interpolate
import maternal_fns

log_dir, jobname, me_id, out_dir, start_year_str, end_year_str = sys.argv[
    1:7]

start_year = int(start_year_str)
end_year = int(end_year_str)

# logging
rlog.open('%s/%s.log' % (log_dir, jobname))
rlog.log('out_dir is %s' % out_dir)

# get list of locations
locations = maternal_fns.get_locations()

if start_year == 1990:
    yearlist = range(1980, 1990) + range(1991, 1995)
    start_year = 1980
else:
    yearlist = range(start_year + 1, end_year)

# call central function to interpolate
rlog.log("Calling interpolate")
interp_df = interpolate(gbd_id_field='modelable_entity_id',
                        gbd_id=int(me_id),
                        source='dismod',
                        reporting_year_start=start_year,
                        reporting_year_end=end_year,
                        age_group_ids=range(7, 16),
                        sex_ids=2)

for year in yearlist:
    rlog.log('saving interpolated draws for year %s' % year)
    interp_df.query("year_id==%s" % year
                    ).to_hdf('FILEPATH.h5' % (out_dir, year), key='draws',
                             mode='w', format='table',
                             data_columns=['location_id', 'year_id',
                                           'age_group_id', 'sex_id'])
rlog.log('Finished!')
