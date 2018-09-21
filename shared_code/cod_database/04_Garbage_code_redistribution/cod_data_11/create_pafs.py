
##########################################################################
# CODEM outputs maternal deaths values that estimate the number of
#              pregnant women, both HIV+ and HIV-, who die. 
#				Estimate for the proportion of those deaths
#              in which the mother is HIV+ (prop_hiv+) and the proportion of
#              HIV+ maternal deaths in which the cause of death should be
#              considered 'maternal causes', rather than 'HIV' (prop_maternal).
#              Using these values, we derive estimates for the number of deaths
#              of pregnant women, both HIV+ and HIV-, whose deaths should be
#              considered 'maternal'.
# Input: HIV prevalence in pregnancy and relative risk draws 
# Output: A .csv saved to the location specified, with HIV PAFS used in 01_HIV
##########################################################################

import pandas as pd
import sys
import os
sys.path.append(str(os.getcwd()).replace('/cod_hiv_correction', ""))
import maternal_fns

year = sys.argv[1]
year = int(year)

current_date = maternal_fns.get_time()
out_dir = '/ihme/centralcomp/maternal_mortality/pafs/%s' % current_date
if not os.path.exists(out_dir):
    os.makedirs(out_dir)
dismod_dir = '/ihme/epi/panda_cascade/prod'
diag_out_dir = '%s/diagnostics' % out_dir
if not os.path.exists(diag_out_dir):
    os.makedirs(diag_out_dir)

############
# Prep Work
############
locations = maternal_fns.get_locations()
columns = maternal_fns.filter_cols()

draw_cols = columns.remove('age_group_id')
columns = maternal_fns.filter_cols()
rr_cols = []
for i in range(0, 1000):
    rr_cols.append('rr_%d' % i)

all_diag = pd.DataFrame()

############
# Load data
############
# load relative risks
rr = pd.read_stata('/home/j/WORK/04_epi/01_database/02_data/maternal/archive_'
                   '2013/04_models/gbd2013/04_outputs/RR_draws_2014_03_18.dta')
rr.drop('dummy', axis=1, inplace=True)
rr = rr.reindex(range(0, 9))
# propagate the relative risks down to all ages of interest
rr['age_group_id'] = range(7, 16)
rr.fillna(method='ffill', axis=0, inplace=True)
rr.set_index('age_group_id', inplace=True)
cols = rr.columns
# rename all draw columns to be draw_0 to draw_999
for idx, i in enumerate(cols):
    rr.rename(columns={'%s' % i: 'draw_%s' % idx}, inplace=True)

# prep hiv prev for loading
model_vers = maternal_fns.get_model_vers('dismod', 9313)

all_pafs = pd.DataFrame()
for geo in locations:
    # load hiv prev
    fname = '%s_%s_2' % (geo, year)
    hiv_prev = pd.read_hdf('%s/%s/full/draws/%s.h5' %
                           (dismod_dir, model_vers, fname), 'draws',
                           where=["'location_id'== %d & 'year_id'== %d"
                                  % (geo, year)])
    hiv_prev = hiv_prev.query(
        "location_id == %s & year_id == %s" % (geo, year))
    hiv_prev = hiv_prev[columns]
    # we only want maternal age groups for hiv_prev
    hiv_prev = hiv_prev[hiv_prev.age_group_id.isin(range(7, 16))]

    # ADD CODE TO COPY HIV PREVALENCE FROM AGE 8 TO 7, 14 TO 15
    assert True==False, 'BOMB! SEE ABOVE COMMENT AND FIX BEFORE RUNNING AGAIN'
    hiv_prev.set_index('age_group_id', inplace=True)

    # replace all nulls with zeroes
    hiv_prev.fillna(0, inplace=True)

    # assert that prev is always bounded between 0 and 1
    if (hiv_prev.values < 0).any():
        raise ValueError(
            'HIV Prev values are below 0 for geo %s and year %s' % (geo, year))
    elif (hiv_prev.values > 1).any():
        raise ValueError(
            'HIV Prev values are above 1 for geo %s and year %s' % (geo, year))
    else:
        pass

    ############
    # Math
    ############
    # multiply hiv prev for every loc/year/age by every rr draw using this:
    # paf_draw = (hiv_prev * (RR_draw - 1))/((hiv_prev * (RR_draw - 1))+1)
    pafs = (hiv_prev * (rr - 1)) / ((hiv_prev * (rr - 1)) + 1)

    # assert that all pafs are bounded between 0 and 1
    if (pafs.values < 0).any():
        raise ValueError(
            'PAF values are below 0 for geo %s and year %s' % (geo, year))
    elif (pafs.values > 1).any():
        raise ValueError(
            'PAF values are above 1 for geo %s and year %s' % (geo, year))
    else:
        pass
    pafs['location_id'] = geo
    pafs['year'] = year
    all_pafs = all_pafs.append(pafs)

    ############
    # Diagnostics
    ############
    # create paf_mean and find countries where paf is high to output seperately
    diagnostics = pafs.copy()
    diagnostics['paf_mean'] = diagnostics.mean(axis=1).to_frame()
    if diagnostics.paf_mean.any() >= .01:
        diagnostics['add'] = 0
    else:
        diagnostics['add'] = 1
    diagnostics['location_id'] = geo

    diagnostics.reset_index(inplace=True)
    all_diag = all_diag.append(diagnostics)

############
# Save
############
# output pafs as hdf
all_pafs.fillna(0, inplace=True)
all_pafs.reset_index(inplace=True)
all_pafs.convert_objects()
all_pafs.to_hdf('%s/pafs_%s.h5' % (out_dir, year), 'pafs', mode='w',
                format='table',
                data_columns=['location_id', 'age_group_id', 'year'])

all_diag.to_csv('%s/diagnostics_%s.csv' % (diag_out_dir, year),
                index=False,
                encoding='utf-8')
