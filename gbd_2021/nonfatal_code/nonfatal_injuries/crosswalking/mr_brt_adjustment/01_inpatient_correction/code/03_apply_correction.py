# team: GBD Injuries
# project: crosswalking GBD 2020
# script: adjust outpatient data

# load packages
import pandas as pd
pd.set_option('display.max_columns', 500)
import numpy as np

# import raw WHS data extractions
dat = pd.read_csv('FILEPATH')

# import smoothed proportions
props = pd.read_csv('FILEPATH')

props.rename(columns={'year_id':'year_start'}, inplace=True)
props['age_start'] = props['age_start'].astype('int')
props['age_end'] = props['age_end'].astype('int')

# multiply outpatient rates by the corresponding proporiton
outpatient = dat[dat['cv_outpatient'] == 1]
outpatient = pd.merge(outpatient, props, on=['sex_id', 'year_start', 'age_start', 'age_end'])
outpatient['correction'] = outpatient['mean'] * outpatient['pred']
corrections = outpatient[['ihme_loc_id', 'year_start', 'age_start', 'age_end', 'sex_id', 'correction']]

# merge these calculated adjustments back onto original dataframe
dat = pd.merge(dat, corrections, on=['ihme_loc_id', 'year_start', 'age_start', 'age_end', 'sex_id'])

# subtract adjustment from outpatient mean
dat['mean'] = np.where(dat['cv_outpatient'] == 1, dat['mean'] - dat['correction'], dat['mean'])
# add adjustment to inpatient mean
dat['mean'] = np.where(dat['cv_inpatient'] == 1, dat['mean'] + dat['correction'], dat['mean'])

dat['cases'] = np.where(dat['cv_outpatient'] == 1, dat['sample_size'] * dat['mean'], dat['cases'])
dat['cases'] = np.where(dat['cv_inpatient'] == 1, dat['sample_size'] * dat['mean'], dat['cases'])

# recalculate standard errors for new means
dat['standard_error'] = np.where((dat['cv_outpatient'] == 1) & (dat['cases'] < 5), ((5-dat['mean']*dat['sample_size'])/dat['sample_size']+dat['mean']*dat['sample_size']*np.sqrt(5/dat['sample_size']**2))/5, dat['standard_error'])
dat['standard_error'] = np.where((dat['cv_outpatient'] == 1) & (dat['cases'] >= 5), np.sqrt(dat['mean']/dat['sample_size']), dat['standard_error'])

dat['standard_error'] = np.where((dat['cv_inpatient'] == 1) & (dat['cases'] < 5), ((5-dat['mean']*dat['sample_size'])/dat['sample_size']+dat['mean']*dat['sample_size']*np.sqrt(5/dat['sample_size']**2))/5, dat['standard_error'])
dat['standard_error'] = np.where((dat['cv_inpatient'] == 1) & (dat['cases'] >= 5), np.sqrt(dat['mean']/dat['sample_size']), dat['standard_error'])

dat.to_csv('FILEPATH', index=False)
