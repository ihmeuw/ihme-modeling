# load packages
import argparse
import os
import pandas as pd
import numpy as np
import db_queries as db

pd.set_option('display.max_columns', 500)

# import WHS data with inpatient correction
df = pd.read_csv('FILEPATH')

# aggregate data across years (2002, 2003, and 2004) and sexes (males and females)
agg = df.groupby(
    ['ihme_loc_id', 'nid', 'age_start', 'age_end', 'cv_inpatient', 'cv_recv_care', 'cv_no_care', 'cv_outpatient'])[
    ['sample_size', 'cases']].sum().reset_index()
# generate ID that corresponds to study-level covariates
agg['cov_id'] = agg['cv_inpatient'].astype('str') + agg['cv_recv_care'].astype('str') + agg['cv_no_care'].astype(
    'str') + agg['cv_outpatient'].astype('str')
# only keep "injuries warranting care" (all covs are 0) and "injuries that received care" (inj_recv_care = 1)
agg = agg[agg['cov_id'].isin(['0000', '0100'])]

# get mean incidence rate
agg['mean'] = agg['cases'] / agg['sample_size']

df2 = agg[['nid', 'ihme_loc_id', 'age_start', 'age_end', 'sample_size', 'cases', 'mean', 'cov_id']]
df2.set_index(['nid', 'ihme_loc_id', 'age_start', 'age_end'], inplace=True)

# send dataframe wide
df3 = pd.pivot_table(df2,
                     columns='cov_id',
                     values=['mean', 'cases', 'sample_size'],
                     index=[df2.index.values],
                     aggfunc='first')

# calculate proportion of those who received care for their injury
df3['proportion'] = df3['mean']['0100'] / df3['mean']['0000']

df3['sample_size_both'] = df3['sample_size']['0000']
df3.reset_index(inplace=True)

df4 = df3[['index', 'proportion', 'sample_size_both']]
df4.columns = ['demo', 'data', 'sample_size']

df4[['nid', 'ihme_loc_id', 'age_start', 'age_end']] = pd.DataFrame(df4['demo'].tolist(), index=df4.index)

locs = db.get_location_metadata(location_set_id=35)
df4 = df4.merge(locs[['ihme_loc_id', 'location_id']])

df4.drop(['demo', 'ihme_loc_id'], axis=1, inplace=True)

# prep additional columns for ST-GPR
df4['measure'] = 'proportion'
df4['is_outlier'] = 0
df4['variance'] = ''
df4['sex_id'] = 3
df4['year_id'] = 2003

# get rid of any implausible proportions
df5 = df4[df4['data'] <= 1]

# apply offset in order to model in logit space
df5['data'] = np.where(df5['data'] == 1, df5['data'] - 0.01, df5['data'])
df5['data'] = np.where(df5['data'] == 0, df5['data'] + 0.01, df5['data'])

# using variance formula for proportions, assuming binomial distribution
df5['variance'] = (df5['data'] * (1 - df5['data'])) / df5['sample_size']

# add age group id information
ages = db.get_age_metadata(age_group_set_id=12)
ages.rename(columns={'age_group_years_start': 'age_start', 'age_group_years_end': 'age_end'}, inplace=True)
ages['age_start'] = ages['age_start'].astype('int')
ages['age_end'] = ages['age_end'].astype('int') - 1
df5 = df5.merge(ages[['age_start', 'age_end', 'age_group_id']])

df5.to_csv('FILEPATH', index=False)
