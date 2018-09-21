# -*- coding: utf-8 -*-
"""
formatting UK UTLA data
"""
import pandas as pd
import numpy as np
import platform
import sys
import statsmodels.formula.api as smf
import statsmodels.api as sm
import time

sys.path.append("FILEPATH")
from hosp_prep import *

# Environment:
if platform.system() == "Linux":
    root = "FILEPATH"
else:
    root = "FILEPATH"

print("need to incorporate injuries data which are stored in separate files")
################################################
# Use data prepped on the cluster
###############################################

# was too big to merge locally so merged on the cluster and written to FILEPATH
# just read in the merged data from drive
both = pd.read_csv("FILEPATH", compression='gzip')
# both = pd.read_csv("FILEPATH", compression='gzip')

#both = pd.read_csv("FILEPATH", compression='gzip')
# back = both.copy()

# the regional level data needs to be split to include age start 90
# it's breaking the models so I'm gonna subset that age group out
# both = both[both.age_start < 80]
# also drop 2011, 2012
# both = both[both.fiscal_year < 2011]

# drop the rows that don't match (only 2 rows before 2011)
both = both[~both.log_rate.isnull()]

##################################
# FIT THE LINEAR MODELS
###################################

causes = both.cause_code.unique()
# both = both[both.cause_code.isin(causes)]
both['preds'] = np.nan  # initialize pred col
# loop over causes and sexes
start = time.time()
counter = 0
counter_denom = causes.size
for cause in causes:
    for s in [1, 2]:
        # create the mask
        mask = (both['cause_code'] == cause) & (both['sex_id'] == s)
        if both[mask].log_rate.isnull().sum() == both[mask].shape[0]:
            print("there's no data")
            continue
        # our formula for predictions
        formula = "log_rate ~ C(age_start) + C(location_id)"
        # fit the model
        fit = smf.ols(formula, data=both[mask]).fit()
        # exponentiate the predicted values
        both.loc[mask, 'preds'] = np.exp(fit.predict(both[mask]))
        if s == 1:
            counter += 1
            if counter % 125 == 0:
                print(round((counter / counter_denom) * 100, 1), "% Done")
                print("Run time: ", (time.time()-start)/60, " minutes")

print("Done in ", (time.time()-start) / 60, " minutes")


# both.to_csv("FILEPATH")

###################################################

# both = back.copy()
# subtract off the existing cases that we have at utla level
# use a groupby transform to leave the data in same format but create sums of
# known values at the regional level
reg_groups = ['cause_code', 'location_parent_id', 'age_start', 'age_end',
              'sex_id', 'fiscal_year']

# fill missing utla level data with zeroes instead of NA so rows will be
# included in groupby
both['value'].fillna(value=0, inplace=True)

# sum the existing utla values up to the regional level
both['utla_val_to_reg'] = both.groupby(reg_groups)['value'].transform('sum')

# split the data
# subset the data to get only rows where utla value was suppressed
pred_df = both[both.utla_log_rate.isnull()].copy()
# drop the rows where utla value was suppressed
both = both[both.utla_log_rate.notnull()]

# subtract the known utla values from the regional values to get
# residual (unknown) values
pred_df['reg_resid_value'] = pred_df['reg_value'] - pred_df['utla_val_to_reg']

# new method
# get into count space
pred_df['pred_counts'] = pred_df['preds'] * pred_df['utla_population']

# sum utla predicted counts to region level
pred_df['utla_pred_to_reg'] = pred_df.groupby(reg_groups)['pred_counts'].\
        transform('sum')

# make the weights
pred_df['weight'] = pred_df['reg_resid_value'] / pred_df['utla_pred_to_reg']

# apply weights to predicted values
pred_df['weighted_counts'] = pred_df['pred_counts'] * pred_df['weight']


# now test
reg_compare = pred_df.copy()
# get the sum of values at the regional level
reg_compare = reg_compare[['cause_code', 'location_parent_id', 'age_start',
                           'age_end', 'sex_id', 'fiscal_year',
                           'reg_resid_value']]
reg_compare.drop_duplicates(inplace=True)
reg_sum = reg_compare.reg_resid_value.sum()
# get the sum of desuppressed values
pred_df_sum = pred_df.weighted_counts.sum()
# pretty dang close to zero
assert round(reg_sum - pred_df_sum, 5) == 0

# assert residual vals are smaller than regional vals
assert (pred_df.reg_value >= pred_df.reg_resid_value).all()

# concat de-suppressed and un-suppressed data back together
both = pd.concat([both, pred_df])

# merge data that needed to be de-suppressed and data that didn't into same col
# fill value with desuppressed val where value = 0 and desuppressed isn't null
condition = (both['value'] == 0) & (both['weighted_counts'].notnull())
both.loc[condition, 'value'] = both.loc[condition, 'weighted_counts']

# write to a csv for use with a Shiny app
both['rates'] = both['value'] / both['utla_population']

both[['location_id', 'location_parent_id', 'age_start', 'age_end', 'sex_id',
      'fiscal_year', 'cause_code', 'utla_log_rate', 'value', 'preds',
      'reg_value', 'reg_resid_value',
      'weight', 'rates', 'utla_population']].\
      to_csv("FILEPATH", index=False)

# write to FILEPATH intermediate data
both[['location_id', 'location_parent_id', 'age_start', 'age_end', 'sex_id',
      'fiscal_year', 'cause_code', 'utla_log_rate', 'value', 'preds',
      'reg_value', 'reg_resid_value', 'weight']].\
      to_csv("FILEPATH", index=False)
