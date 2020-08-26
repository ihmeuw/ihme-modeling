
"""
Created on Tue Jan 17 12:07:25 2017

@author: USER & USER

formatting UK UTLA data
TODO need to incorporate Injuries data, which are stored separately in different files
"""
import pandas as pd
import numpy as np
import platform
import sys
import statsmodels.formula.api as smf
import statsmodels.api as sm
import time

sys.path.append(r"FILEPATH")
from hosp_prep import *


if platform.system() == "Linux":
    root = "FILENAME"
else:
    root = "FILEPATH"

print("need to incorporate injuries data which are stored in separate files")






both = pd.read_csv(root + r"FILENAME"
                   r"FILEPATH", compression='gzip')













both = both[~both.log_rate.isnull()]






causes = both.cause_code.unique()

both['preds'] = np.nan  

start = time.time()
counter = 0
counter_denom = causes.size
for cause in causes:
    for s in [1, 2]:
        
        mask = (both['cause_code'] == cause) & (both['sex_id'] == s)
        if both[mask].log_rate.isnull().sum() == both[mask].shape[0]:
            print("there's no data")
            continue
        
        formula = "log_rate ~ C(age_start) + C(location_id)"
        
        fit = smf.ols(formula, data=both[mask]).fit()
        
        both.loc[mask, 'preds'] = np.exp(fit.predict(both[mask]))
        if s == 1:
            counter += 1
            if counter % 125 == 0:
                print(round((counter / counter_denom) * 100, 1), "% Done")
                print("Run time: ", (time.time()-start)/60, " minutes")

print("Done in ", (time.time()-start) / 60, " minutes")










reg_groups = ['cause_code', 'location_parent_id', 'age_start', 'age_end',
              'sex_id', 'fiscal_year']



both['value'].fillna(value=0, inplace=True)


both['utla_val_to_reg'] = both.groupby(reg_groups)['value'].transform('sum')



pred_df = both[both.utla_log_rate.isnull()].copy()

both = both[both.utla_log_rate.notnull()]



pred_df['reg_resid_value'] = pred_df['reg_value'] - pred_df['utla_val_to_reg']



pred_df['pred_counts'] = pred_df['preds'] * pred_df['utla_population']


pred_df['utla_pred_to_reg'] = pred_df.groupby(reg_groups)['pred_counts'].\
        transform('sum')


pred_df['weight'] = pred_df['reg_resid_value'] / pred_df['utla_pred_to_reg']


pred_df['weighted_counts'] = pred_df['pred_counts'] * pred_df['weight']



reg_compare = pred_df.copy()

reg_compare = reg_compare[['cause_code', 'location_parent_id', 'age_start',
                           'age_end', 'sex_id', 'fiscal_year',
                           'reg_resid_value']]
reg_compare.drop_duplicates(inplace=True)
reg_sum = reg_compare.reg_resid_value.sum()

pred_df_sum = pred_df.weighted_counts.sum()

assert round(reg_sum - pred_df_sum, 5) == 0


assert (pred_df.reg_value >= pred_df.reg_resid_value).all()


both = pd.concat([both, pred_df])



condition = (both['value'] == 0) & (both['weighted_counts'].notnull())
both.loc[condition, 'value'] = both.loc[condition, 'weighted_counts']


both['rates'] = both['value'] / both['utla_population']

both[['location_id', 'location_parent_id', 'age_start', 'age_end', 'sex_id',
      'fiscal_year', 'cause_code', 'utla_log_rate', 'value', 'preds',
      'reg_value', 'reg_resid_value',
      'weight', 'rates', 'utla_population']].\
      to_csv(root + r"FILENAME"
             r"FILEPATH", index=False)


both[['location_id', 'location_parent_id', 'age_start', 'age_end', 'sex_id',
      'fiscal_year', 'cause_code', 'utla_log_rate', 'value', 'preds',
      'reg_value', 'reg_resid_value', 'weight']].\
      to_csv(root + r"FILENAME"
             r"FILEPATH",
             index=False)
