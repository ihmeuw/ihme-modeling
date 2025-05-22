from __future__ import division
from scipy.special import expit
import os
import numpy as np
import pandas as pd
import argparse
import random
import copy
import re
from job_utils import draws, parsers
from db_queries import get_covariate_estimates

def adjust_pms(self):
    '''Adjusts PMS (Pre-menstrual Syndrome) cases for pregnancy prevalence and incidence '''
    pms_key = self.me_map["pms"]["srcs"]["tot"]
    adj_key = self.me_map["pms"]["trgs"]["adj"]

    covariate_index_dimensions = ['age_group_id', 'location_id', 'sex_id', 'year_id']
    
    pms_df = self.me_dict[pms_key]
    asfr_df = get_covariate_estimates(13)
    sbr_df = get_covariate_estimates(2267)

    asfr_df = asfr_df.filter(covariate_index_dimensions + ['mean_value'])
    # Assumes the still birth rates covariate is reported for all_ages 
    # and both sexes. Hence we ignore that and merge only on loc and year
    sbr_df = sbr_df.filter(['location_id', 'year_id', 'mean_value'])

    asfr_df.rename(columns={'mean_value':'asfr_mean'}, inplace=True)
    sbr_df.rename(columns={'mean_value':'sbr_mean'}, inplace=True)

    prop_df = asfr_df.merge(sbr_df, how='inner', 
        on=['location_id', 'year_id'])
    prop_df[prop_df.sex_id == 1].sbr_mean = 0

    prop_df['prop'] = (prop_df.asfr_mean + 
        (prop_df.asfr_mean * prop_df.sbr_mean)) * 46/52
    prop_df.set_index(covariate_index_dimensions, inplace=True)

    adj_df = pms_df.copy()
    adj_df.reset_index(level='measure_id', inplace=True)
    adj_df = adj_df.merge(prop_df.prop, how='inner', left_index=True, right_index=True)

    for col in self.draw_cols:
        adj_df[col] = adj_df[col] * (1 - adj_df.prop)
    
    adj_df.drop(columns='prop', inplace=True)

    self.me_dict[adj_key] = adj_df