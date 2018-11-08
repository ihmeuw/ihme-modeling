# -*- coding: utf-8 -*-
''' 
Description: Contains tests for cancer-pipeline processes involving custom 
    age-sex weights 
How to Use: import by age_sex_core and run_age_sex_splitting
'''



import numpy as np
import pandas as pd



def test_weights(pre_merge_df, post_merge_df):
    ''' verifies that data are not lost when merging with weights.
        for use just before merging with weights
    '''
    assert len(pre_merge_df) <= len(post_merge_df), \
        "Error: merge with age_sex weights or rates will drop some data"


def test_split_age(input_df, output_df, metric):
    ''' verifies that no observations were lost after merge with age splitting 
            map
    '''
    assert all(c in output_df['obs'].unique() for c in input_df['obs'].unique()), \
        "Error: some observations lost during split"
    assert len(input_df) == len(output_df), \
        "Some entries did not merge with the map!"


def compare_pre_post_split(df, input_df, metric):
    ''' compares totals for each observation before and after split and 
        checks if they're equal
    '''
    pre_split_total = round(input_df[metric].sum())
    post_split_total = round(df[metric].sum())
    diff = post_split_total - pre_split_total
    assert abs(diff) <= 0.001 * pre_split_total, \
        "Totals before and after split do not align (difference = {})".format(diff)
    return(None)
