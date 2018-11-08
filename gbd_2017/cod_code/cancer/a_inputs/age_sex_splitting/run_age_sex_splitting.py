# -*- coding: utf-8 -*-
'''
Description: Splits combined age or sex groups in registry data
Inputs: 03_mapped file
Output: .dta file with split ages
How To Use: Pass dataset_id and data_type_id to main()
'''



import sys
import pandas as pd
import numpy as np
import re
from utils import (
    common_utils as utils,
    data_format_tools as dft,
    modeled_locations
)
import age_sex_tests as at
import age_sex_core as core
from registry_pipeline.df_manipulate import add_year_id
from a_inputs.a_mortality_incidence import (
    mi_dataset as md,
    pipeline_tests as pt,
    prep_proportions as pp
)



def manage_no_split(df, metric_name, uid_cols, this_dataset):
    ''' finalize and save dataset where no split is needed
    '''
    uids_noAge = [c for c in uid_cols if 'age' not in c]
    # collapse remaining under5 and 80+ ages
    df = md.stdz_col_formats(df)
    df[metric_name].fillna(value=0, inplace=True)
    final_df = df.copy(deep=True)
    # final_df =  md.collapse_youngAndOld_ages(df, uids_noAge, metric_name)
    #
    if metric_name == "pop":
        md.complete_prep_step(final_df, this_dataset, is_pop=True)
    else:
        md.complete_prep_step(final_df, this_dataset, is_pop=False)
    return(None)


def manage_split(df, metric_name, uid_cols, this_dataset):
    ''' Converts age and sex categories in the df to those used by the cancer prep process.
        1) Adds obs number
        2) Splits aggregated ages
        3) Combines disaggregated ages
        4) Splits unknown age category
        5) Splits aggregated/unknown sex category
    '''
    is_pop = bool(metric_name == "pop")
    #
    df[metric_name].fillna(value=0, inplace=True)
    split_df = df.copy()
    # add observation number by group, without age
    uids_noAge = [c for c in uid_cols if 'age' not in c]
    obs_numbers = split_df[uids_noAge].drop_duplicates()
    obs_numbers['obs'] = obs_numbers.reset_index().index
    split_df = split_df.merge(obs_numbers)
    uid_cols.append('obs')
    # generate cause_weights
    if is_pop:
        cause_wgts = pp.gen_pop_wgts("age_wgt", df['location_id'].unique())
    else:
        # create weights used for splitting
        cause_wgts = pp.create_metric_weights(split_df, uid_cols, this_dataset)
        # collapse to get one weight per observation
        cause_wgts = dft.collapse(cause_wgts, by_cols=['obs', 'age', 'sex'],
                                  func='sum', combine_cols='wgt')
    # split
    if pt.has_nonStdAge(split_df):
        print("      splitting non-standard age...")
        split_df = core.split_age(dataset=split_df,
                                  wgt_df=cause_wgts,
                                  metric=metric_name,
                                  uid_cols=uid_cols)
    # redistribute "unknown age" data according to the current distribution of 
    #   cases/deaths
    if pt.has_age_unk(split_df, metric_name):
        print("      splitting unknown age...")
        # create weights
        split_df = core.split_unknown_age(dataset=split_df,
                                          wgt_df=cause_wgts,
                                          metric=metric_name,
                                          uid_cols=uid_cols)
    # check for errors. 
    at.compare_pre_post_split(split_df, df, metric_name)
    # split sex = 3 and sex = 9 data
    if pt.has_combinedSex(split_df):
        print("      splitting sex...")
        if metric_name == "pop":
            sex_split_prop = pp.gen_pop_wgts(
                "sex_wgt", df['location_id'].unique())
        else:
            sex_split_prop = pp.create_sex_weights(cause_wgts,
                                                   uid_vars=['obs', 'age'],
                                                   metric=metric_name)
        split_df = core.split_sex(split_df,
                                  sex_split_prop,
                                  uid_cols,
                                  metric=metric_name)
    # collapse remaining underu5 and 80+ ages
    final_df = split_df.copy(deep=True)
    final_df = final_df.loc[~final_df['age'].isin([26]), :]
    # collapse to incorperate newly-split data
    output_uids = md.get_uid_cols(5, is_pop)
    final_df = dft.collapse(final_df,
                            by_cols=output_uids,
                            func='sum',
                            combine_cols=metric_name
                            )
    # save and exit
    md.complete_prep_step(final_df, this_dataset, is_pop)
    return(None)


def run_split_pop(this_dataset):
    ''' split population data in combined age groups
    '''
    print("   splitting population...")
    uid_cols = md.get_uid_cols(prep_type_id=4, is_pop=True)
    df = this_dataset.load_pop(prep_type_id=1)
    # update column names (some old 00_pop files still have registry_id column 
    #   instead of registry_index)
    if "registry_id" in df.columns:  
        df.rename(columns={'registry_id': 'registry_index'}, inplace=True)
    # Exit if no population present
    if len(df) == 0:
        return(None)
    # Temporarily reshape and updated dataframe until input is no longer in STATA
    uids_noAge = [c for c in uid_cols if 'age' not in c]
    df = dft.wide_to_long(df,
                          stubnames='pop',
                          i=uids_noAge,
                          j='age',
                          drop_others=True)
    df = df.loc[df.age != 1, :]  # drop 'all ages'
    pop_cols = [p for p in df.columns.values if "pop" in p]
    df.loc[:, pop_cols].fillna(value=0, inplace=True)
    df = md.stdz_col_formats(df)
    if not (pt.has_combinedSex(df) |
            pt.has_age_unk(df, "pop") |
            pt.has_nonStdAge(df)
            ):
        manage_no_split(df, "pop", uid_cols, this_dataset)
    else:
        # replace missing data with zeroes
        uid_cols += ['location_id', 'country_id', 'year']
        df = md.add_location_ids(df)
        df = modeled_locations.add_country_id(df)
        # data with no country_id have no population estimates.
        #   global estimate should be used to generate weights
        df.loc[df['country_id'] == 0, 'location_id'] = 1
        # add mean year to faciliate merge with population weights
        df = add_year_id(df)
        # Split data
        manage_split(df, "pop", uid_cols, this_dataset)
    return(None)


def run_split_metric(this_dataset):
    ''' split metric data in combined age groups
    '''
    print("   splitting metric data...")
    uid_cols = md.get_uid_cols(4)
    metric_name = this_dataset.metric
    input_data = this_dataset.load_input()
    # save if no splitting is needed
    if not (pt.has_combinedSex(input_data) |
            pt.has_age_unk(input_data, metric_name) |
            pt.has_nonStdAge(input_data)):
        manage_no_split(input_data, metric_name, uid_cols, this_dataset)
    else:
        manage_split(input_data, metric_name, uid_cols, this_dataset)
    return(None)


def main(dataset_id, data_type_id):
    '''
    '''
    # load input dataset
    this_dataset = md.MI_Dataset(dataset_id, 4, data_type_id)
    # Split population
    run_split_pop(this_dataset)
    run_split_metric(this_dataset)
    print("Age and Sex are Split.")



if __name__ == "__main__":
    dataset_id = int(sys.argv[1])
    data_type_id = int(sys.argv[2])
    print("Splitting grouped age and sex with the following arguments: {} {}...".format(
        dataset_id, data_type_id))
    main(dataset_id, data_type_id)
