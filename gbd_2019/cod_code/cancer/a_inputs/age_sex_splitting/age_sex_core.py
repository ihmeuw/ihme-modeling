'''
Name of Module: worker.py
Description: Set of functions needed to run age_sex_split
Inputs:    N/A - to use: import subroutines.worker at the
                top of your file with all other import statements
Output: N/A
'''

# import libraries
import pandas as pd
import numpy as np
import re
from warnings import warn
# import cancer_estimation utilities and modules
from cancer_estimation.py_utils import (
    common_utils as utils,
    data_format_tools as dft,
    pandas_expansions as pe
)
import age_sex_tests as astest
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
    pipeline_tests as pt,
    prep_proportions as pp
)


def get_age_frmat_map(frmat_type):
    '''
    '''
    if frmat_type == "im_frmat_id":
        resource = pd.read_csv(utils.get_path(
            'im_frmat_map', process="mi_dataset"))
    elif frmat_type == "frmat_id":
        resource = pd.read_csv(utils.get_path(
            'frmat_map', process="mi_dataset"))
    resource = md.stdz_col_formats(
        resource, additional_float_stubs=['age_specific', 'age_split'])
    return(resource)


def add_missing_ages(df, uid_cols, metric):
    ''' Creates new rows for missing metric-age entries (with metric value set 
            to zero)
        Inputs:
            df  : DataFrame, pandas dataframe
    '''
    age_categories = list(range(2, 27)) + [91, 92, 93, 94]
    missing_ages = [a for a in age_categories if a not in df['age'].unique()]
    fill_missing = df.groupby(
        uid_cols + ['age'], as_index=False)[metric].count()
    fill_missing[metric] = 0
    for a in missing_ages:
        fill_missing['age'] = a
        df = df.append(fill_missing)
    return(df)


def split_age(dataset, wgt_df, metric, uid_cols, replacement_col='gbd_age'):
    ''' Splits aggregate ages
        --- Inputs ---
        dataset : DataFrame
                pandas dataframe
                must contain "obs" column indicating the observation number
        wgt_df  : DataFrame
                    pandas dataframe containing expected numbers for the metric at each age
        metric  : string
                possible values: ['pop', 'cases', 'deaths']
        uid_cols : list
                column names indicating unique identifiers for the wgt_df (eg. cause, location_id)
        replacement_col: string
                name of the column containing the new age values in the wgt_df
    '''
    assert 'wgt' in wgt_df.columns, "Error: no wgt column sent in wgt_df"

    # Ensure that 'age' is not listed as a uid, since it's being split
    uids_noAge = [u for u in uid_cols if 'age' not in u]
    # mark those entries that need to be split. do not split if frmat==9 (unknown)
    standard_age_frmats = [0]  
    standard_age_im_frmats = [1, 2, 8, 9]  
    # fill to a standard im_frmat if frmat == 9
    dataset.loc[:, 'im_frmat_id'].fillna(value=9, inplace=True)
    dataset = add_missing_ages(dataset, uids_noAge, metric)
    # for each format type, mark which age categories need to be split per the 
    #   corresponding format map. split only those categogies. can split 
    #   multiple age formats at once
    dataset['need_split'] = 0
    dataset.loc[~dataset['frmat_id'].isin(standard_age_frmats + [9])
                & ~dataset['age'].isin([26]), 'need_split'] = 1
    dataset.loc[~dataset['im_frmat_id'].isin(standard_age_im_frmats)
                & dataset['frmat_id'] != 9
                & ~dataset['age'].isin([26]), 'need_split'] = 1
    # Split age for each format type
    df = dataset.loc[dataset.need_split == 1, :].copy(deep=True)
    for frmat_type in ['frmat_id', 'im_frmat_id']:
        df = apply_age_spilt_proportions(
            df, frmat_type, wgt_df, uid_cols, metric)
    # rename age formats to original name
    unadjusted = dataset.loc[dataset.need_split == 0, :].copy(deep=True)
    output = unadjusted.append(df)
    del output['need_split']
    output.loc[:, 'im_frmat_id'] = 9
    output.loc[output['frmat_id'] != 9, 'frmat_id'] = 131
    pt.verify_metric_total(dataset, output, metric, "split age")
    return(output)


def apply_age_spilt_proportions(input_df, frmat_type, wgt_df, uid_cols, metric):
    '''
    '''
    # remove dataset_id if present in dataframe
    split_input = input_df.copy()
    if 'dataset_id' in split_input.columns:
        del split_input['dataset_id']
    # merge with the age format map and get an expanded dataframe with the ages 
    #   to be split
    uids_noAge = [u for u in uid_cols if 'age' != u]
    uid_cols = uids_noAge + ['age']
    marked_df = mark_ages_to_be_split(split_input, frmat_type, uid_cols, metric)
    to_expand = marked_df.loc[marked_df['to_expand'].eq(1), :].copy()
    if len(to_expand) == 0:
        return(split_input)
    # merge with expected values ("weights")
    to_expand.rename(columns={'age': 'split_age',
                              'gbd_age': 'age'}, inplace=True)
    to_expand = md.stdz_col_formats(to_expand)
    wgt_df = md.stdz_col_formats(wgt_df)
    weighted_df = pd.merge(to_expand, wgt_df, how='left',indicator=True)
    astest.test_weights(to_expand, weighted_df)
    # calculate proportions 
    to_split = pp.add_proportions(
        weighted_df, uids_noAge+['split_age'])
    # adjust by proportions
    to_split.loc[:, 'split_value'] = to_split[metric]
    to_split.loc[:, metric] = to_split['proportion'] * to_split['split_value']
    # collapse, then update format types of split data
    recombined_df = to_split.append(
        marked_df.loc[marked_df['to_expand'].eq(0), :].copy())
    adjusted_df = dft.collapse(recombined_df,
                               by_cols=uid_cols,
                               func='sum',
                               combine_cols=metric)
    astest.compare_pre_post_split(split_input, adjusted_df, metric)
    adjusted_df.loc[:, 'need_split'] = 1
    pt.verify_metric_total(split_input, adjusted_df,
                           metric, "apply age proportions")
    return(adjusted_df[split_input.columns.values])


def split_unknown_age(dataset, wgt_df, uid_cols, metric):
    ''' redistribute unkonwn ages
        --- Inputs ---
        dataset : DataFrame
                pandas dataframe
        metric  : string
                possible values: ['pop', 'cases', 'deaths']
    '''
    assert 'wgt' in wgt_df.columns, "Error: no wgt column sent in wgt_df"
    # Ensure that 'age' is not listed as a uid
    uids_noAge = [u for u in uid_cols if 'age' != u]
    uid_cols = uids_noAge + ['age']
    dataset = add_missing_ages(dataset, uids_noAge, metric)
    # Split unknown age
    unknown_age = dataset.loc[dataset['age']
                              == 26, uids_noAge + [metric]].copy()
    unknown_age.rename(columns={metric: 'unknown_age_data'}, inplace=True)
    known_age = dataset.loc[dataset['age'] != 26, :].copy()

    # standardize columns to enable merging
    wgt_df = md.stdz_col_formats(wgt_df) 
    wgt_df_uids = list(wgt_df.columns)
    wgt_df_uids = [u for u in wgt_df_uids if 'age' != u]
    wgt_df_uids = [u for u in wgt_df_uids if 'wgt' != u]
    wgt_df = add_missing_ages(wgt_df, wgt_df_uids, 'wgt')
    wgt_df = wgt_df.loc[~(wgt_df['wgt'].eq(0))] 
    known_age['dataset_id'] = 0 #temporary fill so stdz_col_formats() works
    known_age = md.stdz_col_formats(known_age)
    with_weights = known_age.merge(wgt_df, how='left')
    astest.test_weights(known_age, with_weights)
    prop_df = pp.add_proportions(with_weights, uids_noAge)
    # added for more standardized merge
    prop_df = md.stdz_col_formats(prop_df, additional_float_stubs = ['proportion', 'wgt', 'wgt_tot']) 
    unknown_age = md.stdz_col_formats(unknown_age)
    to_distribute = prop_df.merge(unknown_age, how = "left", indicator = True)
    to_distribute.loc[:, 'unknown_age_data'].fillna(value=0, inplace=True)
    to_distribute['orig_data'] = to_distribute[metric].copy()
    to_distribute['unknown_age_data'] = to_distribute['unknown_age_data'].astype(float) 
    to_distribute['proportion'] = to_distribute['proportion'].astype(float) 
    to_distribute.loc[:, 'proportion'].fillna(value=0, inplace=True)
    to_distribute.loc[:, metric] += to_distribute['unknown_age_data'].multiply(
        to_distribute['proportion'])
    output = to_distribute[uid_cols + [metric]]
    output.loc[:, 'frmat_id'] = 131

    pt.verify_metric_total(dataset, output, metric, "split unknown age")
    return(output)


def split_sex(dataset, sex_wgts, uid_cols, metric, replacement_col='new_sex'):
    ''' Splits sex, and scales data using weights
        --- Inputs ---
            dataset : DataFrame
                    pandas dataframe
            uid_cols:
                    list. must contain 'age'
            sex_wgts
            metric  : string
                    possible values: ['pop', 'deaths', 'cases']
            replacement_col: string
                    name of the column containing the new age values in the wgt_df
    '''
    sex_split_uids = ['age', 'obs', 'sex_id']
    assert all(c in uid_cols for c in sex_split_uids), \
        "Error: \'age\' must be listed as a uid column for split_sex"
    assert 'wgt' in sex_wgts.columns, "Error: no wgt column sent in sex_weights df"
    # Subset to non-redundant, sex-splittable  data
    to_split = pp.subset_to_sex_split_data(dataset, uid_cols)
    not_split = dataset.loc[dataset['sex_id'].isin([1, 2]), :]
    if len(to_split) == 0:
        return (not_split)
    # collapse to combine mulitple sex entries
    to_split.loc[to_split['sex_id']== 9, 'sex_id'] = 3
    # merge with weights
    with_weights = pd.merge(to_split, sex_wgts,
                            how='left', indicator=True)
    is_split = pp.add_proportions(with_weights, uid_cols)
    is_split[metric] = is_split[metric].multiply(is_split['proportion'])
    is_split.loc[:, 'sex_id'] = is_split['new_sex_id']

    # fix for gender specific ICCC3 for single causes
    if metric != "pop":
        is_split.loc[(is_split['acause1'] == "gender_specific") & (is_split['sex_id'] == 2), ['gbd_cause', 'acause1']] = "neo_ovarian"
        is_split.loc[(is_split['acause1'] == "gender_specific") & (is_split['sex_id'] == 1), ['gbd_cause', 'acause1']] = "neo_testicular"

    # recombine data
    output = not_split.append(is_split)
    return(output[uid_cols + [metric]])


def mark_ages_to_be_split(df, frmat_type, uid_cols, metric):
    ''' Returns a subset of the df containing markers for the age groups that
        can be split, in which entries have been expanded such that there is an 
        entry for each age that will result from the split (i.e. merges with 
        ma)
        --- Notes ---
            Drops ages that do not merge with the maps
    '''
    # Import age format map
    this_fmt_map = get_age_frmat_map(frmat_type)
    long_fmt_map = dft.wide_to_long(this_fmt_map,
                                  stubnames='age_specific',
                                  i=[frmat_type, 'age'],
                                  j='age_split_num')
    long_fmt_map = long_fmt_map.loc[long_fmt_map.age_specific.notnull(), ]
    long_fmt_map.sort_values([frmat_type, 'age', 'age_specific'], inplace=True)

    # Merge with data. Mark data that should be split
    fmt_mapped = df.merge(long_fmt_map, how='left', on=[frmat_type, 'age'])
    fmt_mapped['to_expand'] = 0
    fmt_mapped.loc[fmt_mapped['age_specific'].notnull(), 'to_expand'] = 1
    
    # edit age formats and rename to enable merge with weights
    fmt_mapped.loc[fmt_mapped['age_specific'].ge(5), 'gbd_age'] = \
        (fmt_mapped['age_specific'].divide(5) + 6 )
    fmt_mapped.loc[fmt_mapped['age_specific'].lt(5), 'gbd_age'] = 2
    return(fmt_mapped)
