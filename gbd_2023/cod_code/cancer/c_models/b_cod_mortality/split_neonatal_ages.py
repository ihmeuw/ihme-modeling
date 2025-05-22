# -*- coding: utf-8 -*-
'''
Description: Splitting grouped ages in cancer data
Arguments: mortality estimates file
Output: .csv file with split ages 0-4 into neonatal, and 1-4 age groups
Contributors: INDIVIDUAL_NAME, INDIVIDUAL_NAME, INDIVIDUAL_NAME
'''

# Import libraries
import sys
import pandas as pd
import numpy as np
import re

# Import cancer_estimation utilities and modules
from cancer_estimation.py_utils import (
    common_utils as utils,
    data_format_tools as dft,
    modeled_locations, 
    gbd_cancer_tools as gct
)
from cancer_estimation.a_inputs.a_mi_registry.age_sex_splitting import (
    age_sex_tests as at,
)
from cancer_estimation.a_inputs.a_mi_registry import (
    mi_dataset as md,
    prep_proportions as pp,
    pipeline_tests as pt,
    populations as pop
)
from cancer_estimation.registry_pipeline.cause_mapping import recode
from cancer_estimation.b_staging import staging_functions as staging


def get_split_pairs():
    return({1: { 2:[91, 92, 93, 94]},
            2: { 93:[388, 389]},
            3: { 94:[34, 238]}})


def get_uid_columns():
    return(['age','location_id', 'year_id', 'sex_id', 'acause', 'registry_index', 
                'dataset_ids', 'sdi_quintile', 'NID', 'country_id'])


def has_neonatal_age(dataset, stage_step = 1):
    ''' Determines if data are present for 'neonatal age'
        -- Inputs:
            dataset : pandas dataframe containing an 'age' column and the metric
            metric_name  : string, possible values includs 'pop' and 'deaths'
    '''
    split_pairs = get_split_pairs()
    if isinstance(dataset, pd.DataFrame):
        age_to_split = dataset['age'].isin(list(split_pairs[stage_step].keys()))
        result = dataset.loc[age_to_split, ].any().any()
    else:
        result = bool((dataset['age'] == list(split_pairs[stage_step].keys())[0]))
    return(result)


def add_missing_ages(df, metric, stage_step, pop_df = None):
    ''' Creates new rows for missing metric-age entries (with metric value set 
            to zero). Adds in missing pop for splitting deaths.
        Inputs:
            df  : DataFrame, pandas dataframe
    '''
    split_pairs = get_split_pairs()
    age_cat = list(split_pairs[stage_step].values())[0]
    missing_ages = [a for a in age_cat if a not in df['age'].unique()]
    fill_missing = df.groupby(
        get_uid_columns(), as_index=False)[[metric]].count()
    fill_missing[metric] = 0
    
    for a in missing_ages:
        fill_missing['age'] = a
        # add in split population to metric data
        if metric != "pop":
            fill_missing= md.stdz_col_formats(fill_missing) 
            pop_df = md.stdz_col_formats(pop_df)
            fill_missing = pd.merge(fill_missing, pop_df[pop_df['age'].eq(a)], 
                                                    on = get_uid_columns())
            df = df.append(fill_missing)
            fill_missing.drop("pop", axis = 1, inplace = True)
        else:
            df = df.append(fill_missing)
    return(df)


def add_cause_rates(dataset, uid_cols, metric):
    ''' Returns expected values used for splitting data in the cancer pipeline.
        Uses two merges
        --- Inputs ---
        dataset : DataFrame
                pandas dataframe
    '''
    print("      loading cause rates...")

    # Make acause long and remove blank acause entries
    merge_df = dataset.copy()
    # Save original acause for later reversion
    merge_df.loc[:, 'orig_acause'] = merge_df['acause']

    # Load maps
    acause_rates = pp.load_cause_rates(3) # 3 for deaths 

    # Merge with rates. If rate is not available, use "average_cancer" rate
    cause_df = md.stdz_col_formats(merge_df)
    acause_rates = md.stdz_col_formats(acause_rates)
    rate_df = cause_df.merge(acause_rates, how='left', 
                                on = ["age", "acause", "sex_id"], 
                                indicator=True)

    not_mapped_to_rate = (rate_df['_merge'].isin(['left_only']))
    rate_df.loc[not_mapped_to_rate, 'acause'] = "average_cancer"
    rate_df.drop(['_merge', 'rate'], axis=1, inplace=True)
    rate_df = rate_df.merge(
        acause_rates, on=['acause', 'sex_id', 'age'], how='left')

    # set rates to 0 for age 2 (0-4 ages)
    rate_df.loc[rate_df['age'].isin([2]), 'rate'] = 0

    # Set rates to 0 for non-prepped ages
    rate_df.loc[~rate_df['age'].isin(acause_rates['age'].unique()), 'rate'] = 0
    # Reset acause to the input acause
    rate_df.loc[:, 'acause'] = rate_df['orig_acause']
    # Validate that values exist for all rate entries

    rate_df = rate_df[get_uid_columns() + ['rate', 'deaths']]
    rate_df = md.stdz_col_formats(rate_df)

    assert not rate_df['rate'].isnull().any(), \
        "Error in merge. Some observations do not have rates"
    return(rate_df)


def split_neo_age(dataset, wgt_df, uid_cols, metric, stage_step):
    ''' redistribute unknown ages
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

    split_pairs = get_split_pairs()
    # Split current age group
    neo_age = dataset.loc[dataset['age'] == list(split_pairs[stage_step].keys())[0], uids_noAge + [metric]].copy()

    neo_age.rename(columns={metric: 'unknown_age_data'}, inplace=True)
    known_age = dataset.loc[dataset['age'] != list(split_pairs[stage_step].keys())[0], :].copy()
    known_age.fillna(value=0, inplace=True)

    # standardize columns to enable merging
    wgt_df = md.stdz_col_formats(wgt_df) 
    wgt_df_uids = list(wgt_df.columns)
    wgt_df_uids = [u for u in wgt_df_uids if 'age' != u]
    wgt_df_uids = [u for u in wgt_df_uids if 'wgt' != u]

    with_weights = known_age.merge(wgt_df, how='left')

    at.test_weights(known_age, with_weights)
    prop_df = pp.add_proportions(with_weights, uids_noAge)

    prop_df = md.stdz_col_formats(prop_df) 
    prop_df['sdi_quintile'] = prop_df['sdi_quintile'].astype('int')
    neo_age = md.stdz_col_formats(neo_age)
    neo_age['sdi_quintile'] = neo_age['sdi_quintile'].astype('float')
    neo_age['sdi_quintile'] = neo_age['sdi_quintile'].astype('int')
    to_distribute = prop_df.merge(neo_age, on = uids_noAge)

    to_distribute.loc[:, 'unknown_age_data'].fillna(value=0, inplace=True)
    to_distribute['orig_data'] = to_distribute['unknown_age_data'].copy()
    to_distribute['unknown_age_data'] = to_distribute['unknown_age_data'].astype(float) 
    to_distribute['proportion'] = to_distribute['proportion'].astype(float) 
    to_distribute.loc[:, metric] += to_distribute['unknown_age_data'].multiply(
        to_distribute['proportion'])

    if metric == "deaths":
        output = to_distribute[uid_cols + [metric] + ['obs','pop']]
    else:
        output = to_distribute[uid_cols + [metric] + ['obs']]
    at.compare_pre_post_split(output, dataset, metric)
    pt.verify_metric_total(dataset, output, metric, 
                                "splitting young ages for {}".format(stage_step))
    return(output)


def create_metric_weights(df, uid_cols, metric):
    ''' Returns a dataframe of expected values ("weights") used for splitting 
        neonatal data in the cancer pipeline.
        --- Inputs ---
        df : DataFrame
                pandas dataframe
    '''
    rate_df = add_cause_rates(df, uid_cols, metric)
    df = md.stdz_col_formats(df)
    # make weights (weights = expected number of events = rate*pop)rate
    weight_df = pd.merge(df, rate_df[get_uid_columns() + ['rate', 'deaths']], 
                                                        on = get_uid_columns() + ['deaths'])
    weight_df.loc[:, 'wgt'] = weight_df['rate'] * weight_df['pop']
    weight_df = md.stdz_col_formats(
        weight_df, additional_float_stubs=['wgt', 'rate'])
    return(weight_df)


def split_neonatal(df, metric_name, uid_cols, pop_df = None):

    '''Splits 0-4 into neonatal age groups and 1-4, and then
    split neonatal and 1-4 further
    '''

    is_pop = bool(metric_name == "pop")
    #
    gbd_id = utils.get_gbd_parameter("current_gbd_round")
    df[metric_name].fillna(value=0, inplace=True)
    split_df = df.copy()
    split_pairs = get_split_pairs()

    # add observation number by group, without age
    uids_noAge = [c for c in uid_cols if 'age' not in c]

    def split_new_ages(split_df, age_groups, stage_step, pop_df = None):
        if metric_name != "pop":
            split_df = add_missing_ages(split_df, metric_name, stage_step, pop_df)
        else:
            split_df = add_missing_ages(split_df, metric_name, stage_step)
        
        # add in split new pop
        if 'obs' in uid_cols:
            uid_cols.remove('obs')
        if metric_name == "pop":
            split_df = split_df[uid_cols + [metric_name]].drop_duplicates()
        else:
            split_df = split_df[uid_cols + ['deaths','pop']].drop_duplicates() 

        # manually make a sequential index-like column
        split_df['obs'] = split_df.reset_index().index
        uid_cols.append('obs')

        # generate cause_weight
        if is_pop:
            cause_wgts = pp.gen_pop_wgts("age_wgt", df['location_id'].unique())
            cause_wgts.rename(columns = {"year" : "year_id"}, inplace = True)
        else:
            # create weights used for splitting
            cause_wgts = create_metric_weights(split_df, uid_cols, metric_name)

            # collapse to get one weight per observation. dropping redundant entries beforehand 
            cause_wgts.drop_duplicates(subset=['obs','age','sex_id'], inplace=True)
            cause_wgts = dft.collapse(cause_wgts, by_cols=['obs', 'age', 'sex_id'],
                                    func='sum', combine_cols='wgt')

        if has_neonatal_age(split_df, stage_step):
            print("      splitting neonatal age...")
            # create weights
            split_df = split_neo_age(dataset=split_df,
                                wgt_df=cause_wgts,
                                metric=metric_name,
                                uid_cols=get_uid_columns(),
                                stage_step = stage_step)

        # collapse remaining under 5 and 80+ ages
        final_df = split_df.copy(deep=True)

        # removing original data age group and preserve split data
        split_pairs = get_split_pairs()
        final_df = final_df.loc[~final_df['age'].isin(list(split_pairs[stage_step].keys())), :]

        # collapse to incorperate newly-split data
        if metric_name == "deaths":
            final_df = dft.collapse(final_df,
                                by_cols=get_uid_columns(),
                                func='sum',
                                combine_cols=[metric_name, 'pop']
                                )
        else:
            final_df = dft.collapse(final_df,
                                by_cols=get_uid_columns(),
                                func='sum',
                                combine_cols=[metric_name]
            )

        assert (set(final_df['age'].unique().tolist()) - set(age_groups)) == set([]) , \
            "Missing age groups"
        if metric_name != 'pop':
            assert (final_df[metric_name].sum() != 0), \
                "{} data has 0s".format(metric_name)
        return(final_df)

    # split 0-4 into neonatal and 1-4 and smaller ages including pop
    if metric_name != "pop":
        final_df = split_new_ages(split_df, age_groups = list(split_pairs[1].values())[0], 
                                            stage_step = 1,
                                            pop_df = pop_df[get_uid_columns() + ['pop']])

        postnatal = split_new_ages(final_df[final_df['age'].isin(list(split_pairs[2].keys()))], 
                                age_groups = list(split_pairs[2].values())[0], 
                                stage_step = 2,
                                pop_df = pop_df[get_uid_columns() + ['pop']])

        # test for 0s or NAs
        one_to_4 = split_new_ages(final_df[final_df['age'].isin(list(split_pairs[3].keys()))], 
                                    age_groups = list(split_pairs[3].values())[0], 
                                    stage_step = 3,
                                    pop_df = pop_df[get_uid_columns() + ['pop']])

    else:
        final_df = split_new_ages(split_df, 
                                    age_groups = list(split_pairs[1].values())[0], 
                                    stage_step = 1)

        # split post-neonatal and 1-4 into new age groups
        postnatal = split_new_ages(final_df[final_df['age'].isin(list(split_pairs[2].keys()))], 
                                    age_groups = list(split_pairs[2].values())[0], 
                                    stage_step = 2)

        one_to_4 = split_new_ages(final_df[final_df['age'].isin(list(split_pairs[3].keys()))], 
                                    age_groups = list(split_pairs[3].values())[0], 
                                    stage_step = 3)
    
    final_df = pd.concat([final_df, postnatal, one_to_4])

    if metric_name != "pop":
        final_df = final_df[final_df['age'].isin([91, 92, 388, 389, 238, 34])]
    else:
        # preserve 93, 94 for splitting metric data
        final_df = final_df[final_df['age'].isin([91, 92, 93, 94, 388, 389, 238, 34])]
    
    assert (final_df[metric_name].sum() != 0), \
        "{} data has 0s".format(metric_name)

    assert (not(final_df[metric_name].isnull().values.any())), \
        "{} data has NAs".format(metric_name)

    final_df = check_and_save(final_df, metric_name)
    return(final_df)


def run_split_metric(this_dataset, metric_name, pop_df):
    '''Calls the split function if split is needed 
    '''
    print("   splitting metric data...")
    uid_cols = get_uid_columns()

    # save if no splitting is needed
    if not (has_neonatal_age(this_dataset)):
        print("There is nothing to split ... ")
        return(None)
    else:
        final_df = split_neonatal(this_dataset, metric_name, uid_cols, pop_df)
        return(final_df)


def run_split_pop(this_dataset, metric_name):
    '''
    '''
    uid_cols = get_uid_columns() 
    # save if no splitting is needed 
    if not (has_neonatal_age(this_dataset)):
        print('there is nothing to split...')
        return(None) 
    else: 
        final_df = split_neonatal(this_dataset, metric_name, uid_cols)
        return(final_df)


def get_required_cols(): 
    '''
    '''
    return(['NID','acause','age_group_id','country_id','dataset_ids',
            'deaths','location_id','pop','registry_index','sdi_quintile','sex_id',
            'year_id'])


def combine_ages(old_df, neo_df, metric):
    '''Combines our datasets together
    '''
    df_total = pd.concat([old_df, neo_df])

    # add back age_group_id and drop cancer age
    df_total = gct.age_group_id_from_cancer_age(df_total)
    df_total.drop("age", axis = 1, inplace = True)
    req_cols = get_required_cols() 
    if metric == 'pop': 
        req_cols.remove('deaths')
    return(df_total[req_cols])


def check_and_save(df, metric):
    '''Does common tests on our dataset for testing
    '''
    output = md.stdz_col_formats(df)
    dataset_verified = pt.verify_prep_step_output(
        output, get_uid_columns(), metric)
    if dataset_verified:
        return(output)
    else:
        raise AssertionError("ERROR: data could not be verified on output")
    pass


def apply_recode(df):
    print("    recoding deaths...")
    input_framework = df[['location_id',
                          'year_id', 'sex_id']].drop_duplicates()
    df = df.loc[df['acause'].str.startswith("neo_"), :]
    # subset to make the recode faster

    # also include new ages
    young_ages = df.loc[df['age_group_id'].isin(utils.get_gbd_parameter("young_ages_new")), :]
    young_ages = recode(young_ages, data_type_id = 3)
    young_ages.drop('age', axis = 1, inplace = True)
    uid_cols = get_uid_columns()
    uid_cols.remove('age')
    uid_cols.append('age_group_id')
    df = df[uid_cols + ['deaths', 'pop']]
    adults = df.loc[~df['age_group_id'].isin(utils.get_gbd_parameter("young_ages_new")),
                    [c for c in df.columns.unique() if c in young_ages.columns]]
    adults = adults[adults.columns.unique()]
    recoded = pd.concat([adults, young_ages])
    recoded = recoded[uid_cols + ['deaths','pop']]
    print("    recombining re-coded data...")
    uid_cols = ['location_id', 'year_id', 'sex_id', 'age_group_id', 'acause', 'country_id', 'sdi_quintile']
    recoded = staging.combine_uid_entries(
                    recoded, uid_cols + ['pop'],
                    combined_cols = ['NID', 'registry_index', 'dataset_ids'],
                    metric_cols=['deaths'])
    # Re-set sdi quintile to account for merges
    recoded = modeled_locations.add_sdi_quintile(recoded, delete_existing=True)
    # Test output
    check_len = len(recoded)
    recoded = recoded.merge(input_framework, how='outer')
    assert len(recoded) == check_len, \
        "Some uids lost after recode"  # ensure that no "null" entries are added on outer merge
    assert not recoded.loc[recoded.duplicated(uid_cols), :].any().any(), \
        "Duplicates exist after recode"
    assert not (recoded['deaths'] < 0).any(), "Erroneous death values exist"
    pt.verify_metric_total(df, recoded, 'deaths', "recode applied")
    return(recoded)


def run_neonatal_split():
    '''
    '''
    gbd_id = utils.get_gbd_parameter("current_gbd_round")
    print('running neonatal splitting...')
    # load input dataset

    input_df = pd.read_csv("{}/mortality_estimates.csv".format(utils.get_path("mortality_model", 
                               base_folder = "storage")))
    # grabbing cancer age column temporarily 
    this_dataset = gct.cancer_age_from_age_group_id(input_df)
    this_dataset.drop("age_group_id", axis = 1, inplace = True)
    this_dataset.rename(columns={"dataset_id":"dataset_ids"}, inplace = True)
    old_df = this_dataset.copy()
    old_df = old_df[~old_df['age'].isin([2])]
    this_dataset = this_dataset[this_dataset.age.isin([2])]
    # Split 0-4 and post neonatal
    pop_df = run_split_pop(this_dataset[get_uid_columns() + ['pop']], 'pop')
    # adds in when pop = 0
    nonzero_pop = pop_df[pop_df['pop'] != 0]
    zero_pop = pop_df[pop_df['pop'] == 0]
    zero_pop.drop('pop', axis = 1, inplace = True)
    new_pop = pop.load_formatted_ihme_pop(zero_pop['location_id'].unique())
    new_pop = gct.cancer_age_from_age_group_id(new_pop)
    new_pop.rename(columns = {'year':'year_id'}, inplace = True)
    zero_pop = pd.merge(zero_pop, new_pop, how='left', 
                                on = ['location_id', 'age', 'sex_id', 'year_id'])
    pop_df = pd.concat([nonzero_pop, zero_pop])
    pop_df.drop('age_group_id', axis = 1, inplace = True)

    if gbd_id >= 7:
        assert ([91, 92, 93, 94, 388, 389, 238, 34] in pop_df['age'].unique()), \
            "pop data missing neonatal ages"
    else:
        assert ([91, 92, 93, 94] in pop_df['age'].unique()), \
            "pop data missing neonatal ages"

    # merge on new ages
    death_df = run_split_metric(this_dataset[get_uid_columns() + ['deaths','pop']], 
                                    'deaths', pop_df)
    if gbd_id >= 7:
        assert ([91, 92, 388, 389, 238, 34] in death_df['age'].unique()), \
            "death data missing neonatal ages"
    else:
        assert ([91, 92, 93, 94] in death_df['age'].unique()), \
            "death data missing neonatal ages"

    # combine non-split data to death and population dataframe 
    df_metric_final = combine_ages(old_df, death_df,'deaths')
    df_pop_final = combine_ages(old_df, pop_df,'pop')
    del df_metric_final['pop'] # remove pop from split_metric dataframe 
    merge_cols = get_uid_columns()
    merge_cols.remove('age')
    merge_cols = merge_cols + ['age_group_id']
    df_metric_final = md.stdz_col_formats(df_metric_final) 
    df_pop_final = md.stdz_col_formats(df_pop_final)
    df_final = pd.merge(df_metric_final, df_pop_final, how='inner', on=merge_cols)
    pt.verify_metric_total(df_metric_final, df_final, 'deaths', "merge in population")
    assert set(utils.get_gbd_parameter("young_ages_new")) < set(df_final['age_group_id'].unique()), \
        "final data missing neonatal ages"

    recoded_df = apply_recode(df_final)
    pt.verify_metric_total(input_df, recoded_df, 'deaths', "comparing input and final split data")
    # save file
    recoded_df.to_csv("{}/split_mortality_estimates.csv".format(utils.get_path("mortality_model", 
                                base_folder = "storage")))
    print("Neo Ages are Split and deaths recoded.")
    return(recoded_df)

if __name__ == "__main__":
    run_neonatal_split()
