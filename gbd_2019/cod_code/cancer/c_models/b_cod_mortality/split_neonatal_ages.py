
'''
Description: Splitting grouped ages in cancer data
Arguments: mortality estimates file
Output: .csv file with split ages 0-4 into neonatal

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
    pipeline_tests as pt
)

def get_uid_columns():
    return(['age','location_id', 'year_id', 'sex_id', 'acause', 'registry_index', 
                'dataset_ids', 'sdi_quintile', 'NID', 'country_id'])


def has_neonatal_age(dataset):
    ''' Determines if data are present for 'neonatal age'
        -- Inputs:
            dataset : pandas dataframe containing an 'age' column and the metric
            metric_name  : string, possible values includs 'pop' and 'deaths'
    '''
    if isinstance(dataset, pd.DataFrame):
        age_to_split = dataset['age'].isin([2])
        result = dataset.loc[age_to_split, ].any().any()
    else:
        result = bool((dataset['age'] == 2))
    return(result)


def add_missing_ages(df, metric):
    ''' Creates new rows for missing metric-age entries (with metric value set 
            to zero)
        Inputs:
            df  : DataFrame, pandas dataframe
    '''
    age_cat = [91, 92, 93, 94]
    missing_ages = [a for a in age_cat if a not in df['age'].unique()]
    fill_missing = df.groupby(
        get_uid_columns(), as_index=False)[metric].count()
    fill_missing[metric] = 0
    for a in missing_ages:
        fill_missing['age'] = a
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
    # Validate that values exist for all rate entriesnn

    rate_df = rate_df[get_uid_columns() + ['rate', 'deaths']]
    rate_df = md.stdz_col_formats(rate_df)

    assert not rate_df['rate'].isnull().any(), \
        "Error in merge. Some observations do not have rates"
    return(rate_df)


def split_neo_age(dataset, wgt_df, uid_cols, metric):
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

    # Split 0-4 age
    neo_age = dataset.loc[dataset['age']
                              == 2, uids_noAge + [metric]].copy()
    neo_age.rename(columns={metric: 'unknown_age_data'}, inplace=True)
    known_age = dataset.loc[dataset['age'] != 2, :].copy()
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
    neo_age = md.stdz_col_formats(neo_age)
    to_distribute = prop_df.merge(neo_age, on = uids_noAge)
    to_distribute.loc[:, 'unknown_age_data'].fillna(value=0, inplace=True)
    to_distribute['orig_data'] = to_distribute[metric].copy()
    to_distribute['unknown_age_data'] = to_distribute['unknown_age_data'].astype(float) 
    to_distribute['proportion'] = to_distribute['proportion'].astype(float) 
    to_distribute.loc[:, metric] += to_distribute['unknown_age_data'].multiply(
        to_distribute['proportion'])
    output = to_distribute[uid_cols + [metric]]
    pt.verify_metric_total(dataset, output, metric, "split unknown age")
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


def split_neonatal(df, metric_name, uid_cols):
    '''Splits 0-4 into neonatal age groups
    '''
    is_pop = bool(metric_name == "pop")
    #
    df[metric_name].fillna(value=0, inplace=True)
    split_df = df.copy()

    # add observation number by group, without age
    uids_noAge = [c for c in uid_cols if 'age' not in c]
    split_df = add_missing_ages(split_df, metric_name)
    obs_numbers = split_df[uid_cols].drop_duplicates()
    obs_numbers['obs'] = obs_numbers.reset_index().index
    split_df = split_df.merge(obs_numbers)
    uid_cols.append('obs')

    # generate cause_weight
    if is_pop:
        cause_wgts = pp.gen_pop_wgts("age_wgt", df['location_id'].unique())
        cause_wgts.rename(columns = {"year":"year_id"}, inplace = True)
    else:
        # create weights used for splitting
        cause_wgts = create_metric_weights(split_df, uid_cols, metric_name)
    
        # collapse to get one weight per observation. dropping redundant entries beforehand 
        cause_wgts.drop_duplicates(subset=['obs','age','sex_id'], inplace=True)
        cause_wgts = dft.collapse(cause_wgts, by_cols=['obs', 'age', 'sex_id'],
                                  func='sum', combine_cols='wgt')

    if has_neonatal_age(split_df):
        print("      splitting neonatal age...")
        # create weights
        split_df = split_neo_age(dataset=split_df,
                            wgt_df=cause_wgts,
                            metric=metric_name,
                            uid_cols=get_uid_columns())

    # collapse remaining underu5 and 80+ ages
    final_df = split_df.copy(deep=True)
    final_df = final_df.loc[~final_df['age'].isin([2]), :]

    # collapse to incorperate newly-split data
    final_df = dft.collapse(final_df,
                            by_cols=get_uid_columns(),
                            func='sum',
                            combine_cols=metric_name
                            )
    final_df = check_and_save(final_df, metric_name)
    return(final_df)



def run_split_metric(this_dataset, metric_name):
    '''Calls the split function if split is needed 
    '''
    print("   splitting metric data...")
    uid_cols = get_uid_columns()

    # save if no splitting is needed
    if not (has_neonatal_age(this_dataset)):
        print("There is nothing to split ... ")
        return(None)
    else:
        final_df = split_neonatal(this_dataset, metric_name, uid_cols)
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

def combine_ages(old_df, neo_df,metric):
    '''Combines our datasets together and saves
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


def run_neonatal_split():
    '''
    '''
    print('running neonatal splitting...')
    # load input dataset
    this_dataset = pd.read_csv("{}/mortality_estimates.csv".format(utils.get_path("mortality_model", 
                               base_folder = "storage")))

    # grabbing cancer age column temporaily 
    this_dataset = gct.cancer_age_from_age_group_id(this_dataset)
    this_dataset.drop("age_group_id", axis = 1, inplace = True)
    this_dataset.rename(columns={"dataset_id":"dataset_ids"}, inplace = True)
    old_df = this_dataset.copy()
    old_df = old_df[~old_df['age'].isin([2])]
    this_dataset = this_dataset[this_dataset.age.isin([2])]
    # Split 0-4
    death_df = run_split_metric(this_dataset[get_uid_columns() + ['deaths','pop']], 'deaths')
    pop_df = run_split_pop(this_dataset[get_uid_columns() + ['pop']], 'pop')
    # combine non-split data to death and population dataframe 
    df_metric_final = combine_ages(old_df, death_df,'deaths')
    df_pop_final = combine_ages(old_df, pop_df,'pop')
    del df_metric_final['pop'] # remove pop from split_metric dataframe 
    merge_cols = get_uid_columns()
    merge_cols.remove('age')
    merge_cols = merge_cols + ['age_group_id']
    df_final = pd.merge(df_metric_final, df_pop_final, how='inner', on=merge_cols)
    # save file
    df_final.to_csv("{}/split_mortality_estimates.csv".format(utils.get_path("mortality_model", 
                                base_folder = "storage")))
    return(df_final)
    print("Neo Ages are Split.")


if __name__ == "__main__":
    run_neonatal_split()
