import getpass
import sys
import warnings
import numpy as np
import pandas as pd
from db_queries import get_population, get_cause_metadata, get_rei_metadata
from db_tools.ezfuncs import query

sys.path.append('FILEPATH')
from cod_prep.process import age_sex_split  # had to throw a __init__ file in the directory
reload(age_sex_split)

# load our functions
if getpass.getuser() == 'USERNAME':
    sys.path.append("FILEPATH")
import hosp_prep

def run_age_sex_splitting(df, split_sources=[], round_id=0, verbose=False):
    """ takes in dataframe of data that you want to split, and a list of sources
    that need splitting, and then uses age_sex_splitting.py to split.
    as of 6/16/2017 this function assumes that the weights you want to use are already
    present. There is some skeletal stuff to deal with multiple rounds for weights
    but it's not currently fleshed out
    """

    # write data used for visualization
    if round_id == 1 or round_id == 2:
        # save pre_split data for viz, attach location name and bundle name and save
        pre_split = df[df.source.isin(split_sources)]
        pre_split = pre_split.merge(query(SQL QUERY),
                                    how='left', on='location_id')
        pre_split.drop(['year_end', 'age_group_unit', 'age_end', 'nid',
                        'facility_id', 'representative_id', 'diagnosis_id',
                        'outcome_id', 'metric_id'], axis=1, inplace=True)
        pre_split.to_csv("FILEPATH", index=False)

    # drop data and apply age/sex restrictions
    df = hosp_prep.drop_data(df, verbose=False)
    df = hosp_prep.apply_restrictions(df, col_to_restrict='val')

    # list of col names to compare for later use
    pre_cols = df.columns
    # we need to fix the terminal ages by source
    lst = []
    for asource in df.source.unique():
        subdf = df[df['source'] == asource].copy()
        old_max_age = subdf.age_end.max()  # store old bad terminal age
        # replace terminal age end so that age_sex_split will recognize this as a "bad" age_group
        subdf.loc[subdf.age_end == old_max_age, 'age_end'] = 124
        lst.append(subdf)
        del subdf
    df = pd.concat(lst)
    del lst

    # make age end match what is in the shared DBB
    df.loc[df['age_end'] > 1, 'age_end'] = df.loc[df['age_end'] > 1, 'age_end'] + 1
    # get age info
    age_group_info = query(SQL QUERY)
    # this age group is wrong.
    age_group_info = age_group_info[age_group_info['age_group_id'] != 161]
    # has same age_start and end as 22, results in duplicates
    age_group_info = age_group_info[age_group_info['age_group_id'] != 27]
    age_group_info.rename(columns={"age_group_years_start": "age_start",
                                   "age_group_years_end": "age_end"},
                                   inplace=True)

    # merge ages on
    pre = df.shape[0]
    df = df.merge(age_group_info, how="left",
                                      on=['age_start', 'age_end'])
    assert pre == df.shape[0],\
    r"the number of rows changed after "
    r"merging on age_group_id, probably because there are "
    r"2 ids that share the same age_start and age_end"
    # drop age start/end for simplicity
    df.drop(['age_start', 'age_end'], axis=1, inplace=True)

    df_list = []  # initialize empty list to concat split dfs to

    # columns that uniquely identify rows of data
    id_cols = ['source', 'nid', 'nonfatal_cause_name',
               'year_id', 'age_group_id', 'sex_id', 'location_id']
    perfect_ages = [5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17,
                    18, 19, 20, 28, 30, 31, 32, 235]
    rows = df.shape[0]
    numer = 0
    # for source in split_sources:
    for source in df.source.unique():
        # make df of one source that needs to be split
        splitting_df = df[df.source == source].copy()
        numer += splitting_df.shape[0]
        print(source)
        print("working: {}% done".format(float(numer)/rows * 100))
        if verbose:
            print "{}'s age groups before:".format(source)
            print splitting_df[['age_group_id']].\
                drop_duplicates().sort_values(by='age_group_id')

        # create year_id
        splitting_df['year_id'] = splitting_df['year_start']
        splitting_df.drop(['year_start', 'year_end'], axis=1, inplace=True)
        if verbose:
            print "now splitting {}".format(source)
        # check if data needs to be split, if not append to dflist and move on
        if set(splitting_df.age_group_id).symmetric_difference(set(perfect_ages)) == set():
            df_list.append(splitting_df)
            continue
        # the function from CoD team that does the splitting
        split_df = age_sex_split.split_age_sex(splitting_df,
                                               id_cols,
                                               value_column='val',
                                               level_of_analysis='nonfatal_cause_name',
                                               fix_gbd2016_mistake=False)
        if verbose:
            print(
                "Orig value sum {} - New value sum {} = {} \n".format(
                    splitting_df.val.sum(),
                    split_df.val.sum(),
                    splitting_df.val.sum() - split_df.val.sum()
                )
            )
        pre_val = splitting_df.val.sum()
        post_val = split_df.val.sum()
        assert pre_val - post_val <= (pre_val * .0001),\
        "Too many cases were lost"
        if verbose:
            print "{}'s ages after:".format(source)
            print split_df[['age_group_id']].\
                drop_duplicates().sort_values(by='age_group_id')
        # append split data to our list of dataframes
        df_list.append(split_df)

    # bring the list of split DFs back together
    df = pd.concat(df_list).reset_index(drop=True)

    df = df.merge(age_group_info, how='left', on='age_group_id')

    # change age_end to match the rest of the data
    df.loc[df.age_end > 1, 'age_end'] =\
        df.loc[df.age_end > 1, 'age_end'] - 1

    df.loc[df.age_end == 124, 'age_end'] = 99

    # check that we have the right number of age groups
    assert df[['age_start', 'age_end', 'age_group_id']].drop_duplicates()\
        .shape[0]==21

    # prepare to reattach split data to main df
    df['year_start'] = df['year_id']
    df['year_end'] = df['year_id']
    df.drop(['year_id', 'age_group_id'], axis=1, inplace=True)
    assert set(df.columns).symmetric_difference(set(pre_cols)) == set()

    # data used for visualization
    if round_id == 1 or round_id == 2:
        df = df.merge(query(SQL QUERY))

        # ? df_for_tableau = df.copy()
        df.drop(['year_end', 'age_group_unit', 'age_end', 'nid',
                       'facility_id', 'representative_id', 'diagnosis_id',
                       'outcome_id', 'metric_id'], axis=1).to_csv(
                       "FILEPATH", index=False)

    return(df)
