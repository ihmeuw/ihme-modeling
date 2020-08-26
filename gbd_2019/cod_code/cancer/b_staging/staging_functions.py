
'''
Description:

How To Use:
'''

import pandas as pd
from functools import partial
from ast import literal_eval
from cancer_estimation.py_utils import (
    common_utils as utils,
    data_format_tools as dft,
    modeled_locations
)
from cancer_estimation.a_inputs.a_mi_registry import (
    populations as pop,
    mi_dataset
)
from cancer_estimation._database import cdb_utils as cdb
from cancer_estimation.py_utils.pandas_expansions import tuple_unique_entries


def update_repness(df):
    ''' Adds superceeding representative status from the override file
    '''
    repres_update_file = utils.get_path("representativeness_override",
                                        process="staging")
    repres_update = pd.read_csv(repres_update_file)
    repres_update = repres_update[[
        'country_id', 'grouping', 'representative']]
    repres_update.rename(
        columns={'representative': 'update_rep'}, inplace=True)
    df = modeled_locations.add_country_id(df)
    df.loc[df['location_id'] == df['country_id'], 'grouping'] = 'national'
    df.loc[df['grouping'] != 'national', 'grouping'] = 'subnational'
    df = df.merge(repres_update, how='left')
    df.loc[df['update_rep'].notnull(), 'representative'] = df['update_rep']
    df.loc[df['representative'].isnull(), 'representative'] = 0
    df = df.drop(['update_rep', 'grouping'], axis=1)
    return(df)


def add_representativeness(df):
    ''' Adds 'representative' integer indicating whether data are representative
            of their attached location_id
    '''
    def _avg_repness(regs, rep_table):
        ''' If all registries are representative, returns 1. Else returns 0
        '''
        try:
            if not isinstance(regs, tuple) and not isinstance(regs, list):
                try:
                    regs = list(literal_eval(regs))
                except:
                    regs = list(regs)
            rep = rep_table.loc[rep_table['registry_index'].isin(regs),
                                'representative_of_location_id']
            if len(rep) == 0:
                return(0)
            else:
                return(rep.min())
        except:
            return(0)

    print("adding representative status...")
    db_link = cdb.db_api("cancer_db")
    # Add representative status based on the input registries
    rep_status = db_link.get_table(
        "registry")[['registry_index', 'representative_of_location_id']]
    rep_df = pd.DataFrame(
        {'registry_index': df['registry_index'].unique().tolist()})
    get_repness = partial(_avg_repness, rep_table=rep_status)
    rep_df.loc[:, 'representative'] = rep_df['registry_index'].apply(
        get_repness)
    output = df.merge(rep_df, on='registry_index')
    output = update_repness(output)
    assert len(output) == len(
        df), "add_representativeness is adding or deleting data"
    return(output)


def add_coverage_metadata(df):
    ''' Returns the dataframe with a boolean indication of whether each
            registry covers it's associated location_id
    '''
    merge_col = ['registry_index']
    metadata_cols = ['full_coverage', 'national_registry']
    assert all(c in df.columns for c in merge_col), \
        "add_coverage_metadata requires {} column(s)".format(merge_col)
    if (all(c in df.columns for c in metadata_cols) and
            df.loc[:, metadata_cols].notnull().all().all()):
        return(df)
    else:
        assert df.loc[:, merge_col].notnull().all().all(), \
            "add_coverage_metadata cannot accept nulls for {} column(s)".format(
                merge_col)
    input_len = len(df)
    df = df.drop(labels=metadata_cols, axis=1, errors='ignore')
    reg_df = cdb.db_api().get_table("registry")
    reg_df.rename(
        columns={'coverage_of_location_id': 'full_coverage'}, inplace=True)
    reg_df.loc[(reg_df['location_id'] == reg_df['country_id']) &
               reg_df['full_coverage'].isin([1]), 'national_registry'] = 1
    reg_df.loc[reg_df['national_registry'].isnull(), 'national_registry'] = 0
    df = df.merge(reg_df[merge_col + metadata_cols], how='left', on=merge_col)
    assert len(df) == input_len, "Data dropped while adding coverage metadata"
    return(df)


def homogenize_pop(df, uid_cols=['registry_index', 'year_id', 'sex_id', 'age_group_id']):
    ''' Returns a dataset with population homogenized by the uid_cols.
        Replaces population values with the mean population for each uid set
        Establishes consistency and fills some missing values
    '''
    mean_pop = df.groupby(uid_cols, as_index=False)['pop'].mean()
    df = df.drop(labels=['pop'], axis=1, errors='ignore')
    output = df.merge(mean_pop, how='left')
    assert len(df) == len(output), "Error homogenizing population"
    return(output)


def add_population(df, data_type_id=2):
    ''' Replaces missing population with IHME estimates where acceptable
    '''
    print("      adding ihme population estimates where possible...")
    # Mark data that may use IHME population estimates
    has_pop = df.loc[df['pop'].notnull(), :]
    no_pop = df.loc[df['pop'].isnull(), :]
    marked_df = _add_ihme_pop_marker(no_pop)
    dont_add_pop = marked_df.loc[marked_df['ihme_pop_ok'].isin([0]), :].copy()
    add_pop = marked_df.loc[marked_df['ihme_pop_ok'].isin([1]), :].copy()
    # Add IHME population to data that can use those estimates
    add_pop = add_pop.drop(labels=['pop'], axis=1, errors='ignore')
    ihme_pop = pop.load_raw_ihme_pop(list(add_pop.location_id.unique()))
    ihme_pop.rename(columns={'population': 'pop'}, inplace=True)
    with_pop = add_pop.merge(ihme_pop, how='left')
    with_pop = with_pop[has_pop.columns.tolist()]
    # Add
    output = has_pop.append(with_pop).append(dont_add_pop)
    assert len(output) == len(
        df), "Error adding population by staging function"
    return(output)


def _add_ihme_pop_marker(df):
    ''' Returns the dataframe with an added 'ihme_pop_ok' column indicating whether
        ihme population estimates may be merged with the uid
    '''
    if not 'sdi_quintile' in df.columns:
        df = modeled_locations.add_sdi_quintile(df)
    if not 'full_coverage' in df.columns:
        df = add_coverage_metadata(df)
    ds_df = cdb.db_api().get_table("dataset")
    df.loc[:, 'ihme_pop_ok'] = 0
    for dsid in df['dataset_id'].unique():
        pop_ok = ds_df.loc[ds_df['dataset_id'] ==
                           dsid, 'can_use_ihme_pop'].values[0]
        if pop_ok == 1:
            df.loc[df['dataset_id'] == dsid, 'ihme_pop_ok'] = pop_ok
    ihme_pop_ok = (df['sdi_quintile'].isin([5]) &
                   (df['full_coverage'].isin([1])))
    df.loc[ihme_pop_ok, 'ihme_pop_ok'] = 1
    return(df)


def combine_uid_entries(df, uid_cols, metric_cols,
                        combined_cols=['NID', 'registry_index', 'dataset_id'],
                        collapse_metrics=True):
    ''' Preserves a list of all entries in the combined_cols before collapsing
            by uid_cols to calculate the sum of the metric_cols
        Returns a dataframe collapsed by uid_cols
        -- Inputs
            collapse_metrics : set to False to prevent collapse after re-setting
                combined cols entries
    '''
    assert not df[uid_cols+combined_cols].isnull().any().any(), \
        "Cannot combine dataframe with null values in uid or combined columns"
    combined_cols = [c for c in combined_cols if c in df.columns]
    static_cols = [c for c in df.columns if c not in combined_cols]
    combined_entries = df[static_cols].copy()
    for col in combined_cols:
        new_entries = df[uid_cols+[col]].groupby(uid_cols, as_index=False)[col].agg(
            lambda c: tuple_unique_entries(c))
        new_entries.loc[:, col] = new_entries[col].astype(str)
        combined_entries = combined_entries.merge(
            new_entries, on=uid_cols, how='left')
        assert not combined_entries[col].isnull().any(), \
            "Error combining uids for column {}".format(col)
    if collapse_metrics:
        output = dft.collapse(combined_entries, by_cols=uid_cols+combined_cols,
                              combine_cols=metric_cols, func='sum')
    else:
        output = combined_entries
    return(output)
