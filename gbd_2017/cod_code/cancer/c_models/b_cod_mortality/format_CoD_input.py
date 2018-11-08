# -*- coding: utf-8 -*-
'''
'''
import os
import pandas as pd
import numpy as np
from ast import literal_eval
from utils import (
    data_format_tools as dft,
    common_utils as utils,
    modeled_locations,
    pydo
)
import b_staging.staging_functions as staging
from registry_pipeline.df_manipulate import tuple_unique_entries
from db_queries import (
    get_envelope,
    get_population
)
import a_inputs.a_mortality_incidence.mi_dataset as md
from _database import cdb_utils as cdb



def get_uid_cols():
    return(['location_id', 'year_id', 'sex_id', 'age_group_id', 'acause'])


def load_mortality_envelope(location_id_list, age_group_list, year_list):
    ''' Returns the current all-cause mortality envelope
    '''
    env = get_envelope(sex_id=[1, 2],
                       location_id=location_id_list,
                       year_id=year_list,
                       age_group_id=age_group_list)
    env.rename(columns={'mean': 'envelope'}, inplace=True)
    pop = get_population(sex_id=[1, 2],
                         location_id=location_id_list,
                         year_id=year_list,
                         age_group_id=age_group_list)
    env = env.merge(pop, on=['location_id', 'year_id',
                             'sex_id', 'age_group_id'])
    env['death_rate'] = env['envelope']/env['population']
    env = env[['location_id', 'year_id', 'sex_id', 'age_group_id', 'death_rate']]
    return(env)


def extract_single_nid(nid_entry):
    ''' If multiple nids are present, returns a single nid
    '''
    try:
        nid_entry = literal_eval(nid_entry)
    except ValueError:
        pass
    if not isinstance(nid_entry, tuple) or isinstance(nid_entry, list):
        nid_entry = [nid_entry]
    if len(nid_entry) == 1:
        if str(nid_entry[0]).isdigit() and str(nid_entry[0]) != '0':
            return(int(nid_entry[0]))
        else:
            pass
    return(int(utils.get_gbd_parameter('generic_cancer_nid')))


def add_subdiv(df):
    '''## Creates "site" information ('subdiv') displayed by CoD vis and source information
    '''
    def _format_registries(reg_tup):
        if not isinstance(reg_tup, tuple):
            try:
                reg_tup = literal_eval(reg_tup)
            except:
                reg_tup = tuple(reg_tup)
        reg_list = [r[(r.find(".")+1):] for r in reg_tup]  # remove country_id
        return(", ".join(reg_list))

    def _format_dataset_id(ds_tup):
        d = str(ds_tup
                ).replace("(", ""
                          ).replace(")", ""
                                    ).replace("\'", ""
                                              ).replace(",", "")
        return(d)

    print("adding subdiv (site labels)...")
    # preserve input length for later testing
    input_len = len(df)
    # the following uids should really be the "combine uid entries" uids for the whole pipeline
    subdiv_uids = ['country_id'] + \
        [c for c in get_uid_cols() if c not in ['acause', 'age_group_id']]
    df = staging.combine_uid_entries(df, subdiv_uids, metric_cols=['deaths'],
                                     collapse_metrics=False)
    # Re-set sdi quintile to account for merges
    df = modeled_locations.add_sdi_quintile(df, delete_existing=True)
    # Generate subdiv values, unique to each cause
    df['subdiv'] = df['dataset_id'].apply(_format_dataset_id) + \
        ": " + df['registry_index'].apply(_format_registries)
    df.loc[df['subdiv'].str.len() >= 200, 'subdiv'] = df.loc[df['subdiv'].str.len()
                                                             >= 200, 'subdiv'].str[:197] + "..."
    assert not df[df.duplicated(get_uid_cols())].any().any(), \
        "Duplicate values present after subdiv"
    # Test output
    assert len(df) == input_len, \
        "Error generating source label. Entries are not consistent"
    assert not df[df.duplicated(get_uid_cols())].any().any(), \
        "Duplicate values present at end of add_subdiv"
    return(df)


def refine_by_cc_code(df):
    ''' Generates a 'cc_code' (measure of the remaining difference between cancer
            mortality and all-cause mortality) and drops data that  that are not
            credible (cancer deaths > 70% of all-cause mortality)
    '''
    uid_cols = ['country_id'] + \
        [c for c in get_uid_cols() if c not in ['acause']]
    # Set max proportion of all-cause mortality that could possibly come from cancer
    max_pct_cancer = 0.70
    print("Entries before cc_code refinement: {}".format(len(df)))
    # Calculate cc_code as the difference between total cancer and all-cause mortality
    env = load_mortality_envelope(df['location_id'].unique().tolist(),
                                  df['age_group_id'].unique().tolist(),
                                  df['year_id'].unique().tolist())
    deaths_df = df.loc[~df['acause'].str.contains(
        "neo_leukemia_"), :]  # remove child causes
    deaths_df = deaths_df.groupby(uid_cols, as_index=False).agg(
        {'deaths': 'sum', 'pop': 'mean'}
    ).rename(columns={'deaths': 'cancer_deaths'})
    cc_df = deaths_df.merge(env, how='inner',
                            on=['location_id', 'year_id', 'sex_id', 'age_group_id'])
    cc_df['total_deaths'] = cc_df['death_rate']*cc_df['pop']
    cc_df.loc[:, ['total_deaths', 'cancer_deaths']] = \
        cc_df[['total_deaths', 'cancer_deaths']].fillna(0)
    valid_estimates = (cc_df['cancer_deaths'] <=
                       max_pct_cancer*cc_df['total_deaths'])
    cc_df = cc_df.loc[valid_estimates, :]
    cc_df['deaths'] = cc_df['total_deaths'] - cc_df['cancer_deaths']
    cc_df['acause'] = "cc_code"
    cc_df['registry_index'] = "(\'0.0.0\',)"
    cc_df['NID'] = utils.get_gbd_parameter('generic_cancer_nid')
    cc_df['dataset_id'] = 3
    cc_df = cc_df.drop(['total_deaths', 'cancer_deaths', 'death_rate'], axis=1)
    cc_df.drop_duplicates(inplace=True)
    # Attach cc_code data to main dataset and return. First subset df to only
    #   those uids with valid cc_code values, then append the full cc_code values
    # subset output to only valid cc_code
    output = df.merge(cc_df[uid_cols], how='inner')
    print("Entries after cc_code refinement: {}".format(len(output)))
    output = output.append(cc_df)  # append
    df = modeled_locations.add_sdi_quintile(df, delete_existing=True)
    print("Final entries with cc_code attached: {}".format(len(output)))
    assert not output[output.duplicated(get_uid_cols())].any().any(), \
        "Duplicate entries present at end of refine_by_cc_code"
    assert not df['deaths'].isnull().any(), \
        "Mortality estimates lost while calulating cc_code"
    return(output)


def add_CoD_variables(df):
    ''' Adds CoD-specific variables
    '''
    print("adding and formatting CoD information...")
    # Add 'national' column indicating representative status
    df = staging.add_representativeness(df)
    df.rename(columns={'representative': 'national'}, inplace=True)
    # set location information
    df = modeled_locations.add_ihme_loc_id(
        df).rename(columns={'ihme_loc_id': 'iso3'})
    df.loc[:, 'iso3'] = df['iso3'].str[:3]
    # Test output
    uid_cols = list(set(get_uid_cols()) - set('location_id')) + ['iso3']
    assert not df[df.duplicated(uid_cols)].any().any(), \
        "Duplicate values present at end of add_CoD_variables"
    return(df)


def format_CoD_variables(df):
    ''' Updates data formats to comply with CoD specifications
    '''
    print("updating variable formats...")
    #
    df.loc[df['country_id'] == df['location_id'], 'location_id'] = np.NaN
    # Ensure presence of single NID column
    df.rename(columns={'NID': 'nid_input'}, inplace=True)
    df['NID'] = df['nid_input'].apply(extract_single_nid)
    # update age categories to refelct CoD categories
    df['age'] = df['age_group_id'] + 1
    df.loc[df['age_group_id'] >= 30, 'age'] = df['age_group_id'] - 8
    df.loc[df['age_group_id'] == 235, 'age'] = 25
    # Test output
    uid_cols = list(set(get_uid_cols()) - set('location_id')) + ['iso3']
    assert not df[df.duplicated(uid_cols)].any().any(), \
        "Duplicate values present at end of add_CoD_variables"
    return(df)


def test_output(df):
    '''
    '''
    print("finalizing...")
    cod_uids = ['iso3', 'subdiv', 'location_id', 'national',
                'NID', 'sex', 'year', 'cause', 'age_group_id']
    df.rename(columns={'acause': 'cause', 'year_id': 'year', 'sex_id': 'sex'},
              inplace=True)
    df = df.drop(['dataset_id', 'registry_index', 'pop', 'country_id'],
                 axis=1, errors='ignore')
    value_cols = [c for c in df.columns if c not in ['location_id']]
    for v in value_cols:
        if df[v].isnull().any():
            print("   null values in {}".format(v))
    assert not df[value_cols].isnull().any(
    ).any(), "Null values exist in output"
    assert not df[df.duplicated(cod_uids)].any().any(), \
        "Duplicate entries exist in output"
    return(df)


def run():
    ''' Finalizes data for CoD prep, then runs CoD prep's format code
    '''
    finalized_file = utils.get_path(
        "formatted_CoD_data", process="cod_mortality")
    CoD_format_script = utils.get_path(
        "finalize_CoD_input", process="cod_mortality")
    input_file = utils.get_path("mortality_estimates", process="cod_mortality")
    df = pd.read_csv(input_file)
    # Ensure that there is a single entry by uid (data will not be collapsed
    #   after this point)
    assert not df[df.duplicated(get_uid_cols())].any().any(), \
        "Duplicate values present at input"
    df = refine_by_cc_code(df)
    df = add_subdiv(df)
    df = add_CoD_variables(df)
    df = format_CoD_variables(df)
    df = test_output(df)
    df.to_csv(finalized_file)
    pydo.do_stata(CoD_format_script, arg_list=None)
    return(df)


if __name__ == "__main__":
    run()
    print("format complete.")
