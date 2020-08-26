
'''
Description: Prepares data for the cod_mortality modeling pipeline
How To Use:
'''
import pandas as pd
from cancer_estimation.py_utils import (
    common_utils as utils,
    modeled_locations,
    pydo,
    gbd_cancer_tools as gct,
    get_version as gv
)
from cancer_estimation.b_staging import staging_functions
from cancer_estimation.a_inputs.a_mi_registry import mi_dataset


def get_uid_vars():
    ''' Returns a list of the variables considered to be unique identifiers
    '''
    return(['location_id', 'year_id', 'sex_id', 'age_group_id', 'acause'])


def _add_location_id(df):
    '''
    '''
    # split india and create location_ids based on subnationals, not country
    india = (df['registry_index'].str.startswith("163."))
    india_df = df[india].copy()
    india_df.loc[:, 'location_id'] = india_df['registry_index'].str.split(
        ".").apply(lambda x: x[1]).astype(int)
    # split UKR data to subnationals instead of country
    ukraine = (df['registry_index'].str.startswith("63."))
    ukraine_df = df[ukraine].copy()
    ukraine_df.loc[:, 'location_id'] = ukraine_df['registry_index'].str.split(
        ".").apply(lambda x: x[1]).astype(int)
    other_df = df[~(india | ukraine)]
    other_df = mi_dataset.add_location_ids(other_df)
    df = other_df.append(india_df)
    df = df.append(ukraine_df)
    assert not df['location_id'].isnull().any(), \
        "Error adding location_ids: null values"
    assert not (df['location_id'] == 1).any(), \
        "Error adding location_ids: some registries mapped to \'Global\'"
    return(df)


def format_incidence_input(df):
    ''' Returns dataframe with added year and location information, plus adjusted
            metric data (representative of the median year where year_span > 1)
    '''
    assert not df['NID'].isnull().any(), "Missing NIDs from input"
    #
    full_uids = get_uid_vars() + ['dataset_id', 'registry_index', 'NID']
    df = _add_location_id(df)
    # add year_id column
    df = gct.add_year_id(df).rename(
        columns={'year': 'year_id', 'sex': 'sex_id'})
    assert not (df['location_id'] == 1).any()
    # Add age_group_id
    df = df.loc[df['age'] != 1, :]
    df = gct.age_group_id_from_cancer_age(df)
    del df['age']
    # Format data to represent single years (median year)
    df.loc[:, 'year_span'] = df['year_end'] - df['year_start'] + 1
    df.loc[:, 'cases'] = df['cases']/df['year_span']
    del df['year_span']
    # Test output
    assert not df['NID'].isnull().any(), "Missing NIDs"
    assert not (df['location_id'] == 1).any()
    return(df)


def add_required_columns(df):
    ''' Add columns whose information is required throughout the process
            These values must be populated for all uids throughout the pipeline
    '''
    # Add country_id and SDI_quintile
    df = modeled_locations.add_country_id(df)
    df.loc[df['registry_index'].str.startswith("163."), 'country_id'] = 163
    df.loc[df['dataset_id'].eq(391) & df['registry_index'].str.startswith("63."), 'location_id'] = 63
    df = modeled_locations.add_sdi_quintile(df)
    # Mark data that are modeled subnationally. Ensure that
    #   (location_id == country_id) for uids that are modeled only nationally
    df = modeled_locations.add_subnational_status(df)
    df = staging_functions.add_coverage_metadata(df)
    df.loc[~df['is_subnational'].eq(1), 'location_id'] = df['country_id']
    for col in ['national_registry', 'full_coverage']:
        df.loc[df[col].isnull(), col] = 0
    return(df)


def update_populations(df):
    ''' Homogenizes population, where present (fills missing values within a uid),
            and adds IHME population where acceptable
    '''
    import numpy as np 
    # Homogenize registry population and add population where missing (and
    #   permitted)
    # convert to rate to preserve information where one source has a much larger
    #   sample size than another
    df['rate'] = df['cases'] / df['pop']
    df = staging_functions.homogenize_pop(df)
    df.loc[(df['pop'].notnull() & df['rate'].notnull() & ~df['rate'].eq(np.inf)),
           'cases'] = df['rate']*df['pop']
    # Add missing population where acceptable
    df = staging_functions.add_population(df)
    return(df)


def restrict(df):
    ''' drop if no population. Placeholder for application of other restrictions
    '''
    # keep only data with population
    df = df.loc[df['pop'].notnull() |
                (df['sdi_quintile'].eq(5) & df['full_coverage'].eq(1)), :]
    df = df.loc[df['cases'].notnull(), :]
    return(df)


def run(cod_mortality_version_id):
    ''' Runs drop_and_check, then prepares output for cod_mortality model pipeline
    '''
    drop_and_check = utils.get_path(
        "drop_and_check_cod", process="cod_mortality")
    selected_incidence = utils.get_path(
        "selected_incidence", process="cod_mortality")
    prepped_incidence = utils.get_path(
        "prepped_incidence", process="cod_mortality")
    print("Selecting incidence data...")
    inc_id = gv.get_inc_version(cod_mortality_version_id)
    #pydo.do_stata(drop_and_check, arg_list=[inc_id])
    print("Formatting incidence for pipeline...")
    df = pd.read_stata(selected_incidence)
    df = format_incidence_input(df)
    df = add_required_columns(df)
    df = update_populations(df)
    df = restrict(df)
    df.to_csv(prepped_incidence, index=False)
    return(df)


if __name__ == "__main__":
    import sys 
    run(int(sys.argv[1]))
