
'''
Description: Applies the IHME population to the incidence rate to create a
    projected estimate matching the IHME coverage population

How To Use:
'''


from cancer_estimation.py_utils import (
    common_utils as utils,
    data_format_tools as dft,
    modeled_locations
)
from cancer_estimation.a_inputs.a_mi_registry import (
    populations as pop,
    mi_dataset
)
from cancer_estimation.b_staging import staging_functions
from cancer_estimation._database import cdb_utils as cdb
import numpy as np
import pandas as pd


def get_uid_columns():
    return(['location_id', 'year_id', 'sex_id', 'age_group_id', 'acause'])


def supplement_national_estimates(df):
    ''' Combines subnational estimates to create national estimates, then
        removes redundancy. These estimates are used for validation only
    '''
    print("      generating national estimations...")
    uid_cols = get_uid_columns()
    output_cols = uid_cols + ['cases', 'pop', 'dataset_id', 'NID']
    combine_uids = uid_cols+['is_subnational', 'national_registry']
    # Subset to data used to create national projections. 
    to_combine = (df['is_subnational'].eq(1) &
                  ~df['national_registry'].eq(1) &
                  ~df['location_id'].eq(354))
    est_df = df.loc[to_combine, :]
    est_df.loc[:, 'location_id'] = est_df['country_id']
    est_df = staging_functions.combine_uid_entries(est_df,
                                                   uid_cols=combine_uids,
                                                   metric_cols=['cases', 'pop'])
    est_df['country_id'] = est_df['location_id']
    # Add sdi quintile information back to new estimates
    est_df = modeled_locations.add_sdi_quintile(est_df, delete_existing=True)
    # Preferentially keep existing data for the same uid
    df = df.append(est_df)
    df.sort_values(uid_cols + ['national_registry'],
                   ascending=False).reset_index()
    df = df.drop_duplicates(subset=uid_cols, keep="first")
    return(df)


def load_ihme_pop(location_list):
    '''
    '''
    ihme_pop = pop.load_raw_ihme_pop(location_list)
    ihme_pop.rename(columns={'population': 'pop'}, inplace=True)
    return(ihme_pop)


def project_incidence():
    ''' For each IHME location_id, projects estimates based in the input cancer
        rates
        Includes generation of national estimates from subnational estimates
        where national estimates are not present 
    '''
    print("   projecting data to ihme demographic specifications...")
    output_file = utils.get_path(
        "projected_incidence", process="cod_mortality")
    input_file = utils.get_path("combined_incidence", process="cod_mortality")
    pop_uids = [c for c in get_uid_columns() if c != 'acause']
    df = pd.read_csv(input_file)
    # define subset that can be projected to the IHME population
    df = modeled_locations.add_subnational_status(df)
    df = supplement_national_estimates(df)
    # Ensure validity of sdi_quintile
    df = modeled_locations.add_sdi_quintile(df, delete_existing=True)
    # Calculate rate of input
    df.loc[:, 'rate'] = df['cases']/df['pop']
    df['registry_pop'] = df['pop']
    # Mark data to be projected
    project_to_ihme = (df['sdi_quintile'].eq(5))
    df_sdi5 = df.loc[project_to_ihme, :].copy()
    df_other = df.loc[~project_to_ihme, :].copy()
    # Add IHME population to applicable uids
    del df_sdi5['pop']
    ihme_pop = load_ihme_pop(list(df.loc[project_to_ihme, 'location_id'].unique()))
    df_sdi5 = df_sdi5.merge(ihme_pop)
    # Homogenize population by group where not applying IHME populations
    df_other = staging_functions.homogenize_pop(df_other, uid_cols=pop_uids)
    output = df_other.append(df_sdi5)
    # reindex to allow multiplying series
    # create new column index, then set that as the new index
    output['index'] = np.arange(len(output))
    output = output.set_index('index')
    # Broadcast rates to the final population estimate for all locations
    output.loc[output['pop'].notnull() & output['rate'].notnull() & ~output['rate'].eq(np.inf),
               'cases'] = output['rate'] * output['pop']
    # Drop registry-specific tags
    output = output.drop(['national_registry', 'full_coverage', 'is_subnational',
                          'registry_pop'], axis=1, errors='ignore')
    assert not output.loc[output.duplicated(get_uid_columns()), :].any().any(), \
        "Duplicates exist after projection"
    assert not output['pop'].isnull().any(), "Missing population data"
    assert len(output) == len(df), "Error during estimate projection"
    output.to_csv(output_file, index=False)
    print("   data projected.")
    return(output)


def project_to_special_locations(df):
    ''' Uses CoD team model covariates to project data onto different location_ids
        i.e. splits india data into urban and rural
    '''
    print("project_to_special_locations is INCOMPLETE")
    return(df)


if __name__ == "__main__":
    project_incidence()
