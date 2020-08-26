
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
from cancer_estimation.c_models import modeling_functions
from cancer_estimation._database import cdb_utils as cdb
import pandas as pd




def get_uid_vars():
    ''' Returns a list of the variables considered to be unique identifiers in 
            the combine_incidence process
    '''
    return(['location_id', 'year_id', 'sex_id', 'age_group_id', 'acause'])


def project_to_special_locations(df):
    ''' Uses CoD team model covariates to project data onto different location_ids
        i.e. splits india data into urban and rural
    '''
    print("project_to_special_locations is INCOMPLETE")
    return(df)


def supplement_national_estimates(df):
    ''' Combines subnational estimates to create national estimates, then
        removes redundancy. These estimates are used for validation only
    '''
    uid_cols = get_uid_vars()
    output_cols = uid_cols + ['cases', 'pop', 'dataset_id', 'NID']
    # Subset to data used to create national projections. 
    for_natnl_projections = (~df['national_registry'].eq(1) & 
                df['is_subnational'].eq(1) &  ~df['location_id'].eq(354)) 
    natnl_est = df.loc[for_natnl_projections, :]
    natnl_est.at[:,'location_id'] = natnl_est['country_id']
    natnl_est = staging_functions.combine_uid_entries(natnl_est, 
                        uid_cols = uid_cols+['is_subnational', 'national_registry'], 
                        metric_cols=['cases','pop'])
    # Preferentially keep existing data for the same uid
    df.at[:, 'existing'] = 1
    df = df.append(natnl_est)
    df.sort_values(uid_cols + ['existing'], ascending=False).reset_index()
    df.drop_duplicates(subset=uid_cols, keep="first", inplace=True)
    return(df)


def project_ihme_location_estimates(df):
    ''' For each IHME location_id, projects estimates based in the input cancer
        rates (includes generation of national estimates from subnational estimates
        if subnational estimates are not present)
    '''
    df = modeled_locations.add_country_id(df)
    df = modeled_locations.add_subnational_status(df)
    df = supplement_national_estimates(df)
    df.at[:,'rate'] = df['cases']/df['pop']
    del df['pop']
    df = df.merge(modeling_functions.load_sdi_map())
    ihme_pop = pop.load_raw_ihme_pop(df.location_id.unique())
    ihme_pop.rename(columns={'population':'pop'}, inplace=True)
    final_df = df.merge(ihme_pop)
    can_project = (final_df['sdi_quintile'].eq(5) | final_df['is_subnational'].eq(1))
    final_df.loc[can_project, 'cases'] = final_df['rate']*final_df['pop']
    assert len(final_df) == len(df), "Error during estimate projection"
    return(df)


def project_data():
    ''' Runs pipeline to combine previously-selected incidence data
        Requires incidence data that are unique by location_id-year-sex-age-acause
    '''
    input_file = utils.get_path("combined_incidence", process="cod_mortality")
    output_file = utils.get_path("projected_incidence", process="cod_mortality")
    df = pd.read_csv(input_file)
    df = project_to_special_locations(df)
    df = project_ihme_location_estimates(df)
    df.to_csv(output_file, index=False)
    print("incidence data projected")

   
if __name__=="__main__":
    project_data()