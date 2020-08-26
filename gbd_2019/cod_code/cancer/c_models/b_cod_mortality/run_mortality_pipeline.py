
'''
Description: Runs the pipeline to estimate mortality and upload into the CoD
    database. Runs all processes necessary to convert prepped incidence to mortality
            estimates, ready for the CoD prep process

'''
import pandas as pd
import cancer_estimation.py_utils.common_utils as utils 
import cancer_estimation._database.cdb_utils as cdb
import subprocess
import rate_adjustments as ra 
import upload_to_cod as cod 
from cancer_estimation.c_models.b_cod_mortality import (
    prep_incidence,
    combine_incidence,
    split_neonatal_ages,
    project_data,
    calculate_mortality,
    format_CoD_input
)
from db_queries import ( 
    get_envelope,
    get_population,
    get_cod_data,
    get_envelope,
    get_cause_metadata
)


def validate_result(df, step_name):
    ''' Validates results of any given step in the pipeline
    '''
    required_cols = ['country_id', 'location_id', 'year_id', 'sex_id',
                     'age_group_id', 'acause', 'registry_index', 'NID',
                     'dataset_id']
    for c in required_cols:
        if df[c].isnull().any():
            print("   null values in {}".format(c))
    assert not df[required_cols].isnull().any().any(), \
        "Required column(s) missing values after {}".format(step_name)
    return(df)


def get_cause_hierarchy(cause_set_id=4): 
    ''' returns current cause hierarchy
    '''
    causes_df = get_cause_metadata(cause_set_version_id = cause_set_id)
    return causes_df 


def get_pop(locset_id=8):
    ''' returns population estimates 
    '''
    d_step = utils.get_gbd_parameter('current_decomp_step')
    gbd_id = utils.get_gbd_parameter('current_gbd_round')
    yr_range = range(1980,2030) 
    yr_list = list(yr_range)
    pop_df = get_population(age_group_id=-1, location_id=-1, location_set_id=locset_id,
                            year_id=yr_list,
                            sex_id = -1,
                            decomp_step = d_step,
                            gbd_round_id = gbd_id)
    return(pop_df)


def get_env(): 
    ''' returns current gbd envelope 
    '''
    d_step = utils.get_gbd_parameter('current_decomp_step')
    gbd_id = utils.get_gbd_parameter('current_gbd_round')
    yr_range = range(1980,2030) 
    yr_list = list(yr_range)
    env_df = get_envelope(age_group_id=-1, location_id=-1, location_set_id=8,
                            year_id=yr_list,
                            sex_id = -1,
                            decomp_step = d_step,
                            gbd_round_id = gbd_id)
    env_df.rename(columns={"mean": "mean_env"}, inplace=True)
    return env_df


def run_nonzero_floor():   
    ''' enforces cause-, age-, sex-, and year-specific minimums on non-zero 
        cause fractions. It allows cause fractions to be 0, but not to be 
        arbitrarily small.
    '''
    print('running nonzero floor regression...')
    db_link = cdb.db_api()
    nr_df = pd.read_csv(utils.get_path(process='cod_mortality', 
                                    key='noise_reduced'))
    cause_df = db_link.get_table('cod_model_entity')
    cause_df = cause_df.loc[cause_df['is_active'].eq(1),['cause_id','acause']]
    cause_metadata = nr_df.merge(cause_df, how='left', on='acause')
    cause_hierarchy = get_cause_hierarchy()
    nonzero_floorer = ra.NonZeroFloorer(cause_metadata)
    df = nonzero_floorer.get_computed_dataframe(get_pop(), get_env(), cause_hierarchy)
    df.to_csv(<FILE PATH>)
    return 


def run_noise_reduction_prior(): 
    ''' calculates CC_code then runs noise reduction 
    '''
    neonat_dir = utils.get_path(process='cod_mortality', key='neonatal_split')
    df = pd.read_csv(neonat_dir)
    split_cc_df = format_CoD_input.refine_by_cc_code(df) 
    split_cc_dir = utils.get_path(process='cod_mortality',key='mortality_split_cc')
    split_cc_df.to_csv(split_cc_dir)
    print('creating noise reduction prior...')
    script_path = utils.get_path(process='cod_mortality', key='noise_reduction_prior')
    subprocess.call(['Rscript', script_path], shell=False)
    return 


def run_noise_reduction_create_metrics(): 
    ''' Noise reduce datapoints based off of prior generated 
    '''
    prior_dir = utils.get_path(process='cod_mortality',key='NR_prior')
    prior_df = pd.read_csv(prior_dir)
    del prior_df['sample_size'] # remove sample_size 
    prior_df_cc = format_CoD_input.refine_by_cc_code(prior_df) 
    prior_df_cc.to_csv(prior_dir)   
    print('Creating noise reduction posterior...')
    script_path = utils.get_path(process='cod_mortality', key='noise_reduction_posterior')
    subprocess.call(['Rscript', script_path], shell=False)


def main(cod_mortality_version_id):
    ''' Runs all processes necessary to convert prepped incidence to mortality
            estimates, ready for the CoD prep process
    '''
    """
    print("Starting pipeline...")
    validate_result(prep_incidence.run(cod_mortality_version_id), "prep_incidence") 
    validate_result(combine_incidence.run(), "combine_incidence")
    validate_result(project_data.project_incidence(), "project_data")
    validate_result(calculate_mortality.run(
        cod_mortality_version_id), "calculate_mortality")
    """
    # split 0-4 age group 
    split_neonatal_ages.run_neonatal_split()

    # add cc_code and noise reduce data 
    run_noise_reduction_prior()
    run_noise_reduction_create_metrics() 
    # Use the noise reduction result to run nonzero floor regressions 
    print('finished noise reducing...!')
    run_nonzero_floor()
    import pdb; pdb.set_trace()
    # format CoD input and upload 
    format_CoD_input.run()
    cod.run_cod_upload('testcod')
    

if __name__ == "__main__":
    import sys
    main(int(sys.argv[1]))