
'''
Description: Runs the pipeline to estimate mortality and upload into the CoD
    database. Runs all processes necessary to convert prepped incidence to mortality
            estimates, ready for the CoD prep process

Contributors: USERNAME
'''
import numpy as np
import pandas as pd
import os
import argparse
import cancer_estimation.py_utils.common_utils as utils 
import cancer_estimation.py_utils.gbd_cancer_tools as gct
import cancer_estimation._database.cdb_utils as cdb
import cancer_estimation.py_utils.cluster_tools as ct
import subprocess
import rate_adjustments as ra 
from elmo import  ( 
    upload_bundle_data,
    save_bundle_version, 
    get_bundle_version, 
    save_crosswalk_version
)
import cancer_estimation.c_models.b_cod_mortality.upload_to_cod as cod
from cancer_estimation.c_models.b_cod_mortality import (
    compare_upload,
    prep_incidence,
    combine_incidence,
    split_neonatal_ages,
    project_data,
    calculate_mortality,
    format_CoD_input,
    upload_to_cod as cod
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
    causes_df = get_cause_metadata(cause_set_id = cause_set_id)
    return causes_df 


def get_pop(loc_list):
    ''' returns population estimates 
    '''
    d_step = utils.get_gbd_parameter('current_decomp_step')
    gbd_id = utils.get_gbd_parameter('current_gbd_round')
    yr_range = range(1980,2030) 
    yr_list = list(yr_range)
    pop_df = get_population(age_group_id=-1, location_id=loc_list, location_set_id=8,
                            year_id=yr_list,
                            sex_id = -1,
                            decomp_step = d_step,
                            gbd_round_id = gbd_id)
    return(pop_df)


def get_env(loc_list): 
    ''' returns current gbd envelope 
    '''
    d_step = utils.get_gbd_parameter('current_decomp_step')
    gbd_id = utils.get_gbd_parameter('current_gbd_round')
    yr_range = range(1980,2030) 
    yr_list = list(yr_range)
    env_df = get_envelope(age_group_id=-1, location_id=loc_list, location_set_id=8,
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
    location_list = nr_df['location_id'].unique().tolist()

    cause_df = db_link.get_table('cod_model_entity')
    cause_df = cause_df.loc[cause_df['is_active'].eq(1),['cause_id','acause']]
    cause_metadata = nr_df.merge(cause_df, how='left', on='acause')
    cause_hierarchy = get_cause_hierarchy()
    nonzero_floorer = ra.NonZeroFloorer(cause_metadata) 
    df = nonzero_floorer.get_computed_dataframe(get_pop(location_list), get_env(location_list), cause_hierarchy)
    df.to_csv(utils.get_path(process='cod_mortality', key='nonzero_floor'))
    return 


def run_noise_reduction_prior(): 
    ''' calculates CC_code then runs noise reduction 
    '''
    neonat_dir = utils.get_path(process='cod_mortality', key='neonatal_split')
    df = pd.read_csv(neonat_dir)
    split_cc_df = format_CoD_input.refine_by_cc_code(df) 
    split_cc_dir = utils.get_path(process='cod_mortality',key='mortality_split_cc')
    split_cc_df.to_csv(split_cc_dir)

    nr_prior_output = utils.get_path(process='cod_mortality',key='NR_prior')
    print('creating noise reduction prior...')
    script_path = utils.get_path(process='cod_mortality', key='noise_reduction_launcher')
    success = subprocess.call(args=['Rscript', script_path], shell=False)
    assert os.path.isfile(nr_prior_output), 'noise reduction priors were not compiled' 
    return 


def run_noise_reduction_create_metrics(): 
    ''' Noise reduce datapoints based off of prior generated 
    '''
    prior_dir = utils.get_path(process='cod_mortality',key='NR_prior')
    prior_cc_dir = utils.get_path(process='cod_mortality', key='NR_prior_cc')
    prior_df = pd.read_csv(prior_dir)

    prior_df_cc = format_CoD_input.refine_by_cc_code(prior_df) 
    prior_df_cc.to_csv(prior_cc_dir)   
    print('Creating noise reduction posterior...')
    script_path = utils.get_path(process='cod_mortality', key='noise_reduction_posterior')
    subprocess.call(['Rscript', script_path], shell=False)
    return


def load_nids() -> pd.DataFrame: 
    ''' 
    '''
    def _attach_registry_index(nid_df : pd.DataFrame) -> pd.DataFrame: 
        '''
        '''
        can_db = cdb.db_api('cancer_db') 
        reg = can_db.get_table('registry')[['registry_index','registry_name','location_id','country_id']]
        fin = pd.merge(nid_df, reg, on=['location_id','country_id','registry_name'], how='left', indicator=True)
        assert all(fin['_merge'].eq('both')), 'some registries werent merged!' 
        del fin['_merge']
        return fin 

    def _apply_IND_corrections(in_df : pd.DataFrame) -> pd.DataFrame: 
        ''' Append additional IND registries to dataframe. This is due to splitting of India registries that occurs and 
            post-split registry_index values do not exist in the nqry.
        '''
        
        df = pd.DataFrame()
        queue_id_list = [20, 21, 49, 51, 48, 149, 150, 182, 221, 229, 230, 231, 257, 305, 661]
        registry_list = ['163.43882.2','163.43888.2','163.43918.2','163.43892.1','163.43894.1','163.43900.1',
            '163.43910.3','163.43910.2','163.43924.2','163.43928.1','163.43930.1','163.43936.1','163.43892.2',
            '163.43928.2','163.43893.1','163.43895.1','163.43903.1','163.43929.1','163.43931.1','163.43939.1',
            '163.43873.4','163.43873.3','163.43873.2','163.43909.2', '163.43909.3','163.43909.4','163.43898.3',
            '163.43934.3','163.43898.4','163.43936.4','163.43934.4','163.43898.2','163.43934.2','163.43901.1',
            '163.4867.3','163.4867.2','163.4867.4', '163.43890.2','163.43924.3','163.43927.4','163.43937.2'
        ]
        tmp = in_df.loc[in_df['queue_id'].isin(queue_id_list) & in_df['registry_index'].str.startswith('163.'), ]
        # for each queue_id, transform each list of registries, replicating index values 
        # Example, queue_id 20 will have nrows* len(registry_list) additional rows created
        for q in queue_id_list: 
            work_df = tmp.loc[tmp['queue_id'].eq(q), ]
            for i in work_df.index.values: 
                work_df.at[i,'registry_index'] = registry_list 
            work_df = work_df.explode('registry_index') 
            df = df.append(work_df)
        df = df.append(in_df)
        
        return df

    # load nqry database 
    can_db = cdb.db_api('cancer_db') 
    nqry = can_db.get_table('nqry')
    nqry = nqry.loc[nqry['is_best'].eq(1)  & ~nqry['nid'].eq(0), 
        ['nid','underlying_nid','queue_id','registry_index','year_start','year_end','data_type_id']]

    missing_nids = pd.read_csv('FILEPATH/missing_nids_filled.csv')
    missing_nids = missing_nids.loc[~(missing_nids['new_nid'].isnull()) & ~(missing_nids['queue_id'].eq(137)), ['location_id','country_id','new_nid','new_underlying_nid','queue_id','registry_name','year_start','year_end','data_type_id']]
    
    missing_nids.rename(columns={'new_nid':'nid',
                    'new_underlying_nid':'underlying_nid'}, inplace=True)
    missing_nids = _attach_registry_index(missing_nids)

    # attach registry_index 
    nid_df_reg = missing_nids.append(nqry)
 
    # expand entries where data_type_id = 1 to have both 2 and 3 (inc and mor respectively)
    expand_needed = nid_df_reg.loc[nid_df_reg['data_type_id'].eq(1), ]
    expand_needed['data_type_id'] = 2 
    nid_df_reg = nid_df_reg.append(expand_needed)
    nid_df_reg = nid_df_reg.loc[~nid_df_reg['data_type_id'].isin([1,3]), ].reset_index(drop=True)

    # expand dataframe where rows have year ranges (i.e year_start != year_end)
    nid_df = gct.fill_from_year_range(nid_df_reg)

    nid_df.drop_duplicates(subset=['registry_index','queue_id','data_type_id','year_start','year_end'], inplace=True) # NOTE: order of append matters in line 183 (missing_nids should be first)
    nid_df.rename(columns={'year_start': 'year_id'}, inplace=True)
    nid_df = _apply_IND_corrections(nid_df)

    # replace entries where underlying nid = 0 
    nid_df.loc[nid_df['underlying_nid'].eq(0), 'underlying_nid'] = np.nan

    # replace invalid underlying nid values with NAN
    # these underlying NIDs cannot be found in GHDx
    invalid_underlying_nids = [401773, 169837, 150561, 400594, 400602, 400604, 400607,400610, 113821]
    nid_df.loc[nid_df['underlying_nid'].isin(invalid_underlying_nids), 'underlying_nid'] = np.nan

    # underlying_nid and nid flipped for queue_id 270 
    nid_df['nid'] = np.where(nid_df['queue_id'].eq(270), nid_df['underlying_nid'], nid_df['nid'])

    assert any(~nid_df['nid'].isna() | ~nid_df['nid'].eq(0)), 'missing NIDs exist!'
    return(nid_df[['nid','underlying_nid','registry_index','queue_id','data_type_id','year_id']])


def format_bundle(df : pd.DataFrame) -> pd.DataFrame: 
    ''' Formats data according to shape validation of GBD custom bundles 
    '''
    def _attach_queue_id(df : pd.DataFrame) -> pd.DataFrame:
        '''
        '''
        can_db = cdb.db_api('cancer_db') 
        dset = can_db.get_table('dataset')[['queue_id','dataset_id']]

        fin_df = pd.merge(df, dset, on='dataset_id', how='left', indicator=True)
        assert all(fin_df['_merge'].eq('both'))
        del fin_df['_merge']
        return fin_df 

    # attach queue_id and year_id to selected_incidence data 
    df = gct.add_year_id(_attach_queue_id(df)) 

    # keep only required columns needed for bundles 
    df.drop(labels=['NID','underlying_nid'], axis=1, inplace=True)

    # remove queue_id 277, year 1990 incidence data 
    # these data points do not exist in raw, and was included due to errors in formatting/prep
    df = df.loc[~((df['queue_id'].eq(277)) & (df['year_id'].eq(1990))), ]

    # Custom bundle required columns
    req_cols = ['acause','location_id','sex','measure','nid','year_start','year_end','seq','underlying_nid']
    nids = load_nids()

    fin = pd.merge(df, nids, on=['queue_id','registry_index','year_id'], how='left', indicator=True)
    assert all(fin['_merge'].eq('both')) #test merge result 
    
    # attach any other required columns that are required from CC 
    fin = fin.reset_index(drop=True)
    fin['measure'] = 'incidence'
    fin['seq'] = fin.index
    fin['sex'] = 'Male'
    fin.loc[fin['sex_id'].eq(2), 'sex'] = 'Female'
    fin = fin[req_cols]

    return fin


def save_bundle_data(df : pd.DataFrame, mortality_version : int) -> None: 
    ''' Saves bundle inputs as excel files for later upload 
    '''
    work_dir = utils.get_path(process='cod_mortality', key='bundle_workspace')
    path = '{}/bundle_inputs_v{}'.format(work_dir, mortality_version)
    utils.ensure_dir(path)
    cause_list = df['acause'].unique().tolist() 
    for this_cancer in cause_list: 
        tmp = df.loc[df['acause'].eq(this_cancer), ]
        tmp.to_excel('{}/{}_bundle_input.xlsx'.format(path, this_cancer), index=False, sheet_name='extraction')
    return

    

def main(cod_mortality_version_id, compare_step, upload_bundles):
    ''' Runs all processes necessary to convert prepped incidence to mortality
            estimates, ready for the CoD prep process
    '''
    print("Starting pipeline...")
    validate_result(prep_incidence.run(cod_mortality_version_id), "prep_incidence")

    if upload_bundles: 
        selected_incidence = utils.get_path(
        "selected_incidence", process="cod_mortality")
       
        bundle_input = pd.read_csv('FILEPATH/prepped_incidence.csv') 
        
        # format and save cancer-specific bundle inputs 
        bundle_formatted = save_bundle_data(format_bundle(bundle_input)) 

        # submit jobs        
        cause_list = bundle_formatted['acause'].unique().tolist()
        for this_cancer in cause_list: 
            jobs = ct.create_jobs(script_path='{}/bundles_and_crosswalks.py'.format(utils.get_path(process='common', key='gbd_utils')),
                                    memory_request=10,
                                    script_args='mortality {} {}'.format(this_cancer, cod_mortality_version_id),
                                    project_name='cancer',
                                    job_header='can_bundle_{}'.format(this_cancer),
                                    shell='gbd_py_shell')
            jobs.launch[0]['job'].launch()
        print('all bundle jobs launched!')
    validate_result(combine_incidence.run(), "combine_incidence")
    validate_result(project_data.project_incidence(), "project_data")
    
    validate_result(calculate_mortality.run(
        cod_mortality_version_id), "calculate_mortality")
    # split 0-4 age group 
    split_neonatal_ages.run_neonatal_split()
    # adding cc_code is within each of noise_reduction functions 
    # cc_code is needed to get the correct sample_size 
    run_noise_reduction_prior()
    run_noise_reduction_create_metrics() 

    # Use the noise reduction result to run nonzero floor regressions 
    print('finished noise reducing...!')
    run_nonzero_floor()

    # format CoD input and upload 
    format_CoD_input.run()
    compare_upload.run('nzf', compare_step, 20, 20)
    cod.run_cod_upload('testcod')


def parse_args(): 
    parser = argparse.ArgumentParser() 
    parser.add_argument('-rid', '--run_id',
                        type=int,
                        help='cod mortality version id')
    parser.add_argument('-compare_version',
                        type=str, default='',
                        help='another version or run to compare to (default no comparing)')
    parser.add_argument('-upload_bundles',
                        type=bool, default=False,
                        help='Upload bundles and create bundle version or not')
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    import sys
    args = parse_args() 
    main(cod_mortality_version_id =args.run_id, 
        compare_step=args.compare_version,
        upload_bundles=args.upload_bundles) 