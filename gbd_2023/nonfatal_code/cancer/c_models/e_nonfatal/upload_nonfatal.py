#!  /ihme/code/central_comp/miniconda/envs/gbd_env/bin/python
# shebang ensures that code is run on gbd_env
'''
Description: Processes used to upload final results from the nonfatal model
Contents :
How To Use:
Note: requires a minimum of 45 slots (90GB)
Contributors: INDIVIDUAL_NAME, INDIVIDUAL_NAME 
'''
from cancer_estimation.py_utils import (
    common_utils as utils,
    pandas_expansions as pe,
    cluster_tools
)
import cancer_estimation._database.cdb_utils as cdb 
import cancer_estimation.c_models.e_nonfatal.nonfatal_dataset as nd
from cancer_estimation.c_models import epi_upload
import os
import numpy as np
from sys import argv
import pandas as pd
import glob
from datetime import datetime
from save_results import save_results_epi
from db_queries import get_location_metadata
from elmo import ( 
        upload_bundle_data,
        save_bundle_version,
        get_bundle_version,
        save_crosswalk_version
)


def get_notes(cnf_model_version_id : int) -> str:
    ''' Returns the notes for the given cnf_model_version_id 
    '''
    record = nd.get_run_record(cnf_model_version_id)
    notes = record.at[0, 'notes']
    version_info = "cnf_model_version_id {}".format(cnf_model_version_id)
    print("{}, {}".format(version_info, notes))
    return("{}, {}".format(version_info, notes))


def which_model_type(me_id : int) -> str:
    ''' Returns the cancer_model_type entry for the modelable_entity_id
    '''
    me_table = nd.load_me_table()
    model_type = me_table.loc[me_table['modelable_entity_id'] == me_id,
                              'cancer_model_type'].item()
    return(model_type)


def load_custom_model_metadata(me_id : int, is_estimation_yrs : bool) -> dict:
    ''' Returns a dictionary of the upload metadata for the given me_id
    '''
    me_table = nd.load_me_table()
    me_table = me_table.loc[me_table['modelable_entity_id'] == me_id, :]
    me_tag = me_table.loc[:, 'me_tag'].item()
    acause = me_table.loc[:, 'acause'].item()
    print("    determining model metatdata for {} {}...".format(
                acause, me_tag))
    # If running for epi upload (procedure_sequelae) add epi-specific info and
    #       return
    if me_tag == "procedure_sequelae":  # get epi upload info and return
        description = "incidence of procedures beyond ten years"
        bundle_id = int(me_table.loc[:, 'bundle_id'].item())
        return({'description': description, 'acause': acause, 'me_tag': me_tag,
                'bundle_id': bundle_id})
    else:
        pass
    # load the description and the measure_id(s)
    description = "{} prevalence".format(acause)
    measure_ids = [utils.get_gbd_parameter("prevalence_measure_id")]
    # Add diagnosis and treatment info if applicable
    if me_tag == "primary_phase":
        description += " and incidence"
        measure_ids.append(utils.get_gbd_parameter("incidence_measure_id"))

    # create instance of nonfatal dataset class
    # use the class to retrieve metadata for upload 
    this_step = nd.nonfatalDataset("upload", me_id)
    years = nd.get_expected_years(is_estimation_yrs, this_step)
    age_sex_dict = nd.get_expected_ages_sex(acause)
    sexes = age_sex_dict['expected_sex']
    ages = age_sex_dict['expected_ages']
    metadata = {'measure_ids': measure_ids, 'description': description,
                'years': years, 'sexes': sexes, 'ages': ages,
                'acause': acause, 'me_tag': me_tag}
    # verify the result truly has correct values
    verify_metadata_dict(acause, metadata) 
    return(metadata)


def verify_metadata_dict(acause, metadata : dict) -> None: 
    ''' Accepts a dictionary with NF metadata information and will run some checks
        based off what is expected for a given acause
    '''
    # verify measure_ids 
    if metadata['me_tag'] == "primary_phase": 
        assert metadata['measure_ids'] == [5,6] 
    else: 
        assert metadata['measure_ids'] == [5] 
    
    # verify sex and age ids  
    age_sex_dict = nd.get_expected_ages_sex(acause)
    assert (set(metadata['sexes']) - set(age_sex_dict['expected_sex']) == set() & 
        set(age_sex_dict['expected_sex']) - set(metadata['sexes']) == set())
    assert (set(age_sex_dict['expected_ages']) - set(metadata['ages']) == set() & 
        set(metadata['ages']) - set(age_sex_dict['expected_ages']) == set()) 
    return 


def upload_dismod_input(me_id : int, cnf_model_version_id : int, is_estimation_yrs : bool) -> pd.DataFrame:
    ''' Determine
    '''
    # Find filepath
    upload_notes = get_notes(cnf_model_version_id)
    gbd_round = utils.get_gbd_parameter('current_gbd_name')
    gbd_id = utils.get_gbd_parameter('current_gbd_round')
    d_step = 'iterative' #utils.get_gbd_parameter('current_decomp_step')
    metadata = load_custom_model_metadata(me_id, is_estimation_yrs)
    acause = metadata['acause']
    bundle_id = metadata['bundle_id']
    data_dir = "{}/{}".format(nd.get_folder('dismod_inputs',
                                            acause), bundle_id)
    bundle_dir = "{}/{}/{}/01_input_data/01_nonlit".format(
        utils.get_path('epi_bundle_staging', process="cancer_model"),
        acause, bundle_id)
    upload_file = "{}/{}_epi_upload_{}.xlsx".format(
        bundle_dir, bundle_id, utils.display_datestamp())
    # Load files as one dataframe

    print("Concatenating files and saving upload data...")
    dismod_inputs = glob.glob(data_dir + "/*.csv")
    list_files = [i for i in os.listdir(data_dir) if '0_' in i]
    df = pd.concat([pd.read_csv('{}/{}'.format(data_dir, i)) for i in list_files])
    df['file_path'] = upload_file
    df['site_memo'] = df['site_memo'] + ", {}".format(upload_notes) 
    df['is_outlier'] = 0
    df['underlying_nid'] = np.nan
    df['uncertainty_type_value'] = 95
    df['effective_sample_size'] = np.nan
    df['input_type']=np.nan
    df['design_effect'] = np.nan
    df['{}_location_year'.format(d_step)] = 'Decomp {} update'.format(d_step)
    df.reset_index(inplace=True, drop=True) 
    del df['seq']
    df['seq'] = df.index
    del df['outlier_type_id']
    df.to_excel(upload_file, sheet_name='extraction')
    
    upload_result = upload_bundle_data(bundle_id=bundle_id, 
                        filepath=upload_file)
    save_result = save_bundle_version(bundle_id=bundle_id)
    b_vers_id = save_result['bundle_version_id'] 
    print('bundle version id for this model is...{}'.format(int(b_vers_id)))
    crosswalk_input = get_bundle_version(bundle_version_id=int(b_vers_id))
    crosswalk_input['crosswalk_parent_seq'] = crosswalk_input['seq']
    crosswalk_input.to_excel(upload_file, sheet_name='extraction')
    crosswalk_result = save_crosswalk_version(int(b_vers_id),
                            data_filepath=upload_file, 
                            description='Decomp {}, {} update'.format(d_step,
                                                                    gbd_round))
    return(crosswalk_result)


def compile_me(directory): 
    '''
    '''
    print('compiling model results for all locations...')
    # get all output files for a given measure_id 
    list_files = glob.glob(directory + "/*.csv")
    if not any(list_files):
        raise AssertionError("No files to upload for this modelable_entity_id")
    combined_csv = pd.concat([pd.read_csv('{}'.format(i)) for i in list_files])
    return(combined_csv)


def get_bundle_id(acause): 
    '''
    '''
    both_sex_concord_id = 3
    release_id = utils.get_gbd_parameter('current_release_id')
    mi_table = pd.read_csv('{}/mir_model_entity.csv'.format(utils.get_path(process='nonfatal_model', key='database_cache')))
    bundle_ids = mi_table.loc[mi_table['release_id'].eq(release_id) & 
                            mi_table['acause'].eq(acause) &
                            mi_table['mir_process_type_id'].ne(both_sex_concord_id), 'mir_bundle_id']
    if len(bundle_ids) == 0:
        raise AssertionError(f"No bundle id found for {acause}. Check that the entries in the 'mir_model_entity' table are correct for release_id {release_id}.")
    if len(bundle_ids) > 1:
        raise AssertionError(f"More than one bundle id found for {acause} Check that the entries in the 'mir_model_entity' table are correct for this cause for release_id {release_id}.")
    bundle_id = int(bundle_ids)
    return(bundle_id)


def get_crosswalk_version_id(acause, cnf_model_version_id): 
    pass
    '''
    # load cnf_model_upload table 
    upload_tbl = pd.read_csv('{}/cnf_model_upload.csv'.format(utils.get_path(process='nonfatal_model', key='database_cache')))

    # check for computational total for the version_id acause 
    sub_tbl = upload_tbl.loc[upload_tbl['cnf_upload_description'].eq('{}, computational_total'.format(acause)) & 
                upload_tbl['cnf_model_version_id'].eq(cnf_model_version_id), 'crosswalk_version_id']

    # if previous or failed uploads exist, take the max 
    final_id = sub_tbl.max()
    return(final_id)'''

     
def get_crosswalk_version(acause : str, cnf_id : int): 
    ''' 
    This function will return you the appropriate crosswalk_version_id for a 
        given cancer cause. 
        generated during MIR modeling step
    '''

    # get mir_model_version for this given run 
    cnf_vers_tbl = pd.read_csv('{}/cnf_model_version.csv'.format(utils.get_path(process='nonfatal_model', key='database_cache')))
    cnf_vers_tbl = cnf_vers_tbl.loc[cnf_vers_tbl['cnf_model_version_id'].eq(cnf_id), 'mir_model_version_id']
    mir_vers = int(cnf_vers_tbl)
    
    gbd_name = utils.get_gbd_parameter('current_gbd_name')
    crosswalk_dir = f'FILEPATH{gbd_name}/c_models/a_mi_ratio/bundle_crosswalk_ids/v{mir_vers}'
    # assert that file exists. otherwise, error out
    assert os.path.isfile('{}/{}.csv'.format(crosswalk_dir, acause)), \
        f'A crosswalk version was not created for {acause}. Please create a crosswalk version \
            by using c_models/a_mi_ratio/00_create_bundle_versions.r' 

    # load file and get crosswalk version 
    df = pd.read_csv(f'{crosswalk_dir}/{acause}.csv')
    crosswalk_id = int(df['crosswalk_version_id'])

    return(crosswalk_id)


def upload_epi_model(me_id : int, cnf_model_version_id : int, is_estimation_yrs : bool) -> pd.DataFrame:
    num_draws = nd.nonfatalDataset().num_draws
    metadata = load_custom_model_metadata(me_id, is_estimation_yrs)
    #this_step = nd.nonfatalDataset("upload", me_id)
    #success_file = this_step.get_output_file('upload')
    acause = metadata['acause']
    upload_notes = get_notes(cnf_model_version_id)
    model_description = " ".join([metadata['description'], upload_notes])
    print("uploading {}...".format(metadata['description']))
    data_dir = nd.get_folder('final_results', acause) + '/' + str(me_id)
    release_id = utils.get_gbd_parameter('current_release_id')
    
    # GBD2020 additional framework added: using a bundle_id, create a 
    # crosswalk_version_id for an acause for the computational phase. 
    # This id will then be used for all other phases for the same acause. 
    bundle_id = get_bundle_id(acause)
    c_walk_vers_id = get_crosswalk_version(acause, cnf_model_version_id)
    success_df = save_results_epi(modelable_entity_id=me_id,
                                input_dir=data_dir,
                                bundle_id=bundle_id,
                                crosswalk_version_id=c_walk_vers_id,
                                metric_id=3,
                                n_draws=num_draws,
                                year_id=metadata['years'],
                                sex_id=metadata['sexes'],
                                description=model_description,
                                measure_id=metadata['measure_ids'],
                                input_file_pattern="{measure_id}_{location_id}.csv",
                                mark_best=True,
                                release_id=release_id)
    success_df['crosswalk_version_id'] = c_walk_vers_id
    return(success_df)


def upload_failed():
    '''
    '''
    raise SystemExit("upload failed. error during upload.")


def manage_model_upload(me_id, cnf_model_version_id, is_resubmission, is_estimation_yrs):
    ''' Determines the model type than runs the upload function for the me_id
        -- Inputs
            me_id : a valid modelable_entity_id or bundle_id
            cnf_model_version_id : 
    '''
    #assert (nd.load_me_table())['modelable_entity_id'].eq(me_id).any(), \
    #    "Upload is currently diabled for modelable_entity_id %i" % me_id
    this_step = nd.nonfatalDataset("upload", me_id)
    success_file = this_step.get_output_file('upload')
    # Skip repeated upload if it there is already a successful one.
    #   Sometimes the launcher is closed and then re-run. Cluster_tools does
    #   not yet check for existing jobs with the same arguments that have been
    #   submitted by a different method. There are limited uploads that
    #   are difficult to delete and the upload takes many resources, so it's
    #   worth it   
    if is_resubmission and os.path.isfile(success_file):
        print("Successful upload already complete!")
        return (True)
    # Upload data
    model_type = which_model_type(me_id)
    if model_type == "custom_dismod":
        success_df = upload_dismod_input(me_id, cnf_model_version_id, is_estimation_yrs)
    else:
        success_df = upload_epi_model(me_id, cnf_model_version_id, is_estimation_yrs)
    # If theu upload is successfully uploaded, save tracking information
    if (len(success_df) > 0) and isinstance(success_df, pd.DataFrame):
        if model_type == "custom_dismod":
            if success_df.at[0, 'request_status'] == "Successful":
                upload_id = success_df.at[0, 'request_id']
                success_upload = 1
                crosswalk_id = success_df.at[0,'crosswalk_version_id']
                success_df['crosswalk_version_id'] = crosswalk_id
            else:
                upload_failed()
    
        else:
            if 'model_version_id' in success_df.columns:
                upload_id = success_df.at[0, 'model_version_id']
                crosswalk_id = success_df.at[0, 'crosswalk_version_id']
                success_upload = 1
            else:
                upload_failed()
        print("upload complete. saving record...")
        success_df.to_csv(success_file, index=False)
        epi_upload.update_upload_record(me_id, cnf_model_version_id, 
                                        upload_id, crosswalk_id, model_type, success_upload)
        return (True)
    else:
        upload_failed()


def run_array_job(): 
    '''
    '''
    params = pd.read_csv(argv[1])
    task_id = cluster_tools.get_array_task_id()
    me_id = int(params.loc[task_id-1, 'me_id'])
    cnf_model_version_id= params.loc[task_id-1, 'cnf_model_version_id']
    is_resub = params.loc[task_id-1, 'is_resubmission']
    is_estimation_yrs = params.loc[task_id-1, 'is_estimation_yrs']
    success = manage_model_upload(me_id, cnf_model_version_id, is_resub, is_estimation_yrs)
    return 

def run_serially(): 
    '''
    '''
    me_id = int(argv[1])
    cnf_model_version_id = int(argv[2])
    is_resubmission = bool(int(argv[3]))
    is_estimation_yrs = bool(int(argv[4]))
    success = manage_model_upload(me_id, cnf_model_version_id, is_resubmission, is_estimation_yrs)

if __name__ == "__main__":
    if len(argv) == 2: #array job
        run_array_job()
    else: 
        run_serially() 


   
