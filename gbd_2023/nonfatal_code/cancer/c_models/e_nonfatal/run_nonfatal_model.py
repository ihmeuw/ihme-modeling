
# -*- coding: utf-8 -*-
'''
Description: Launches jobs to run pipeline for each location_id and 
    modelable_entity_id 
Contents:
    generate_outputs :
    upload_outputs : 
    run_model : runs the above functions in order
How To Use: pass the script an acause with an optional boolean indicating whether
    to run in resubmission mode
    resubmission mode:
Contributors: INDIVIDUAL_NAME, INDIVIDUAL_NAME, USERNAME
'''
try:
    import db_queries
except:
    print("Unable to load a central IHME package. Make sure you have the team's current environment activated")
    exit()
from cancer_estimation.py_utils import(
    common_utils as utils,
    cluster_tools
)
from cancer_estimation._database import cdb_utils as cdb
import cancer_estimation.py_utils.response_validator as rv
from cancer_estimation.c_models.e_nonfatal import nonfatal_dataset as nd
from cancer_estimation.c_models.e_nonfatal.tests import all_nonfatal_locations 
import os
import pandas as pd
import argparse
from time import sleep
import subprocess
import re 
import time
from db_queries import(
    get_location_metadata,
    get_population 
)
import getpass
import logging
logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(
    format='%(asctime)s %(levelname)s %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)

# these are used when prompting the user whether to clear previous logs and estimates
CLEARING_MENU = {'y' : 'yes', 'n' : 'no'}
YES = 'y'


def call_prep_job(acause : str, cnf_model_version_id : int, is_resubmission : int, 
                            faux_correct : int, is_estimation_yrs : int, 
                            run_prep=True, prep_steps = [1,2,3,4], clear_all_estimates_and_logs='n') -> str: 
    ''' generates job command to submit to cluster 
    '''
    # print(f'call_prep_job fn called for: \nacause: {acause}\ncnfModel_version_id: {cnf_model_version_id}\nis_resubmission: {is_resubmission}\nfaux_correct: {faux_correct}\nis_estimation_yrs: {is_estimation_yrs}\nrun_prep: {run_prep}\nprep_steps: {prep_steps}\n\n-------------------------------\n')
    # run only 100 jobs in parallel for if incidence step included in steps
    if 1 in prep_steps:
        job_lim = 100
    # runs 500 jobs in parallel when it's running the rest of the pipeline 
    else:
        job_lim = 500

    run_upload=False
    param_dict = gen_param_dict(run_prep, run_upload, argslist=[acause, cnf_model_version_id, 
                                                                is_resubmission, faux_correct, 
                                                                is_estimation_yrs, prep_steps])
    # print(f'call_prep_job fn: param_dict: \n{param_dict}')
    cluster_dict = define_cnf_cluster_settings(run_prep, run_upload)

    output_path = '{}/nfPipe_{}_output'.format(cluster_dict['output_logs'], acause)
    error_path = '{}/nfPipe_{}_error'.format(cluster_dict['error_logs'], acause)
    # clear logs only when not in resubmission mode
    if not bool(is_resubmission):
        if clear_all_estimates_and_logs == 'n':
            # cont = rv.response_validator("Proceed with clearing previous logs for {}? \n".format(acause), CLEARING_MENU)
            cont = 'y'
            if cont == YES:
                clear_previous_logs(acause, log_paths = [output_path, error_path])
        else:
            clear_previous_logs(acause, log_paths = [output_path, error_path])

    # no missing locs results in no call
    if len(param_dict) == 0:
        print("{} has no jobs to run...".format(acause))
        return(None)
    else:
        # create job parameters and settings
        job_dict = {"memory":cluster_dict['memory'],
                    "threads":cluster_dict['cores'],
                    "time":cluster_dict['time'],
                    "job_limit":job_lim,
                    "partition":cluster_dict['cluster'],
                    "queue":cluster_dict['cluster'],
                    "account":cluster_dict['project'],
                    "project":cluster_dict['project'],
                    "output_log_path": output_path,
                    "error_log_path": error_path,
                    "job_name":'{}_nfPipe'.format(re.search(r'neo_(.*)', acause).group(1)),
                    "shell":utils.get_path('gbd_py_shell'),
                    "script":utils.get_path("inc_prev_pipeline", process="nonfatal_model")}
        
    params_df = create_params_file(param_dict)
    save_params_file(params_df, acause, run_prep, run_upload)

    cluster_tools.launch_array_job(job_dict, 
        script_param_dict = param_dict,
        script_param_name = "prep_{}".format(acause), 
        script_param_path = '{}/array_job_parameters/'.format(utils.get_path(process='nonfatal_model', 
                                                            key='this_nonfatal_model')))
    logging.info("Error logs saved here: {}".format(error_path))
    logging.info("Output logs saved here: {}".format(output_path))
    return(None)


def create_params_file(param_dict : dict) -> pd.DataFrame:
    ''' Takes a dictionary of lists and returns cartesian product of dictionary
    '''
    from itertools import product
    params_df = pd.DataFrame([row for row in product(*param_dict.values())], columns=param_dict.keys())
    return(params_df)


def save_params_file(params_df : pd.DataFrame, acause : str, run_prep : bool, run_upload: bool) -> str: 
    ''' Takes a csv with job arguments and saves to correct directory
    '''
    filepath = '{}/array_job_parameters/'.format(utils.get_path(process='nonfatal_model', 
                                                                key='this_nonfatal_model'))
    utils.ensure_dir(filepath)

    if run_prep: 
        filepath = '{}/prep_{}.csv'.format(filepath, acause)
    elif run_upload: 
        filepath = '{}/upload_{}.csv'.format(filepath, acause)
    params_df.to_csv(filepath, index=False)

    return(filepath) 


def updated_generate_estimates(acause : str, cnf_model_version_id : int, 
                              is_resubmission : int, faux_correct : int, 
                              is_estimation_yrs : int, run_prep : bool,
                              prep_steps : list, clear_all_estimates_and_logs: str) -> None:
    ''' Produces new estimates for the acause 
        -- Inputs
            acause: GBD acause classification to estimate
            is_resubmission: Boolean. If False, first removes previous estimates. 
                Otherwise produces estimates only for cause-locations that don't
                 exist
    '''
    call_prep_job(acause, cnf_model_version_id, is_resubmission,
                                   faux_correct, is_estimation_yrs, run_prep, prep_steps, clear_all_estimates_and_logs)
    return()


def gen_param_dict(run_prep : bool, run_upload : bool, argslist : list) -> dict: 
    ''' Function will return a dictionary with keys mapped to list of arguments 
    '''
    # if resub - load missing locations for a given acause
    if bool(argslist[2]):
        try:
            # if multiple prep steps, picks the missing locs in 1st step
            missing_locs = all_nonfatal_locations.check_locs_exist(
                location_list=nd.list_location_ids(), 
                acause=argslist[0], step_id = argslist[5][0])
        except:
            missing_locs = nd.list_location_ids()
    else:
        missing_locs = nd.list_location_ids()
 
    if run_prep:
        param_dict = {'acause' : [argslist[0]],
            'location_id' : missing_locs,
            'cnf_model_version' : [argslist[1]],
            'faux_correct' : [argslist[3]],
            'is_resubmission' : [argslist[2]],
            'is_estimation_yrs' : [argslist[4]],
            'prep_steps' : [','.join(str(p) for p in argslist[5])]
            }

    elif run_upload: 
        param_dict ={'me_id' : argslist[5], 
            'cnf_model_version_id' : [argslist[1]],
            'is_resubmission' : [argslist[2]],
            'is_estimation_yrs' : [argslist[4]],
            'upload_measures' :[','.join(str(p) for p in argslist[6])]
            } 
    return(param_dict)


def define_cnf_cluster_settings(run_prep : bool, run_upload : bool) -> dict:
    ''' Returns cluster settings depending on if launching prep or upload jobs 
    '''
    if run_prep: 
        cluster_settings = {'memory' : 10,
                            'cores' : 3,
                            'time' : '2:30:00',
                            'cluster' : 'all.q', #Note: feature: prompt user for this 
                            'project' : 'proj_cancer_prep',
                            'error_logs' :  utils.get_path(key="cancer_logs", process = "common"), 
                            'output_logs' :  utils.get_path(key="cancer_logs", process = "common")
                            }
    if run_upload:
        cluster_settings = {'memory' : 160,
                            'cores' : 30,
                            'time' : '10:30:00',
                            'cluster' : 'long.q', #Note: feature: prompt user for this 
                            'project' : 'proj_cancer_prep',
                            'error_logs' :  utils.get_path(key="cancer_logs", process = "common"), 
                            'output_logs' :  utils.get_path(key="cancer_logs", process = "common")
                            }
    return(cluster_settings)

def remove_uploaded_mes(me_df : list) -> pd.DataFrame: 
    '''
    '''
    check_dir = '{}/upload'.format(utils.get_path(process='nonfatal_model', key='this_nonfatal_model'))
    me_list = me_df['me_id'].unique().tolist()
    for m in me_list: 
        if os.path.isfile('{}/{}/upload.csv'.format(check_dir, m)):
            me_df = me_df.loc[~me_df['me_id'].eq(m), ]
    return me_df

# THIS FUNCTION APPEARS NOT TO BE USED IN THE PIPELINE.
def gen_upload_qsub(acause : str, cnf_model_version_id : int, uploaded_tags : list,
                    is_resubmission : int, is_estimation_yrs : int, 
                    run_upload=True, upload_measures=[5,6]) -> str:
    ''' Returns qsub command used to generate upload jobs 
    '''
    run_prep=False
    job_name = 'U_nfPipe_{}'.format(acause)
    worker_script = utils.get_path("nonfatal_uploader", process="nonfatal_model")
    # generate list of me_ids for a given acause 
    me_ids = nd.get_modelable_entity_id(acause, uploaded_tags)
    if not isinstance(me_ids, list):
        me_ids = [me_ids]
    if (is_resubmission):
        # helper function that will remove from list where models are already uploaded 
        uploaded_tags = remove_uploaded_mes(uploaded_tags)
    me_ids = uploaded_tags['me_id'].unique().tolist()
    param_dict = gen_param_dict(run_prep, run_upload, argslist=[acause,
                                                    cnf_model_version_id, 
                                                    is_resubmission,
                                                    0, #faux_correct argument not relevant for uploads  
                                                    is_estimation_yrs,
                                                    me_ids,
                                                    upload_measures])
    # merge me_tag to param_file
    param_file = create_params_file(param_dict)
    param_file = pd.merge(param_file, uploaded_tags, on='me_id', how='left')
    assert ~param_file['me_tag'].isnull().any()

    # generate qsub uploads for all phases if computational phase has already been uploaded.
    # Otherwise, subset to param_file to just the computational phase 
    param_fpath = save_params_file(param_file, acause, run_prep, run_upload)
    cluster_dict = define_cnf_cluster_settings(run_prep, run_upload)

    output_file = '{}/U_nfPipe_{}_o'.format(cluster_dict['output_logs'], acause)
    error_file = '{}/U_nfPipe_{}_e'.format(cluster_dict['error_logs'], acause)

    # remove previous outputs/error logs if they are files, make error folders
    for path in [output_file, error_file]:
        if os.path.isfile(path): # remove if is file
            try:
                os.remove(path)
            except:
                print("Unable to remove {}".format(path))
        if not os.path.isdir(path): # make dir if dir not present
            os.mkdir(path)

    # clear logs only when not in resubmission mode
    if not bool(is_resubmission):
        cont = rv.response_validator("Proceed with clearing previous logs for {}? \n".format(acause), CLEARING_MENU)
        if cont == YES:
            clear_previous_logs(acause, log_paths = [output_file, error_file])

    qsub_call = ('qsub -l m_mem_free={mem}G -l fthread={th} -l h_rt={rt} -l archive=True'
        ' -q {clust} -cwd -P {proj}'
        ' -o {o}'
        ' -e {e}'
        ' -N {jname}'
        ' -t 1:{njobs}'
        ' {sh}'
        ' {ws}'
        ' {p}'.format(mem=cluster_dict['memory'], 
                        th=cluster_dict['cores'],
                        rt=cluster_dict['time'],
                        clust=cluster_dict['cluster'], 
                        proj=cluster_dict['project'], 
                        o=output_file,
                        e=error_file,
                        jname=job_name,
                        njobs=len(param_file),
                        sh=utils.get_path(key="gbd_py_shell", process = "common"),
                        ws=worker_script,
                        p=param_fpath))
    return(qsub_call)


def call_upload_job(acause : str, cnf_model_version_id : int, uploaded_tags : list,
                        is_resubmission : int, is_estimation_yrs : int, 
                        run_upload=True, upload_measures=[5,6]) -> str:
    ''' generates job command to submit to cluster 
    '''
    run_prep=False
    # generate list of me_ids for a given acause 
    me_ids = nd.get_modelable_entity_id(acause, uploaded_tags)
    if not isinstance(me_ids, list):
        me_ids = [me_ids]
    if (is_resubmission):
        # helper function that will remove from list where models are already uploaded
        uploaded_tags = remove_uploaded_mes(uploaded_tags)
    me_ids = uploaded_tags['me_id'].unique().tolist()
    
    # get array job script params
    param_dict = gen_param_dict(run_prep, run_upload, argslist=[acause,
                                                    cnf_model_version_id, 
                                                    is_resubmission,
                                                    0, #faux_correct argument not relevant for uploads  
                                                    is_estimation_yrs,
                                                    me_ids,
                                                    upload_measures])
    script_param_grid, script_param_file = cluster_tools.get_array_job_params(
        script_param_dict = param_dict,
        script_param_name = "upload_{}".format(acause),
        script_param_path = '{}/array_job_parameters/'.format(utils.get_path(process='nonfatal_model', 
                                                            key='this_nonfatal_model')))
     # check me_ids in param_dict
    script_param_grid = pd.merge(script_param_grid, uploaded_tags, on='me_id', how='left')
    assert ~script_param_grid['me_tag'].isnull().any()

    # generate qsub uploads for all phases if computational phase has already been uploaded.
    # Otherwise, subset to param_file to just the computational phase 
    cluster_dict = define_cnf_cluster_settings(run_prep, run_upload)
    output_path = '{}/U_nfPipe_{}_o'.format(cluster_dict['output_logs'], acause)
    error_path = '{}/U_nfPipe_{}_e'.format(cluster_dict['error_logs'], acause)

    # clear logs only when not in resubmission mode
    if not bool(is_resubmission):
        cont = rv.response_validator("Proceed with clearing previous logs for {}? \n".format(acause), CLEARING_MENU)
        if cont == YES:
            clear_previous_logs(acause, log_paths = [output_path, error_path])

    # create job parameters and settings
    job_dict = {"memory":cluster_dict['memory'],
                "threads":cluster_dict['cores'],
                "time":cluster_dict['time'],
                "job_limit":0,
                "partition":cluster_dict['cluster'],
                "queue":cluster_dict['cluster'],
                "account":cluster_dict['project'],
                "project":cluster_dict['project'],
                "output_log_path": output_path,
                "error_log_path": error_path,
                "job_name":'U_nfPipe_{}'.format(acause),
                "shell":utils.get_path('gbd_py_shell'),
                "script":utils.get_path("nonfatal_uploader", process="nonfatal_model")}
    cluster_tools.launch_array_job(job_dict, 
        script_param_grid = script_param_grid,
        script_param_file = script_param_file)
    logging.info("Error logs saved here: {}".format(error_path))
    logging.info("Output logs saved here: {}".format(output_path))
    return(None)

def updated_generate_uploads(acause : str, cnf_model_version_id : int, uploaded_tags : list, 
                             is_resubmission : bool, is_estimation_yrs : bool,
                             run_upload=True, upload_measures=[5,6]) -> None: 
    ''' generate qsub command for upload jobs and submits to the cluster 
    '''
    call_upload_job(acause, cnf_model_version_id, uploaded_tags, is_resubmission, 
                        is_estimation_yrs, run_upload, upload_measures)
    return()
    

def split_liver_estimates(cnf_model_version_id : int, is_resubmission : bool) -> bool:
    ''' Calls split_estimates for liver cancer
    '''
    print("Splitting liver cancer model...")
    split_worker = utils.get_path("split_epi",process="cancer_model")
    # Get a list of the parent mes to split
    me_table = pd.read_csv('{}/cnf_model_entity.csv'.format(
        utils.get_path(process='nonfatal_model', key='database_cache')))

    liver_data = me_table.loc[me_table['is_active'].eq(1) &
                        me_table['acause'].eq("neo_liver") &
                        me_table['cancer_model_type'].eq("custom_epi"),:]
    parent_mes = liver_data['modelable_entity_id'].unique().tolist()
    print(parent_mes)
    arg_list = [[p, cnf_model_version_id] for p in parent_mes]
    
    # Start jobs 
    job_dict = cluster_tools.create_jobs(script_path=split_worker,
                                         job_header="csl",
                                         memory_request=120,
                                         fthread = 40,
                                         runtime = '12:00:00',
                                         id_list=parent_mes,
                                         script_args=arg_list,
                                         project_name="cancer",
                                         shell='gbd_py_shell')
    
    if not is_resubmission:
        for i in job_dict:
            job_dict[i]['job'].launch()
    # Check for results
    description="split liver"
    success_df = cluster_tools.wait_for_results(job_dict,
                                                jobs_description=description,
                                                noisy_checker=False,
                                                output_file_function=upload_file_func,
                                                skip_resubmission=False,
                                                max_minutes=800)
    success = cluster_tools.validate_success(success_df, description)
    return(success)


def is_valid_run(cnf_model_version_id : int) -> bool:
    ''' checks that all columns in cancer_db.cnf_model_version has been filled 
    '''
    record = nd.get_run_record(cnf_model_version_id)
    return(bool(len(record)==1))


def clear_previous_incidence(acause : str, cnf_model_version_id: int) -> None:
    ''' Clears all inputs for incidence, as well as the output file 
    '''
    print("Clearing previous incidence files for {}...".format(acause))
    steps = ['incidence', 'mortality', 'mir']
    for step in steps:
        if step=='mir' and cnf_model_version_id == 40 and acause=='neo_neuro': 
            continue
        utils.clean_directory_tree(nd.get_folder(step, acause))
    # For leukemia subtypes, also wipe data for the parent cause
    if acause.startswith("neo_leukemia_"):
        utils.clean_directory_tree(nd.get_folder(step, "neo_leukemia"))
    return()


def clear_previous_pipeline(acause : str) -> None:
    ''' Clears all steps in the Nonfatal pipeline 
        steps where files are removed: [final_results, survival, prevalence, dismod_inputs]
    '''
    print("Clearing previous pipeline outputs for {}...".format(acause))
    steps =  ['final_results', 'survival', 'prevalence', 'dismod_inputs']
    for step in steps:
        utils.clean_directory_tree(nd.get_folder(step, acause))
    return()


def clear_previous_logs(acause : str, log_paths : list) -> None:
    ''' Clears previous nf logs for given cause
    '''
    print("Clearing previous logs for {}...".format(acause))
    for log_path in log_paths:
        utils.ensure_dir(log_path)
        utils.clean_directory_tree(log_path)
    return()


def upload_file_func(id : int):
    ''' Returns an anonymous function which defines an upload output relative to
            some indexed 'id'
    '''
    return (nd.nonfatalDataset("upload", id[0]).get_output_file('upload'))


def clear_previous_upload(me_ids : list) -> None:
    ''' Removes any existing me_id output files defined by the file_func 
        lambda function (as a function of me_id)
    '''
    print("Clearing previous upload success indicators...")
    if not isinstance(me_ids, list):
        raise TypeError("me_ids must be a list")
    
    assert len(me_ids) > 0,\
        "me_ids is empty!"
        
    mes = nd.load_me_table()
    if not set(me_ids) <= set(list(mes['modelable_entity_id'].unique())):
        raise ValueError("These me_ids: {} \ndo not exist in the cnf_modelable_entity table".format(
            set(me_ids) - set(list(mes['modelable_entity_id'].unique()))))
    
    output_files = [upload_file_func([m]) for m in me_ids]
    
    for f in output_files:
        try:
            os.remove(f)
        except FileNotFoundError:
            print("\033[91mWARNING: Can't remove: {}. File doesn't exist.\033[0m".format(f))
    return("    previous upload cleared.")


def cache_cnf_db_tables(pop_run_id : int) -> None:
    ''' Saves all NF-related database tables to scratch for pipeline 
        run to later reference
    '''
    print('Caching database tables for pipeline run...')
    release_id = utils.get_gbd_parameter('current_release_id')
    outdir = utils.get_path(process='nonfatal_model', key='database_cache')
    utils.ensure_dir(outdir)
    can_db = cdb.db_api()

    # loop through each table name, and export 
    list_db_tables = ['cancer_age_conversion','cnf_model_entity','cnf_model_version',
                    'mir_lower_cap','mir_upper_cap','mir_model_run','registry_input_entity',
                    'cod_model_entity', 'cnf_lambda_version', 'rel_survival_version',
                    'sequelae_durations','mir_model_entity', 'cnf_model_upload']
    for d in list_db_tables:
        can_db.get_table(d).to_csv('{}/{}.csv'.format(outdir, d))

    # save version of population for conversions to rates 
    get_population(location_id=nd.list_location_ids(), sex_id=[1,2], 
        age_group_id=nd.nonfatalDataset.estimated_age_groups, 
        year_id='all', release_id = release_id,
        run_id = pop_run_id).to_csv('{}/population.csv'.format(outdir))

    loc_metadata = get_location_metadata(location_set_id=35, release_id = release_id) 

    loc_metadata.to_csv('{}/location_metadata.csv'.format(outdir))
    print('Caching tables complete!')
    return


def gen_me_tags(acause : str) -> list: 
    ''' Returns a list of me_tags
        me_tag values: [computational_total, primary_phase, metastatic_phase, 
                        terminal_phase, controlled_phase]
    '''
    # Generate list of uploaded tags
    if not isinstance(acause, str):
        raise TypeError("Input value for gen_me_tags's param acause: {} is not a str".format(acause))
    tbl = nd.load_me_table()
    uploaded_tags = tbl.loc[tbl['acause'].eq(acause), ['modelable_entity_id','me_tag']]
    uploaded_tags.rename(columns={'modelable_entity_id' : 'me_id'}, inplace=True)
    assert len(uploaded_tags) > 0,\
        "Input value for gen_me_tags's param acause: {}"\
            " has no values/entries in the cnf_modelable_entity table".format(acause)
    return uploaded_tags 


def check_job_status(cause_list : list) -> None:
    ''' Checks how many locations are left for each cause
        for each nf prep step
    '''
    all_locs = nd.list_location_ids()
    for acause in cause_list:
        def _get_num_locs_left(all_locs, step):
            # get number of locations remaining for the cause
            cur_locs = all_nonfatal_locations.check_locs_exist(all_locs, acause, step)
            return(len(cur_locs))

        # print a status report every 30 min
        print("Cause: {}\n\nLocations remaining for each step\n"\
            "incidence: {}\nsurvival: {}\n"\
            "prevalence: {}\nfinal_results: {}\n\n".format(
                acause, _get_num_locs_left(all_locs, "incidence"),
                _get_num_locs_left(all_locs, "survival"),
                _get_num_locs_left(all_locs, "prevalence"),
                _get_num_locs_left(all_locs, "final_results")))


def check_cnf_model_settings(cnf_model_version_id : int) -> None:
    ''' Triggers a response validator to ensure correct release_id, gbd_round_id,
        and other req. gbd_parameters variables are accurate
    '''
    # set key variables to display
    key_settings = ['cnf_model_version_id', 'cnf_lambda_version_id', 
                    'mir_model_version_id', 'codcorrect_model_version_id', 
                    'rel_survival_version_id']

    cnf_tab = pd.read_csv("{}/cnf_model_version.csv".format(
        utils.get_path(process='nonfatal_model', key='database_cache')))
    cnf_tab = cnf_tab.loc[cnf_tab['cnf_model_version_id']==cnf_model_version_id]
    assert len(cnf_tab) == 1, \
    "Current pipeline settings for cnf_model_version_id {} is empty or greater than 1!".format(
        cnf_model_version_id)

    # create message, concaneate 
    key_settings_msg = "".join(
        ["{}: \033[92m{}\033[00m\n".format(key, cnf_tab[key].values[0]) 
         for key in key_settings])

    res = {"y":"Yes, proceed"}
    rv.response_validator(
        prompt = "\033[91mVerify that the following non-fatal pipeline settings are accurate before proceeding.\n"\
               "(enter q to quit): \033[00m\n{}".format(key_settings_msg),
        correct_responses = res)
    return(None)


def run_models(cause_list : list, cnf_model_version_id=None, is_resubmission=False, faux_correct=False,
                    is_estimation_yrs=True, run_prep=True, prep_steps = [1,2,3,4], 
                    run_upload=True, upload_measures = [5,6], split_liver=True) -> None:
    ''' Produces and uploads estimates for each cause
        Note: begins upload for each cause once all estimates are availabe, but
            does not begin checking for upload success until all causes have 
            estimates. This increases the submission efficiency of the process
        -- Inputs
            cause list: A list of GBD acause entries
            resubmission: Boolean. Determines file removal, wait_time, and 
                cluster_utils resubmission behaviour
    '''
    if is_valid_run(cnf_model_version_id):
        print("Starting Cancer Nonfatal Model Run {}".format(cnf_model_version_id))
    if is_resubmission:
        print("resubmission mode")

    # If you would like the option to launch all causes from the beginning of the pipeline, you can do so by enabling the prompt below.
    # However, a careless 'y' given to the prompt can clear out pipeline inputs if a user isn't paying attention. 
    # clear_all_estimates_and_logs = rv.response_validator("Clear all logs and estimates for all launched causes?", CLEARING_MENU)
    clear_all_estimates_and_logs = 'n'

    generating_estimates = False
    if run_prep:
        generating_estimates = True
        if not is_resubmission:
            for acause in cause_list:
                # Delete incidence separately so that it could be manually skipped 
                #   if resubmitting to fix something late in the pipeline
                if clear_all_estimates_and_logs == 'n':
                    # cont = rv.response_validator("Proceed with clearing previous estimates for {}? \n".format(acause), CLEARING_MENU)
                    cont = 'y'

                    # This block should take p_step as an argument and only clear out steps for which the user is requesting new prep data
                    if cont == YES:
                        clear_previous_incidence(acause, cnf_model_version_id)
                        clear_previous_pipeline(acause)
                else:
                    clear_previous_incidence(acause, cnf_model_version_id)
                    clear_previous_pipeline(acause)                    

        # Run prep pipeline and validate results
        for acause in cause_list:
            new_estimates = updated_generate_estimates(
                acause, cnf_model_version_id, is_resubmission, 
                faux_correct, is_estimation_yrs, run_prep, prep_steps, clear_all_estimates_and_logs)
            time.sleep(60)

    # if generating estimates is complete. check if all estimates are generated
    # if so, submit upload jobs
    incomplete_causes = []
    while generating_estimates:
        # print status of location files by nf prep step
        time.sleep(60)
        check_job_status(cause_list)
        # do a job check
        job_cnt = cluster_tools.get_job_count(to_search = "nfPipe")
        if job_cnt == 0:
            check_job_status(cause_list)
            for acause in cause_list: # check status of location estimates
                # we run incidence first, by itself; then we run survival, prevalence, and final results all at once. 
                # so if the step is 1, check the results of the 'incidence' step; otherwise check 'final_results'
                step = 'incidence' if prep_steps == [1] else 'final_results'
                all_locs_gen = all_nonfatal_locations.main(acause, step)
                if (not all_locs_gen):
                    incomplete_causes += [acause]
            generating_estimates = False 
    print('Estimates generated! These causes are still missing estimates...{}'.format(incomplete_causes))
    upload_success = False
    if (run_upload) and (not generating_estimates):
        print('submitting upload jobs...')
        for acause in incomplete_causes: # remove causes that still need estimates generated 
            cause_list.remove(acause)
        if not is_resubmission:
            # Delete previous upload indicators
            for acause in cause_list:
                me_tags = gen_me_tags(acause)
                me_ids = nd.get_modelable_entity_id(acause = acause, 
                                                    me_tag = list(me_tags['me_tag'].unique()))
                clear_previous_upload(me_ids)
        # save all other phases for all causes 
        for acause in cause_list:
            uploaded_tags = gen_me_tags(acause)
            new_uploads = updated_generate_uploads(
                acause, cnf_model_version_id, uploaded_tags, is_resubmission, 
                is_estimation_yrs, run_upload, upload_measures)
     # Iff liver cancer is either successfully uploaded or the only process
     #      requested, split liver cancer results
    if ("neo_liver" in cause_list and split_liver and
        ( (run_upload and upload_success) or not (run_upload and run_prep)) ):
        split_liver_estimates(cnf_model_version_id, is_resubmission)
    
    

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-rid', '--cnf_version_id',
                        type=int,
                        help='The cnf_model_version id number')
    parser.add_argument('-c', '--cause_list',
                        nargs='*',
                        default = nd.modeled_cause_list,
                        type=str,
                        help='List of acauses to upload')
    parser.add_argument('-resub', '--resubmission',
                        type=int, nargs='?',
                        default=1,
                        help=('Activate resubmission mode (default=True).'
                              ' Set mode to False to delete previous outputs'))
    parser.add_argument('-faux_correct', 
                        type=int, nargs='?',
                        default=0,
                        help='Running on 100 vs. 1000 draws')
    parser.add_argument('-is_estimation_yrs', 
                        type=int, nargs='?', 
                        default=1, 
                        help='Run pipeline for annualvs estimation years')
    parser.add_argument('-prep', '--run_prep',
                        type=utils.str2bool, nargs='?',
                        default=True,
                        help='Run prep pipeline (default=True)')
    parser.add_argument('-p_step', '--prep_steps',
                        nargs='*',
                        default = [1,2,3,4],
                        type=int,
                        help='Run for specific nf prep steps') 
    parser.add_argument('-upload', '--run_upload',
                        type=utils.str2bool, nargs='?',
                        default=True,
                        help='Run upload (default=True)')    
    parser.add_argument('-upload_measure', '--upload_measures',
                        type=int, nargs='*',
                        default=[5,6],
                        help='Measures to upload (default = incidence and prevalence') 
    parser.add_argument('--split_liver',
                        type=utils.str2bool, nargs='?',
                        default=False,
                        help=("Run liver splits when uploads complete"+
                            "(default=True, requires neo_liver upload to complete successfully before beginning)"))  
    parser.add_argument('-refresh_cache', '--refresh_cache',
                        type=utils.str2bool, nargs='?',
                        default=True,
                        help=("Refresh database tables cache (default=True"))
    parser.add_argument('-pop_rid', '--population_run_id',
                        type=int, default=None,
                        help='The population run_id to use for this NF run. See codcorrect HUB tracking page')
    args = parser.parse_args()
    return (args)



if __name__ == "__main__":
    start_time = time.time()
    debug=False
    args = parse_args()
    if debug:
        args.refresh_cache=False
        args.cause_list = ['neo_neuro']
        args.run_upload=False
        args.cnf_version_id=39
        args.population_run_id=377
        args.is_estimation_yrs = 0
        args.prep_steps = [4]
        args.run_prep=True
        args.resubmission = 1

    utils.check_gbd_parameters_file()
    if args.refresh_cache:
        cache_cnf_db_tables(args.population_run_id)
    check_cnf_model_settings(args.cnf_version_id)
    cause_list = args.cause_list() if callable(args.cause_list) else args.cause_list
    run_models(cause_list = cause_list, cnf_model_version_id=args.cnf_version_id, 
               is_resubmission=args.resubmission, faux_correct=args.faux_correct,
               is_estimation_yrs=args.is_estimation_yrs,
               run_prep= args.run_prep, prep_steps= args.prep_steps,
               run_upload=args.run_upload, upload_measures=args.upload_measures,
               split_liver=args.split_liver)
    print(f"\nTotal Execution Time\n --- {(time.time() - start_time)} seconds ---")