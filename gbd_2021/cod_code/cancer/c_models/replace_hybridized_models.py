'''
Description: Used to create a new hybrid CODEm model by replacing specific locations from
    an older DR/GB model with a newer DR/GB model. The result will then be combined with the 
    other model in order to create a new hybrid model_version

Workflow:
1) Get model version ids from modelers for female specific and male specific models
2) Get two qlogins
3) Run replace_hybridized_models.py for female models on one qlogin and males for the other
4) Wait for array job completion
5) Check that all locations have been generated at work_dir (to get location list, 
   filter on most_detailed = 1 in get_location_metadata with location_set_id = 35)
6) Continue script to move into upload
7) Check codviz for model upload success

TODO: Create job checking on location completion

How to Use: python replace_hybridized_models.py -c <acause> 
                                                -gb_mv_id <gb_mv_id> 
                                                -dr_mv_id <dr_mv_id>
                                                -spec_loc_mv_id <spec_loc_mv_id>
                                                -model_type <model_type_to_replace>
                                                -loc_type <loc_type>
                                                -is_resub <is_submission>
                                                -upload_only <upload_only>
                                                -sex_id <sex_id>

Arguments:
    acause (str) : cancer to replace/hybridize models for
    gb_mv_id (int) : model_version_id for CODEm global model
    dr_mv_id (int) : model_version_id for CODEm data-rich model
    spec_loc_mv_id (int) :  model_version_id for specific location global/data-rich model
    model_type (str) : model type to replace with spec_loc_mv_id model locs,
                                  Allowed values: ['gb', 'dr']
                                  dr = data rich, gb = global
    loc_type (str) : location type to draw from, must add new locations to list_locations
                     Current availiable: ['SSA', 'USA']
    is_resub (bool) : whether to overwrite previous outputs, Allowed values: T, F
    upload_only (bool) : whether to upload saved location files only, Allowed values: T, F
    sex_id (int) : Optional, specifies what sex_id you are uploading

Resource Requirements:
    - >= 60 GB Ram 
    - >= 10 cores/threads
    - must run on either gbd or cancer environment
    - >= 1.5 hours (if cluster is not busy)

Contributors: USERNAME
'''


# import libraries 
import sys 
import os
import pandas as pd
import numpy as np
import subprocess
import argparse

from get_draws.api import get_draws 
from db_queries import get_location_metadata
from save_results import save_results_cod
from cancer_estimation.py_utils import common_utils as utils 
from cancer_estimation._database import cdb_utils as cdb 
import cancer_estimation.c_models.e_nonfatal.nonfatal_dataset as nd 


def submit_qsub(acause : str, sex_id : int) -> str: 
    ''' generates qsub command to submit to cluster 
    '''
    job_name = 'custom_cod_hybrid_{}'.format(acause)
    worker_script = utils.get_path("codem_hybridize_worker_script", 
                                   process = "cancer_model")
    param_dict = {'acause' : [acause],
                 'location_id' : nd.list_location_ids(),
                 'sex_id' : [sex_id],
                }
    param_file = create_params_file(param_dict)
    param_fpath = save_params_file(param_file, acause, sex_id)
    cluster_dict = define_cnf_cluster_settings() 
    output_path = '{}/custom_cod_hybrid_{}_output'.format(cluster_dict['output_logs'], acause)
    error_path = '{}/custom_cod_hybrid_{}_error'.format(cluster_dict['error_logs'], acause)
    utils.ensure_dir(output_path)
    utils.ensure_dir(error_path)

    qsub_call = ('qsub -l m_mem_free={mem}G -l fthread={th} -l h_rt={rt} -tc 500 -l archive=True'
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
                        o=output_path,
                        e=error_path,
                        jname=job_name,
                        njobs=len(param_file),
                        sh=utils.get_path(key="gbd_py_shell", process = "common"),
                        ws=worker_script,
                        p=param_fpath))
    return(qsub_call)


def define_cnf_cluster_settings() -> dict:
    ''' Returns cluster settings depending on if launching prep or upload jobs 
    '''
    cluster_settings = {'memory' : 40,
                        'cores' : 1,
                        'time' : '00:12:00',
                        'cluster' : 'all.q',
                        'project' : 'proj_cancer_prep',
                        'error_logs' :  utils.get_path(key="cancer_logs", process = "common"), 
                        'output_logs' :  utils.get_path(key="cancer_logs", process = "common")
                        }
    return(cluster_settings)


def create_params_file(param_dict : dict) -> pd.DataFrame:
    ''' Takes a dictionary of lists and returns cartesian product of dictionary
    '''
    from itertools import product
    params_df = pd.DataFrame([row for row in product(*param_dict.values())], columns=param_dict.keys())
    return(params_df)


def save_params_file(params_df : pd.DataFrame, acause : str, sex_id : int) -> str: 
    ''' Takes a csv with job arguments and saves to correct directory
    '''
    filepath = '{}/array_job_parameters/codem_{}_sex_id_{}.csv'.format(
        utils.get_path("codem_hybridize_outputs", process = "cancer_model"), 
                                    acause, sex_id)
    params_df.to_csv(filepath, index=False)
    return(filepath) 


def get_cause_id(acause): 
    ''' Takes an acause, and returns its' respective cause_id
    '''
    cdb_api = cdb.db_api('cancer_db') 
    cause_table = cdb_api.get_table('registry_input_entity')
    cause = cause_table.loc[cause_table['acause'].eq(acause), 'cause_id']
    cause_id = cause.values[0] # get cause_id - should be unique per cause 
    return(cause_id)


def list_locations(gbd_id, loc_type):
    ''' Returns a list of all locations
    '''
    loc_df = get_location_metadata(location_set_id=35, gbd_round_id=gbd_id)
    if loc_type == "USA":
        locs = loc_df.loc[loc_df['ihme_loc_id'].str.startswith('USA'), 'location_id'].unique().tolist()
    elif loc_type == "SSA":
        locs = loc_df.loc[(loc_df['region_name']!="Southern Sub-Saharan Africa") &
                          (loc_df['most_detailed']==1) &
                          (loc_df['super_region_name']=="Sub-Saharan Africa"), 'location_id'].unique().tolist()
    print("Pulled locations for location_type: {}".format(loc_type))
    assert len(locs) > 0, "Locations to replace for is empty!"
    return(locs) 


def get_global_model(cause_id, gb_mv_id, gbd_round, d_step): 
    ''' function pulls a specified global model.
        NOTE: currently no check in place that this is truly a global model 
    '''
    print('pulling global model...')
    global_model = get_draws(gbd_id_type='cause_id', 
                      gbd_id=cause_id, 
                      source='codem',
                      version_id=gb_mv_id,
                      gbd_round_id=gbd_round,
                      decomp_step=d_step)
    global_model['model_version_id'] = gb_mv_id
    global_model['model_type'] = "gb"
    assert len(global_model) > 0, \
        'unable to pull global model result'
    return(global_model)


def get_dr_model(cause_id, dr_mv_id, gbd_round, d_step): 
    ''' function pulls a specific DR model
        NOTE: currently no check in place that this is truly a DR model 
    '''
    print('pulling data rich model...')
    dr_model = get_draws(gbd_id_type='cause_id',
                        gbd_id=cause_id,
                        source='codem',
                        version_id=dr_mv_id, 
                        gbd_round_id=gbd_round, 
                        decomp_step=d_step)
    dr_model['model_version_id'] = dr_mv_id
    dr_model['model_type'] = "dr"
    assert len(dr_model) > 0, \
        'unable to pull DR model result'
    return(dr_model) 


def get_spec_loc_model(cause_id, spec_loc_mv_id, gbd_round, d_step, model_type): 
    ''' function to pull another model used to replace loc-specific entries 
        NOTE: currently no check in place for model type
    '''
    print('pulling new model...')
    spec_loc_model = get_draws(gbd_id_type='cause_id',
                            gbd_id=cause_id, 
                            source='codem',
                            version_id=spec_loc_mv_id, 
                            gbd_round_id=gbd_round, 
                            decomp_step=d_step)
    spec_loc_model['model_version_id'] = spec_loc_mv_id
    spec_loc_model['model_type'] = model_type
    assert len(spec_loc_model) > 0, \
        'unable to pull specific location model result'
    return(spec_loc_model)  


def replace_with_spec_loc_model(model_to_replace, spec_loc_model, gbd_id, loc_type): 
    ''' Takes the older model and removes all loc_type entries. then takes the newer 
        model, subsets on loc_type locations, and then appends the two results together
    '''
    print('replacing {} entries...'.format(loc_type))
    locs = list_locations(gbd_id, loc_type)

    assert set(list(model_to_replace['model_version_id'].unique())) != set(list(spec_loc_model['model_version_id'].unique())),\
        "Your replacement model and your model to replace's model_version_ids are the same!"
    
    # check that both models have the locations that you want to switch out and replace
    models = {"model_to_replace":model_to_replace, "replacement_model":spec_loc_model}
    for model in models.keys():
        assert set(locs) <= set(list(models[model]['location_id'].unique())),\
            "You have missing locations in the {} model:\n{}".format(model,
                list(set(locs)-set(list(models[model]['location_id'].unique())))
            )
    
    # remove all locations from model
    model_to_replace_no_locs = model_to_replace.loc[~model_to_replace['location_id'].isin(locs), ]

    # keep locations from newer model
    new_loc_model = spec_loc_model.loc[spec_loc_model['location_id'].isin(locs), ]

    # append dataframes together. and test on number of rows 
    new_model = model_to_replace_no_locs.append(new_loc_model) 
    assert len(new_model) == len(model_to_replace), \
        'some rows from original model are missing'
    assert len(new_model['cause_id'].unique()) == 1, \
        'there is more than 1 cause_id in the model you are trying to create'
    assert len(new_model['sex_id'].unique()) == 1, \
        'there is more than 1 sex in the model you are trying to create'
    return(new_model)


def create_new_hybrid_model(new_model, model_to_combine, model_type): 
    ''' Hybrid models are the combination of data rich and global models, where data rich models 
        take precedence. any missing locations are then filled from global model entries 
    '''
    # append data rich and global models. then removing any duplicates coming from global models
    if model_type == "dr":
        combine_model = new_model.append(model_to_combine)
    elif model_type == "gb":
        combine_model = model_to_combine.append(new_model)

    # simply using drop_duplicates works here since all DR models come before global models 
    final_model = combine_model.drop_duplicates(subset=[
        'age_group_id','year_id','location_id','sex_id'], keep='first')

    # some trivial checks ensuring that model_versions entered are for the same cause/sex/measure
    assert len(final_model['cause_id'].unique()) == 1, \
        'careful! there is more than 1 cause in the model you are about to upload'
    assert len(final_model['sex_id'].unique()) == 1, \
        'careful! there is more than 1 sex in the model you are trying to upload'
    assert len(final_model['measure_id'].unique()) == 1, \
        'careful! there are other data types other than deaths'
    print('new hybrid model created!')
    return(final_model)


def export_model_locations(acause, final_model = None, is_resub = False, sex_id = None, 
                            upload_only = False): 
    ''' Creates a working directory, then saves the hybrid model, and then 
        calls an arry job to split the hybrid model into location specific files
    '''
    
    if sex_id is None:
        sex_id = final_model['sex_id'].values[0]
    codem_path = utils.get_path("codem_hybridize_outputs", process = "cancer_model")
    work_dir = '{}/{}/sex_id_{}'.format(codem_path, acause, sex_id) # NOTE: hard-coded path 
    utils.ensure_dir(work_dir) # create directory if it doesn't already exist 

    # only save/re-save csv if not in resubmission mode
    if not is_resub:
        final_model.to_csv("{}/{}/sex_id_{}/final_model.csv".format(codem_path,
            acause, sex_id), index = False)
        print('saving compiled results to...{}'.format(work_dir))
    
    # only run array jobs to split into loc specific files if not uploading only
    if not upload_only:
        qsub_call = submit_qsub(acause, sex_id)
        subprocess.call(qsub_call, shell=True)
        pause = input("Pause here to wait for jobs to finish.\n"\
                     "Check this path {} to make sure all locations files are generated\n"\
                     "otherwise check logs or relaunch ~8-10 min if cluster isn't busy...".format(
                         work_dir))
    return(work_dir)


def prompt_for_description():
    ''' Asks user for a description of what will be uploaded 
    '''
    prompt = ('please enter a useful description for the updated hybrid model before saving: \n>')
    ask_me = input(prompt) 
    return(ask_me)


def save_results(work_dir, user_description, cause_id, this_sex, gbd_round, d_step):
    ''' save results to cod. this will create a new model_version_id in CoDViZ
    '''
    save_results_cod(input_dir=work_dir, 
                    input_file_pattern='{location_id}.csv', 
                    cause_id=cause_id,
                    sex_id=this_sex,
                    description=user_description,
                    gbd_round_id=gbd_round,
                    decomp_step=d_step,
                    mark_best=False
    ) 
    return 


def main(acause, global_model_version_id, data_rich_model_version_id, 
         spec_loc_model_version_id, model_type_to_replace, 
         location_type, is_resubmission, upload_only, sex_id): 
    '''
    1. pull global model result
    2. pull DR model result
    3. pull DR/GB new model result 
    4. replace DR with DR/GB 
    5. append results 1 & 4. removing any duplicates. 
    6. export location-specific flat files 
    7. use save_results_cod() 
    '''
    
    # pull decomp_step and gbd_round and cause_id
    gbd_id = utils.get_gbd_parameter('current_gbd_round')
    d_step = utils.get_gbd_parameter('current_decomp_step')
    cause_id = get_cause_id(acause) 

    # ask for user-description of model that will be updated 
    user_description = prompt_for_description()
    
    # retrieve codem models at draw level and create a new hybrid model
    if not upload_only:
        if not is_resubmission:
            gb_model = get_global_model(cause_id, global_model_version_id, gbd_id, d_step)
            dr_model = get_dr_model(cause_id, data_rich_model_version_id, gbd_id, d_step)
            replacement_model = get_spec_loc_model(cause_id, spec_loc_model_version_id, gbd_id, d_step, 
                                                   model_type_to_replace) 

            # determine based on parameters, what to replace with what
            if model_type_to_replace == "dr":
                new_model = replace_with_spec_loc_model(dr_model, replacement_model, 
                                                        gbd_id, location_type)
                new_hybrid_model = create_new_hybrid_model(new_model, gb_model, model_type_to_replace)
            elif model_type_to_replace == "gb":
                new_model = replace_with_spec_loc_model(gb_model, replacement_model, 
                                                        gbd_id, location_type)
                new_hybrid_model = create_new_hybrid_model(new_model, dr_model, model_type_to_replace)
            this_sex = new_hybrid_model['sex_id'].unique().tolist()
        else:
            new_hybrid_model = None # in resubmission mode, we ignore model
            this_sex = [sex_id]
        # export model results and use save_results_cod 
        working_directory = export_model_locations(acause, new_hybrid_model, is_resub = is_resubmission,
                                                   sex_id = this_sex[0], upload_only = upload_only)
    else:
        working_directory = export_model_locations(acause, new_hybrid_model=None, is_resub = is_resubmission,
                                                   sex_id=sex_id, upload_only = upload_only)
    save_results(working_directory, user_description, cause_id, sex_id, gbd_id, d_step)
    print('successfully saved new model! check codviz for new model_version')
    return 


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--acause',
                        type=str, nargs='?',
                        help='cause to hybridize and upload')
    parser.add_argument('-gb_mv_id', '--gb_mv_id',   
                        type=int, nargs='?')
    parser.add_argument('-dr_mv_id', '--dr_mv_id',   
                        type=int, nargs='?')
    parser.add_argument('-spec_loc_mv_id', '--spec_loc_mv_id',   
                        type=int, nargs='?')
    parser.add_argument('-model_type', '--model_type_to_replace',
                        type=str, nargs='?', default="gb")
    parser.add_argument('-loc_type', '--loc_type',
                        type=str, nargs='?')
    parser.add_argument('-is_resub', '--is_resubmission',
                        type=utils.str2bool, nargs='?',
                        default=False,
                        help=('Activate resubmission mode (default=True).'
                              ' Set mode to False to replace previous outputs'))     
    parser.add_argument('-upload_only', '--upload_only',
                    type=utils.str2bool, nargs='?', default=False)
    parser.add_argument('-sex_id', '--sex_id',
                    type=int, default=None)
    args = parser.parse_args()
    return(args)


if __name__ == "__main__": 
    utils.check_gbd_parameters_file()
    args = parse_args()
    main(args.acause, args.gb_mv_id, args.dr_mv_id, args.spec_loc_mv_id, 
         args.model_type_to_replace, args.loc_type, args.is_resubmission, 
         args.upload_only, args.sex_id)