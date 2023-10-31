#! /ihme/code/cancer/miniconda3/envs/cancer_env/bin/python
# -*- coding: utf-8 -*-
'''
Description: Runs each section of the nonfatal model for the acause-location_id pair 
How To Use:
Contributors: USERNAME
'''

from cancer_estimation.py_utils import (
    cluster_tools,
    common_utils as utils
)
from cancer_estimation.c_models.e_nonfatal import (
    incidence_draws,
    winsorize_incidence,
    survival,
    prevalence,
    adjust_and_finalize,
    nonfatal_dataset as nd
)
import pandas as pd
import sys 
import numpy as np
from sys import argv
import os 
import subprocess


def get_nf_pipeline_prep_steps() -> dict:
    ''' Returns a dict of the nf prep function 
        associated with each prep_step
    '''
    prep_dict = {
        1:incidence_draws.generate_estimates,
        2:survival.generate_estimates,
        3:prevalence.generate_estimates,
        4:adjust_and_finalize.generate_estimates}
    return(prep_dict)


def get_nf_pipeline_params(prep_step : int, prep_params : dict, 
                          prep_dict : dict) -> dict:
    ''' select only params used by function
    '''
    func_params = prep_dict[prep_step].__code__.co_varnames[:prep_dict[prep_step].__code__.co_argcount]
    prep_params_subset = {p:prep_params[p] for p in func_params}

    assert set(prep_params_subset) == set(func_params), \
        "Missing prep params {} for NF step {}".format(
                list(set(func_params) - set(prep_params_subset)), prep_step)
    return(prep_params_subset)


def generate_inc_prev_estimates(acause, location_id, cnf_model_version_id, is_resubmission, 
                                faux_correct=True, is_estimation_yrs=True, prep_steps = [1,2,3,4]):
    ''' For the given acause and location_id, runs each stage of the model
    '''
    rb_custom_rids = utils.get_gbd_parameter("neo_eye_rb_winsorize", 
                                            parameter_type = "nonfatal_parameters")

    prep_params = {'acause':acause,
                   'location_id':location_id,
                   'cnf_model_version_id':cnf_model_version_id,
                   'is_resubmission':is_resubmission,
                   'faux_correct':faux_correct,
                   'is_estimation_yrs':is_estimation_yrs}
    prep_dict = get_nf_pipeline_prep_steps()

    for prep_step in prep_steps:
        prep_params_subset = get_nf_pipeline_params(prep_step, prep_params, prep_dict)
        prep_dict[prep_step](**prep_params_subset)
        if prep_step == 1 and (acause == 'neo_eye_rb') and cnf_model_version_id in rb_custom_rids:
            print("Going through apply rb modeling hotfix")
            apply_rb_modeling_hotfix(acause, location_id, cnf_model_version_id)
            

def apply_rb_modeling_hotfix(acause, location_id, cnf_model_version_id): 
    '''
    '''
    # look at incidence output directory, and count how many files are present 
    total_locs = len(nd.list_location_ids())
    output_files = len(os.listdir('{}/{}/pre_winsorize'.format(utils.get_path(
        process='nonfatal_model', key='incidence_temp_draws'),
                                                            acause)))
    all_inc_generated = (total_locs == output_files)
    # NOTE: all incidence files need to be generated before winsorizing
    # don't run the rest of the pipeline if all incidence files aren't generated 

    if (all_inc_generated):
        print("Going through winsorize incidence") 
        winsorize_incidence.main(location_id)
    elif (~all_inc_generated):
        print('need all incidence draws before before winsorizing data!')
        sys.exit()
    return 



def run_array_job():
    '''
    '''
    params = pd.read_csv(sys.argv[1])
    task_id = cluster_tools.get_array_task_id()
    acause = params.loc[task_id-1, 'acause']
    location_id = params.loc[task_id-1, 'location_id']
    cnf_model_version_id = params.loc[task_id-1, 'cnf_model_version']
    faux_correct = params.loc[task_id-1, 'faux_correct']
    is_resubmission = params.loc[task_id-1, 'is_resubmission']
    is_estimation_yrs = params.loc[task_id-1, 'is_estimation_yrs']
    prep_steps = params.loc[task_id-1, 'prep_steps']
    if isinstance(prep_steps, (int, np.integer)):
        prep_steps = [int(prep_steps)]
    else:
        prep_steps = list(map(int, prep_steps.split(',')))

    # run nonfatal pipeline 
    generate_inc_prev_estimates(acause, location_id,
                                cnf_model_version_id, is_resubmission, 
                                faux_correct, is_estimation_yrs, prep_steps)
    return


def run_serially(): 
    '''
    '''
    acause = argv[1]
    location_id = int(argv[2])
    cnf_model_version_id = int(argv[3])
    faux_correct = bool(int(argv[4]))
    is_resubmission = bool(int(argv[5]))
    is_estimation_yrs = bool(int(argv[6]))
    prep_steps = argv[7]
    prep_steps = list(map(int, prep_steps.split(',')))
    generate_inc_prev_estimates(acause, location_id,
                                cnf_model_version_id, is_resubmission, 
                                faux_correct, is_estimation_yrs,
                                prep_steps)
    return 


if __name__ == "__main__":
    ''' update this to read from param file''' 
    print(len(sys.argv))
    if len(sys.argv) == 2: # array job was submitted from launcher
        run_array_job()
    else: 
        run_serially() 
        
   