#! /ihme/code/cancer/miniconda3/envs/cancer_env/bin/python
# -*- coding: utf-8 -*-
'''
Description: Check if all locations are present for a nonfatal upload.
How To Use: 
Contributors: INDIVIDUAL_NAME
'''

# Import libraries 
import cancer_estimation.py_utils.common_utils as utils
from cancer_estimation.c_models.e_nonfatal import nonfatal_dataset as nd 
from db_queries import get_location_metadata
import pandas as pd 


def load_nonfatal_locations():
    ''' Returns a list of locations that should exist for the nonfatal pipeline.
    '''
    return(nd.list_location_ids()) 

def get_step_name(step_id):
    ''' Returns step name when given step id
    '''
    steps = {1:"incidence", 2:'survival', 
            3:'prevalence',4:'final_results'}
    return(steps[step_id])

def get_output_dir(acause, step):
    ''' 
    '''
    nf_dir = utils.get_path(process='nonfatal_model', key='this_nonfatal_model')
    output_dir ='{d}/{s}/{c}'.format(d=nf_dir, s=step, c=acause)
    return output_dir 


def check_locs_exist(location_list, acause, step=None, step_id = None):
    '''
    '''
    if step_id is not None:
        step = get_step_name(step_id)
    import os.path
    output_dir = get_output_dir(acause, step)
    exist = True
    missing_locs = []
    for i in location_list:
        if step == 'final_results':
            exist = os.path.isfile('{}/finalized_{}.csv'.format(output_dir,i))
        else: 
            exist = os.path.isfile('{}/{}.csv'.format(output_dir,i))
        if exist == False:
            exist = True
            missing_locs += [i]  
    return missing_locs


def main(acause, step): 
    '''
    1. get current nonfatal locations
    2. loop through a given acause and check if all locations are present
    '''
    nf_dir = utils.get_path(process='nonfatal_model', key='this_nonfatal_model')
    current_locs = load_nonfatal_locations()
    missing_locs = check_locs_exist(current_locs, acause, step)
    print('missing locations for {} for {}...{}'.format(acause, step, missing_locs))
    missing_df = pd.DataFrame({'acause':acause, 
                                'missing_location_id':missing_locs})
    utils.ensure_dir('{}/troubleshooting_missing_locs/'.format(nf_dir))
    missing_df.to_csv('{}/troubleshooting_missing_locs/{}.csv'.format(nf_dir, acause))
    return(len(missing_df) == 0)

if __name__ == "__main__":
    import sys 
    import pdb
    acause = sys.argv[1]
    step = sys.argv[2] # incidence, survival, prevalence, or final_results
    pipeline_steps = ['incidence', 'survival', 'prevalence', 'final_results']
    pretty_print_pipeline_steps = '"' + '", "'.join(pipeline_steps[0:len(pipeline_steps)-1]) + '", and "' + str(pipeline_steps[-1]) + '"'
    if step not in pipeline_steps:
        print(f'"{step}" is not a step in our pipeline. The steps are {pretty_print_pipeline_steps}')
        print('Perhaps you have the parameters in the wrong order? The acause should be first.')
    else:
        # Block of code  that allowed comparison betwen nonfatal_dataset.dataset_status()
        # for acause in nd.modeled_cause_list():
        #     print(f'\nacause: {acause}')
        #     for step in pipeline_steps:
        #         print(f'\nstep: {step}')
        #         main(acause, step)
        main(acause, step)


