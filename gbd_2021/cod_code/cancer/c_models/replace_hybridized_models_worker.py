'''
Description: Worker script for replace_hybridized_models.py, runs for one 
            location. Saves the hybridized model for the specific location
            in preparation for upload
How to Use: python replace_hybridized_models_worker.py <acause> <location_id> <sex_id>
Arguments:  acause (str) : cancer 
            location_id (int) : sex_id for the given model
            sex_id (int) : sex_id for the given model
Resource Requirements:
    - >= 40 GB Ram 
    - = 1 cores/threads
    - must run on either gbd or cancer environment
Contributors: USERNAME
'''

# import libraries 
import sys 
import os 
from sys import argv
import pandas as pd
from cancer_estimation.py_utils import common_utils as utils 
import cancer_estimation.c_models.e_nonfatal.nonfatal_dataset as nd 


def validate_squareness(df_sub : pd.DataFrame, acause : str):
    ''' Check data is still square for each sex
    '''
    EXP_YEARS = list(range(utils.get_gbd_parameter("min_year_cod"), 
                 utils.get_gbd_parameter("max_year")+1))
    check_years = set(EXP_YEARS) - set(df_sub['year_id'].unique().tolist())
    nd.check_zero_values_exist(df_sub, 'draw_cols', acause)
    nd.validate_expected_ages(df_sub, 'draw_cols', acause)
    assert check_years == set()


def export_model_locations(acause, location_id, sex_id): 
    ''' Creates a working directory, then splits the models by 
        locations and saves to directory 
    '''
    codem_path = utils.get_path("codem_hybridize_outputs", process = "cancer_model")
    final_model = pd.read_csv("{}/{}/sex_id_{}/final_model.csv".format(
        codem_path, acause, sex_id))
    work_dir = '{}/{}/sex_id_{}'.format(codem_path, acause, sex_id)
    utils.ensure_dir(work_dir) # create directory if it doesn't already exist 
    print('saving results by location to...{}'.format(work_dir))

    sub = final_model.loc[final_model['location_id'].eq(location_id)]
    validate_squareness(sub, acause)
    sub.to_csv('{}/{}.csv'.format(work_dir, location_id), index = False)
    print('files exported to {}! ready to use save_results_cod'.format(work_dir))
    return(None)


def run_array_job():
    '''
    '''
    params = pd.read_csv(sys.argv[1])
    task_id = int(os.environ["SGE_TASK_ID"])
    acause = params.loc[task_id-1, 'acause']
    location_id = int(params.loc[task_id-1, 'location_id'])
    sex_id = int(params.loc[task_id-1, 'sex_id'])
    export_model_locations(acause, location_id, sex_id)
    return  


def run_serially(): 
    ''' Run jobs one at a time
    '''
    acause = argv[1]
    location_id = int(argv[2])
    sex_id = int(argv[3])
    export_model_locations(acause, location_id, sex_id)
    return 


if __name__ == "__main__":
    ''' update this to read from param file''' 
    print(len(sys.argv))
    if len(sys.argv) == 2: # array job was submitted from launcher
        run_array_job()
    else: 
        run_serially() 