''' Name: liver_hotfix.py
    Description: 
        - Code that exports and uploads custom models draws for hepatoblastoma (HBL
        ) for <10 yo 
        using liver parent.
        - model_version_ids used are provided by modeler who models liver parent.
        - make_best in save_results_cod is set to False
    Arguments: 
        - male_model_id - int, the version_id of the male liver parent model
        - female_model_id - int, the version_id of the female liver parent model
    How to Run: 
        python liver_hotfix.py <male_model_version_id> <female_model_version_id>

    Contributors: USERNAME
'''


# libraries 
import os 
import pandas as pd
from get_draws.api import get_draws 
from save_results import save_results_cod
from db_queries import (
    get_location_metadata, get_cause_metadata
)
# team modules 
import cancer_estimation.c_models.e_nonfatal.nonfatal_dataset as nd 
from cancer_estimation.py_utils import common_utils as utils

# parameters/globals 
EXP_YEARS = list(range(utils.get_gbd_parameter("min_year_cod"), 
                 utils.get_gbd_parameter("max_year")+1))
EXP_AGES = [2,3,388,389,238,34,6]
OUTDIR = 'FILEPATH'
GBD_ID = utils.get_gbd_parameter("current_gbd_round")
DECOMP_STEP = utils.get_gbd_parameter("current_decomp_step")
CAUSES = get_cause_metadata(cause_set_id = 2, gbd_round_id = GBD_ID)
LIVER_ID = CAUSES.loc[CAUSES['acause'].eq("neo_liver"), 'cause_id'].values[0]
HBL_ID = CAUSES.loc[CAUSES['acause'].eq("neo_liver_hbl"), 'cause_id'].values[0]


def combine_and_export_draws(male_model_id :int , female_model_id : int) -> None:
    ''' Retrieves, saves the custom model draws and checks to 
        make sure all expected parameters exist
    '''
    model_dict = {"male" :male_model_id, "female" : female_model_id}
    liv_parents = []

    def _validate_squareness(df_sub : pd.DataFrame):
        ''' Check data is still square for each sex and expected values are present
        '''
        check_years = set(EXP_YEARS) - set(df_sub['year_id'].unique().tolist())
        check_ages = set(EXP_AGES) - set(df_sub['age_group_id'].unique().tolist())
        nd.check_zero_values_exist(df_sub, 'draw_cols', 'neo_liver_hbl')
        nd.validate_expected_ages(df_sub, 'draw_cols', 'neo_liver_hbl')
        assert check_years == set() and check_ages == set()

    for d in model_dict.keys():
        # pull liver model from given model_id 
        liv_parent = get_draws(gbd_id_type='cause_id', 
                                gbd_id=LIVER_ID,
                                source='codem',
                                measure_id=1, # mortality  
                                metric_id=1, # count space
                                version_id= model_dict[d], 
                                gbd_round_id=GBD_ID,
                                decomp_step=DECOMP_STEP)
        # subset data to age restrction of HBL
        liv_parent = liv_parent.loc[liv_parent['age_group_id'].isin(EXP_AGES), ]
        assert len(liv_parent) > 0, \
            "pulled liver parent draws are empty for version_id {}".format(d)
        _validate_squareness(liv_parent)
        liv_parents.append(liv_parent)

    # saving and checking by location
    df_sub = pd.concat(liv_parents)
    loc_list = nd.list_location_ids()
    assert len(loc_list) > 0, "data is all missing locations"

    for l in loc_list: 
        sub = df_sub.loc[df_sub['location_id'].eq(l), ]
        if nd.validate_data_square(sub, 'draw_cols', 'neo_liver_hbl',False, nd.nonfatalDataset('','neo_liver_hbl')):
            del sub['cause_id']
            sub.to_hdf('{}/both/{}.h5'.format(OUTDIR, l), key='draws', mode='w', 
                                                          format = 'fixed')
            print('data exported!')
    return()


def main(male_model_id :int, female_model_id : int) -> None: 
    ''' Runs the custom model for upload
    '''
    combine_and_export_draws(male_model_id, female_model_id)
    save_results_cod(input_dir ='{}/both'.format(OUTDIR), # directory that has both sexes 
                    input_file_pattern='{location_id}.h5',
                    cause_id=HBL_ID,
                    description='<10 Liver parent for HBL',
                    metric_id=1, 
                    decomp_step=DECOMP_STEP,
                    gbd_round_id=GBD_ID,
                    mark_best=False)
    
    return

if __name__ == "__main__": 
    import sys
    utils.check_gbd_parameters_file()
    male_model_id = int(sys.argv[1]) 
    female_model_id = int(sys.argv[2])
    main(male_model_id = male_model_id, 
         female_model_id = female_model_id)