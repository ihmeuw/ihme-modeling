
'''
Description: Calculates absolute survival estimates for each uid and saves a
    dataframe with uid, survival month, and absolute survival
How To Use:
'''

from cancer_estimation.py_utils import (
    common_utils as utils,
    data_format_tools as dft,
    pandas_expansions as pe
)
from cancer_estimation._database import cdb_utils as cdb
import cancer_estimation.c_models.e_nonfatal.nonfatal_dataset as nd
from cancer_estimation.py_utils.gbd_cancer_tools import validate_proportions
from sys import argv, exit
import numpy as np
from os.path import isfile
import pandas as pd
from time import time



def decomp_prefix_cols(faux_correct): 
    ''' This function will take in faux_correct boolean, and return a string that 
        specifies whether 'decomp' should be added as a prefix to column names
    '''
    if faux_correct == True: 
        decomp_string = 'decomp_'
    else: 
        decomp_string = ''
    return(decomp_string) 


def load_lambda_file(cnf_model_version_id):
    ''' Using the cnf_lambda_version_id, returns the datestamp suffix
            of that version
    '''
    lambda_file_default = utils.get_path(
        "lambda_values", process="nonfatal_model")
    record = nd.get_run_record(cnf_model_version_id)
    lambda_version = record.at[0, 'cnf_lambda_version_id']
    db_link = cdb.db_api()
    this_version = db_link.get_entry(table_name='cnf_lambda_version', 
                                uniq_col='cnf_lambda_version_id',
                                val=lambda_version)
    suffix = str(this_version.at[0, 'date_generated'])
    lambda_file = lambda_file_default.replace("<date>", suffix)
    return(lambda_file)


def load_surv_folder(cnf_model_version_id):
    ''' Using the cnf_lambda_version_id, returns the datestamp suffix
            of that version
    '''
    surv_folder = surv_folder = utils.get_path("relative_survival",
                                             process="nonfatal_model")
    record = nd.get_run_record(cnf_model_version_id)
    rs_version = record.at[0, 'rel_survival_version_id']
    db_link = cdb.db_api()
    this_version = db_link.get_entry(table_name='rel_survival_version', 
                                uniq_col='rel_survival_version_id',
                                val=rs_version)
    suffix =  str(this_version.at[0, 'date_generated'])
    rs_folder = surv_folder.replace("<date>", suffix)
    return(rs_folder)


def load_lambda_values(location_id, cnf_model_version_id):
    ''' Loads and returns lambda estimates for the given location_id
    '''
    print("       loading lambdas...")
    lambda_value_file = load_lambda_file(cnf_model_version_id)
    lambdas = pd.read_csv(lambda_value_file)
    lambdas = lambdas.loc[lambdas['location_id'] == location_id, :]
    return(lambdas)


def load_rel_surv_values(acause, location_id, cnf_model_version_id, faux_correct):
    ''' Loads and returns survival best-case/worst-case estimations for the given
            acause
    '''
    print("       loading survival...")
    decomp_str = decomp_prefix_cols(faux_correct)
    uid_cols = nd.nonfatalDataset().uid_cols
    uid_cols = uid_cols + ['surv_year']
    scaled_survival = nd.get_columns('{}scaled_survival'.format(decomp_str))
    sex_restrictions = {'neo_prostate': 1, 'neo_testicular': 1,
                        'neo_cervical': 2, 'neo_ovarian': 2, 'neo_uterine': 2}
    # Load specific input based on version_id
    surv_folder = load_surv_folder(cnf_model_version_id)
    input_file = "{}/{}/{}.csv".format(surv_folder, acause, location_id)
    # import and update names
    this_surv = pd.read_csv(input_file)
    this_surv = this_surv.loc[this_surv['surv_year'] <= 10,]
    this_surv.rename(columns={'year': 'year_id',
                              'sex': 'sex_id'}, inplace=True)
    # Add 'year 0' survival equal to 1 (no time has passed through which to survive)
    tmp = this_surv.loc[this_surv['surv_year'].eq(1),]
    tmp['surv_year'] = 0
    tmp[scaled_survival] = 1 
    this_surv = this_surv.append(tmp)
    # Subset by sex
    if acause in sex_restrictions.keys():
        this_surv = this_surv.loc[this_surv['sex_id']
                                  == sex_restrictions[acause],: ]
    # Test and return
    assert not this_surv.isnull().any().any(), \
        "Null values found in relative survival input after formatting"
    validate_proportions(this_surv[scaled_survival])
    return(this_surv)


def _fix_survival_ages(this_surv):
    ''' Adjusts age categories to extend '80+' categories if not all extended
            age categories are present 
    '''
    age_groups = this_surv.age_group_id.unique() 
    youngest_ages = [2, 3, 4, 5]
    oldest_ages = [30, 31, 32, 235]
    if 1 in age_groups and any(a for a in youngest_ages if a not in age_groups):
        young_age_data = this_surv.loc[this_surv['age_group_id'].eq(1),:].copy()
        for a in youngest_ages:
            if a not in age_groups:
                young_age_data['age_group_id'] = a
                this_surv = this_surv.append(young_age_data)
    if 21 in age_groups and any(a for a in oldest_ages if a not in age_groups):
        oldest_age_data = this_surv.loc[this_surv['age_group_id'].eq(21),:].copy()
        for a in oldest_ages:
            if a not in age_groups:
                oldest_age_data['age_group_id'] = a
                this_surv = this_surv.append(oldest_age_data)
    this_surv = this_surv[~this_surv['age_group_id'].isin([1,21])]
    return(this_surv)


def create_estimation_frame(acause, location_id, cnf_model_version_id, faux_correct):
    ''' Returns a dataframe containing the ages and covariates used to estimate
            survival and incremental mortality
    '''
    print("    creating estimation frame...")
    # inc_input = load_inc_data(acause)
    max_surv = nd.nonfatalDataset().max_survival_months
    uid_cols = nd.nonfatalDataset().uid_cols
    keep_ages = list(range(1, 21)) + list(range(30, 34)) + [235]
    # load and subset survival curve to match the estimation parameters
    surv_data = load_rel_surv_values(acause, location_id, cnf_model_version_id, faux_correct)
    surv_data['survival_month'] = surv_data['surv_year'] * 12
    surv_data = surv_data.loc[surv_data['survival_month'] <= max_surv]
    # merge with lambda values to create the survival estimation_frame
    lambda_input = load_lambda_values(location_id, cnf_model_version_id)
    estim_frame = surv_data.merge(lambda_input[uid_cols + ['lambda']])
    estim_frame = estim_frame.loc[estim_frame['age_group_id'].isin(
        keep_ages), :]
    estim_frame['lambda_years'] = (
        estim_frame['lambda'] * (estim_frame['surv_year']))
    return(estim_frame)


def calc_abs_surv(df, acause, faux_correct):
    ''' Returns the dataframe with estimate of absolute survival for the requested draw_num
    '''
    # Generate absolute survival draws
    print("    estimating absolute survival")
    decomp_str = decomp_prefix_cols(faux_correct)
    if len(decomp_str) > 0: 
        max_draws = 100
    else: 
        max_draws = 1000
    abs_surv_col = nd.get_columns("{}absolute_survival".format(decomp_str))
    for i in list(range(0,max_draws)): 
        #df.rename(columns={'{}_{}'.format('surv_abs',i): '{}_{}_{}'.format('decomp','surv_abs',i)}, 
        #        inplace=True)
        df.loc[:, '{}_{}'.format('surv_abs',i)] = \
            (df['{}_{}'.format('scaled_surv',i)]*np.exp(df['lambda_years'])).clip(upper=1, lower=0)
    validate_proportions(df[abs_surv_col])
    return(df)


def generate_estimates(acause, location_id, cnf_model_version_id, faux_correct=False):
    ''' Runs the survival estimation pipeline
    '''
    faux_correct = False
    print("Beginning survival estimation...")
    estim_frame = create_estimation_frame(acause, location_id, cnf_model_version_id, faux_correct)
    output_df = calc_abs_surv(estim_frame, acause, faux_correct)
    nd.save_outputs("survival", output_df, acause)


if __name__ == "__main__":
    acause = argv[1]
    location_id = int(argv[2])
    cnf_model_version_id = int(argv[3])
    faux_correct = argv[4]
    faux_correct = False
    generate_estimates(acause, location_id, cnf_model_version_id, faux_correct)
