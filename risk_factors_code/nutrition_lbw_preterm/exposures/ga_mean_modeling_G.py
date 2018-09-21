'''
Module used to train the model gestational age mode, transform draws, then
upload. Namely:
1. Train linear regression of mean gestational age based off gestational age
under 37 weeks, and an upper SDI dummy variable.
2. Apply model to under 37 week draws to get mean birthweight for each
demographic
3. Upload the results.

Ideally in the future:
1. Explore other models besides linear regression (hierarchical models)
2. Understand relation to haqi or other indicators better
3. Clean data more (years past 1995 excluded for Spain but maybe more)
4. Split the training and calculations into seperate models (perhaps Pickle)
'''

import numpy as np
import pandas as pd
import os
import statsmodels.formula.api as smf
from transmogrifier.gopher import draws
from db_queries import get_location_metadata
from adding_machine import agg_locations as al


def get_most_detailed(location_set, gbd_round):
    location_df = get_location_metadata(location_set_id=location_set,
                                        gbd_round_id=gbd_round)
    location_df = location_df[location_df['most_detailed'] == 1]
    location_list = location_df['location_id'].tolist()
    return location_list


def index_by_demographics(df, other_keep_cols=[]):
    '''
    Input: dataframe with location, year, sex, measure
    Outpur: Draws along with other columns indexed by demographics and other
    columns dropped
    '''
    draw_cols = ["draw_{i}".format(i=i) for i in range(0, 1000)]
    index = ['location_id', 'year_id', 'sex_id', 'measure_id', 'age_group_id']
    good_stuff = draw_cols + index + other_keep_cols
    df = df[good_stuff]
    return_df = df.set_index(index)
    return return_df


def generate_sample_params(mean_parameters, cov_matrix):
    '''
    Input: mean parameters vector (from an OLS model for example) and
    and a corresponding covariance matrix
    Output: a vector of sampled parameters assuming the parameters are
    multivariate normally distributed
    '''
    sample_params = np.random.multivariate_normal(mean_parameters, cov_matrix)
    return sample_params


def apply_model(prev_draws, mean_params, cov_matrix):
    '''
    Samples 1000 parameters of the model and calculates the output of the model
    for columnwise.

    Inputs: A dataframe of prevalence draws along with the mean parameters and
    cov_matrix of the model.
    Outputs: A dataframe of draws where the model has been applied.
    '''

    # for each draw, generate a column of the upper_SDI dataframe and apply
    # the intercept and prevalence transformations to the ga_u_37 frame
    for i in range(0, 1000, 1):
        params = generate_sample_params(mean_params, cov_matrix)
        intercept_coeff, ga_u_37_coeff, _ = params
        draw = 'draw_{}'.format(i)
        prev_draws[draw] = intercept_coeff + ga_u_37_coeff * prev_draws[draw]
    # create frames and sum together to get the results
    results_df = prev_draws
    return results_df


def save_to_hdf(df, save_file):
    '''
    Input: dataframe with location, year, sex, measure, age_group, and draws
    Output: saves the dataframe to one hdf file given the save_file parameter
    '''
    save_df = df.copy()
    index = ['location_id', 'year_id', 'sex_id', 'measure_id', 'age_group']
    save_df.to_hdf(save_file, key="draws", format="table",
                   data_columns=index)


if __name__ == '__main__':

    # define directories and filepaths
    work_dir = os.path.dirname(os.path.realpath(__file__))
    data_file = '{}FILEPATH'.format(work_dir)
    output_dir = 'FILEPATH'
    savefile = '{}/FILEPATH'.format(output_dir)
    # load training dataset
    training_data = pd.read_csv(data_file)
    # fit model, get mean and covariance matrix of parameters
    ols_model = smf.ols('mean_ga ~ ga_u_37 + upper_SDI',
                        data=training_data).fit()
    mean_params = ols_model.params
    # might be wrong about this being covariance matrix. Also consider
    # cov_params() or other robust covariance matrices.
    cov_matrix = ols_model.cov_HC0

    # set demographic data
    me_id = 8675
    location_set = 35
    gbd_round = 4
    locations = get_most_detailed(location_set, gbd_round)
    years = [1990, 1995, 2000, 2005, 2010, 2016]
    ages = [164]
    sexes = [1, 2]
    measure = 5
    upload_me = 15802
    # grab the u_3700 birth prevalence and replace age_group and measure ids
    prev_df = draws(source='dismod',
                    gbd_ids={"modelable_entity_ids": [me_id]},
                    location_ids=locations, year_ids=years,
                    age_group_ids=ages, sex_ids=sexes,
                    status='best', measure_ids=[measure],
                    gbd_round_id=gbd_round)
    prev_df['age_group_id'] = 2
    prev_df['measure_id'] = 19

    # index draws and apply model
    indexed_draws_df = index_by_demographics(prev_df)
    mean_ga_df = apply_model(indexed_draws_df, mean_params, cov_matrix)

    # reset index before saving (to not interfere with save_results)
    mean_ga_df = mean_ga_df.reset_index()
    save_to_hdf(mean_ga_df, savefile)
    description = ('Estimate of mean gestational age using multilinear '
                   'regression. Regressed on prevelance of gestationa age '
                   'under 37 weeks, and dummy variable upper SDI '
                   '(defined as high high-middle SDI). '
                   'Units in weeks. Dummy not used in prediction.')

    al.save_custom_results(meid=upload_me, description=description,
                           input_dir=output_dir,
                           sexes=sexes, mark_best=True, env='prod',
                           years=years, custom_file_pattern="all_draws.h5",
                           h5_tablename="draws")
