'''
Module used to train the model birth weight model, transform draws, then
upload. Namely:
1. Train linear regression of mean birthweight based off prevalence under 2500
2. Apply model to under 2500 draws to get mean birthweight for each demographic
3. Upload the results.
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


def index_draws_by_demographics(df):
    '''
    Input: dataframe with location, year, sex, measure, and draws
    Outpur: Draws indexed by demographics and other columns dropped
    '''
    draw_cols = ["draw_{i}".format(i=i) for i in range(0, 1000)]
    index = ['location_id', 'year_id', 'sex_id', 'measure_id']
    good_stuff = draw_cols + index
    df = df[good_stuff]
    return_df = df.set_index(index)
    return return_df


def predict_for_simple_ols(predictor, mean_parameters, cov_matrix):
    '''
    Input: a predictor value, a numpy array representing parameter estimates
    from an OLS, and a 2x2 covariance matrix
    Output: a predicted value where the parameters are smapled using the
    covariance matrix
    '''
    sampled_params = np.random.multivariate_normal(mean_parameters, cov_matrix)
    predicted_val = sampled_params[0] + (sampled_params[1] * predictor)
    return predicted_val


def save_to_hdf(df, save_file):
    '''
    Input: dataframe with location, year, sex, measure, age_group, and draws
    Output: saves the dataframe to one hdf file given the save_file parameter
    '''
    save_df = df.copy()
    index = ['location_id', 'year_id', 'sex_id', 'measure_id', 'age_group']
    save_df = save_df.reset_index()
    # save as continuous since this is a weird model
    save_df['measure_id'] = 19
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
    ols_model = smf.ols('birth_weight ~ bw_under_2500',
                        data=training_data).fit()
    mean_parameters = ols_model.params
    # might be wrong about this being covariance matrix. Also consider
    # cov_params() or other robust covariance matrices.
    cov_matrix = ols_model.cov_HC0

    # set demographic data
    me_id = 8691
    location_set = 35
    gbd_round = 4
    locations = get_most_detailed(location_set, gbd_round)
    years = [1990, 1995, 2000, 2005, 2010, 2016]
    ages = [164]
    sexes = [1, 2]
    measure = 5
    upload_me = 15803

    # grab the u_2500 birth prevalence
    prev_df = draws(source='dismod',
                    gbd_ids={"modelable_entity_ids": [me_id]},
                    location_ids=locations, year_ids=years,
                    age_group_ids=ages, sex_ids=sexes,
                    status='best', measure_ids=[measure],
                    gbd_round_id=gbd_round)
    prev_df = index_draws_by_demographics(prev_df)

    def mapping(x):
        y = predict_for_simple_ols(x, mean_parameters, cov_matrix)
        return y

    mean_weight_df = prev_df.applymap(mapping)
    mean_weight_df['age_group_id'] = 2
    save_to_hdf(mean_weight_df, savefile)
    description = ('Estimate of mean birth weight from simple linear'
                   'regression. Units in grams')
    al.save_custom_results(meid=upload_me, description=description,
                           input_dir=output_dir,
                           sexes=sexes, mark_best=True, env='prod',
                           years=years, custom_file_pattern="all_draws.h5",
                           h5_tablename="draws")
