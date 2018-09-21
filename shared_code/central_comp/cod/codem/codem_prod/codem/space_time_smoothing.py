import pandas as pd
import numpy as np
from model_list import Model_List
from model import Model
import multiprocessing as mp
import json


def import_json(f):
    '''
    (str) -> list of lists

    Given a a string leading to a valid JSON file will return a list
    of lists with the terminal nodes being dictionaries that can later
    be converted to data frames or matrices.
    '''
    json_data = open(f)
    data = json.load(json_data)
    json_data.close()
    return data

def import_cv_vars(f):
    '''
    (str) -> list of lists

    Given a a string leading to a valid JSON file will return a list
    of lists with the terminal nodes being dictionaries that can later
    be converted to data frames or matrices.
    '''
    data = import_json(f)
    data = data["rate_vars"] + data["cf_vars"]
    data = {x: data for x in ["space", "mixed"]}
    return data


def lt_to_ln(array, envelope, population):
    '''
    (array, array, array) -> array

    Converts an array of lt_cf values to ln_rate. All input arrays must be of
    the same length.
    '''
    return np.log((np.exp(array) / (1 + np.exp(array))) *
                  envelope[..., np.newaxis] / population[..., np.newaxis])


def uniform_predictions(df, pred_mat, response_list):
    '''
    (data frame, array, array) -> array

    Converts a knockout prediction data frame to all predicted ln_rate values
    so that comparison of metrics such as RMSE are uniform across models.
    '''
    lt_cols = [i for i, x in enumerate(response_list) if x == "lt_cf"]
    new_mat = np.copy(pred_mat)
    new_mat[:, lt_cols] = lt_to_ln(pred_mat[:, lt_cols], df["envelope"].values,
                                   df["pop"].values)
    return new_mat


def extract(model):
    '''
    (model) -> dict
    '''
    return {"fix_eff": model.fix_eff, "ran_eff": model.ran_eff,
            "variables": model.variables}


def adjust_predictions(vector, linear_floor, df_sub, response):
    '''
    (array, float, data frame, str)

    Given an 1-D array "vector" which is a set of predictions adjusts the
    upper and lower limits of the vector so now value is outside of the range.
    linear_floor and df_sub are used to calculate wether a prediction is
    outside of the range.
    '''
    if response == "ln_rate":
        vector[vector < np.log(linear_floor/100000.)] = \
            np.log(linear_floor/100000.)
        ceiling = np.log((df_sub["envelope"] / df_sub["pop"]).values)
        need_replace = (vector > ceiling).astype(np.int8)
        vector = (vector * 0**need_replace) + (ceiling * need_replace)
    elif response == "lt_cf":
        x = (linear_floor/100000.) / (df_sub["envelope"]/df_sub["pop"])
        floor = np.log(x / (1 - x))
        need_replace = (vector < floor).astype(np.int8)
        vector = (vector * 0**need_replace) + (floor * need_replace)
    return vector


def make_prediction(dic, df, ko, linear_floor, response):
    '''
    (dictionary, data frame, data frame) -> array

    Takes in two data frames, "df" is the full data frame used in the
    course of the analysis and ko is a data frame indicating which rows are
    used for training, and different phases of testing. In addition to
    returning an array of predicted values, the self.RMSE value is also
    updated to show the out of sample predictive validity on test set 1.
    '''
    ran_eff = dic["ran_eff"]
    fix_eff = dic["fix_eff"]
    variables = dic["variables"]
    df_sub = df.copy()
    fix = (np.dot(df_sub[variables[1:]], fix_eff.values[1:]) +
           fix_eff.values[0]).ravel()
    ran = np.array([ran_eff[x].loc[df_sub[x]].values.ravel() for
                    x in ran_eff.keys()]).sum(0)
    df_sub["predictions"] = adjust_predictions(fix+ran, linear_floor,
                                               df_sub, response)
    return df_sub["predictions"].values


def make_all_predictions(list_o_dics, df, ko, linear_floor, response_list):
    '''
    (list of dictionaries, data frame, data frame, float, list of str) -> array

    Make all predictions using the list of dictionaries which contain the base
    parameters (fixed effects and random effects) for calculating expected
    values. Each column contains the predictions for a specific model.
    '''
    preds = [make_prediction(list_o_dics[i], df, ko, linear_floor,
                             response_list[i]) for i in range(len(list_o_dics))]
    return np.array(preds).T


def make_all_residuals(preds, df, ko, response_list):
    '''
    (array, data frame, data frame, list of str) -> array

    Creates an array that contains all the residuals of each model for a given
    knockout pattern. Only in sample residuals are calculated for space time
    use. Each column contains the residuals for a specific model.
    '''
    ko2 = ko.copy()
    ko2 = ko2.reset_index(drop=True)
    keep_rows = ko2[ko2.ix[:,0]].index.values
    df_sub = df.copy()
    res_mat = df_sub[response_list].values - preds
    return res_mat[keep_rows, :]


def g(inputs):
    '''
    (list) -> list of arrays

    Helper function that allows for a parallelized version of make predictions
    function across all knockout patterns.
    '''
    dic, df, ko, linear_floor, response = inputs
    return make_all_predictions(dic, df, ko, linear_floor, response)


def h(inputs):
    '''
    (list) -> list of arrays

    Helper function that allows for a parallelized version of make residuals
    function across all knockout patterns.
    '''
    preds, df, ko, response_list = inputs
    return make_all_residuals(preds, df, ko, response_list)


def make_all_models(df, ko_all, linear_floor, json_dict, make_preds):
    '''
    (data frame, list of data frames, float, dictionary)

    Parallelized calculations of all predictions and residuals for either
    linear or space time models. Returns a list of model list objects which
    contain predictions, residuals, and a list of class model which contain
    model parameter information.
    '''
    df["super_region"] = df.super_region.astype(str)
    temp = pd.core.reshape.get_dummies(df.age)
    temp.columns = ["age" + str(int(s)) if s >= 1 else "age" + str(s)
                    for s in temp.columns]
    df2 = pd.concat([df, temp], axis=1)
    keys = sorted(json_dict.keys())
    kos = sorted(json_dict[json_dict.keys()[0]].keys())
    response_list = [x.rsplit("_", 1)[0] for x in keys]
    models = [[Model(json_dict[keys[x]][k], response_list[x])
               for x in range(len(keys))] for k in kos]
    f = lambda i: Model_List(models[i], df2, ko_all[i], linear_floor)
    model_list = map(f, range(len(models)))
    if make_preds:
        dics = [[extract(ml.models[i]) for i in range(len(ml.models))] for ml in model_list]
        inputs = [[dics[i], df2, ko_all[i], linear_floor, response_list]
                  for i in range(len(dics))]
        preds = map(g, inputs)
        for i in range(len(ko_all)):
            model_list[i].pred_mat = preds[i].astype('float32')
        inputs = [[preds[i], df2, ko_all[i], response_list]
                  for i in range(len(dics))]
        res = map(h, inputs)
        for i in range(len(ko_all)):
            model_list[i].res_mat = res[i].astype('float32')
    return model_list


def apply_smoothing(list_of_model_lists, df, ko_all, omega_age_smooth,
                    lambda_time_smooth, lambda_time_smooth_nodata,
                    zeta_space_smooth, zeta_space_smooth_nodata):
    '''
    (list of model_lists) -> none

    Applies space time to all models all all knockouts.
    '''
    tempAge = list(df.age.unique())
    df["ageC"] = np.array([tempAge.index(x) for x in list(df.age)])
    for i in range(len(list_of_model_lists)):
        list_of_model_lists[i].spacetime_predictions(df, ko_all[i],
                                                     omega_age_smooth,
                                                     lambda_time_smooth,
                                                     lambda_time_smooth_nodata,
                                                     zeta_space_smooth,
                                                     zeta_space_smooth_nodata)
    return None


def reset_residuals(list_of_model_lists, df, ko_all, response_list):
    '''
    (list of model_lists, data frame, list of data frames, list) -> None

    Reset the Residual data frame of all the models for post spacetime results.
    '''
    for i in range(len(list_of_model_lists)):
        list_of_model_lists[i].reset_residuals(response_list, ko_all[i], df)


def bound_gpr_draws(df, matrix, linear_floor):
    '''
    (data_frame, matrix, float) <- matrix

    Bounds the GPR draws so that they are not above or below a reasonable value
    for a particular location-age-year.
    '''
    matrix[matrix < np.log(linear_floor/100000.)] = \
        np.log(linear_floor/100000.)
    ceiling = np.expand_dims(np.log((df["envelope"] /
                                     df["pop"]).values), axis=1)
    need_replace = (matrix > ceiling).astype(np.int8)
    matrix = (matrix * 0**need_replace) + (ceiling * need_replace)
    return matrix
