import numpy as np
import pandas as pd
from pymc import gp
from numexpr import evaluate as EV
import multiprocessing as mp
import re



def adjust_predictions(matrix, linear_floor, df_sub, response):
    '''
    (array, float, data frame, str)

    Given an 2-D array "matrix" which is a set of predictions adjusts the
    upper and lower limits of the matrix so now value is outside of the range.
    linear_floor and df_sub are used to calculate wether a prediction is
    outside of the range.
    '''
    if response == "ln_rate":
        matrix[matrix < np.log(linear_floor/100000.)] = \
            np.log(linear_floor/100000.)
        ceiling = np.expand_dims(np.log((df_sub["envelope"] /
                                         df_sub["pop"]).values), axis=1)
        need_replace = (matrix > ceiling).astype(np.int8)
        matrix = (matrix * 0**need_replace) + (ceiling * need_replace)
    elif response == "lt_cf":
        x = (linear_floor/100000.) / (df_sub["envelope"]/df_sub["pop"])
        floor = np.expand_dims(np.log(x / (1 - x)), axis=1)
        need_replace = (matrix < floor).astype(np.int8)
        matrix = (matrix * 0**need_replace) + (floor * need_replace)
    return matrix


def linear_draw_prediction(dic, df, linear_floor, response, draw_count):
    '''
    (dictionary, data frame, float, str, int) -> array

    Takes in two data frames, "df" is the full data frame used in the
    course of the analysis and ko is a data frame indicating which rows are
    used for training, and different phases of testing. In addition to
    returning an array of predicted values, the self.RMSE value is also
    updated to show the out of sample predictive validity on test set 1.
    '''

    beta_draws = np.array([np.dot(np.linalg.cholesky(dic["vcov"]),
                                  np.random.normal(size=len(dic["vcov"]))) +
                           dic["fix_eff"].values.ravel()
                           for i in range(draw_count)]).T
    preds = np.dot(df[dic["variables"][1:]], beta_draws[1:,:])
    beta_draws = beta_draws[0,:]
    preds = preds + beta_draws
    ran = np.array([dic["ran_eff"][x].loc[df[x]].values.ravel() for
                    x in dic["ran_eff"].keys()]).sum(0)[...,np.newaxis]
    preds = preds + ran
    preds = adjust_predictions(preds, linear_floor, df, response)
    return preds


def linear_draws(list_of_dics, df, linear_floor, response_list, draws):
    '''
    (list, data frame, float, list, list) -> array

    Makes draws for all linear models and return the results in a single
    2-dimensional array where each row is an observation and each column
    is a draw from a particular linear model parameterization.
    '''
    preds = [linear_draw_prediction(list_of_dics[i], df, linear_floor,
                                    response_list[i], draws[i])
             for i in range(len(draws)) if draws[i] != 0]
    if len(preds) != 0:
        return np.hstack(preds).astype("float32")
    else:
        return []


def ca_realizations(gpr_dict, years, draws):
    '''
    (dict, array, int) -> matrix

    Make draws for a specific country age group.
    '''
    realizations = np.array([gp.Realization(gpr_dict["M"], gpr_dict["C"])(years)
                            for d in range(draws)]).T
    return realizations


def ca_gpr_draw_map(inputs):
    ca_gpr_data, years, draws = inputs
    return ca_gpr_draw(ca_gpr_data, years.values, draws)


def new_gpr_draws(ko_gpr_data, df, ca_df, draws):
    preds = np.zeros((len(df), sum(draws)))
    inputs = [{"age": ca_df.age.values[i], "loc": ca_df.location_id.values[i]}
              for i in range(len(ca_df))]
    inputs = [([ko_gpr_data[j][inputs[i]["age"]][inputs[i]["loc"]] for j in range(len(ko_gpr_data))],
               df[(df.age == inputs[i]["age"]) & (df.location_id == inputs[i]["loc"])]["year"],
               draws) for i in range(len(inputs))]
    new_draws = map(ca_gpr_draw_map, inputs)
    for i in range(len(inputs)):
        preds[inputs[i][1].index.values, :] = new_draws[i]
    return preds


def ca_gpr_draw(ca_gpr_data, years, draws):
    preds = np.zeros((len(years), sum(draws))).astype("float32")
    start = 0
    for i in range(len(ca_gpr_data)):
        ca_data = ca_realizations(ca_gpr_data[i], years, draws[i])
        end = start + draws[i]
        preds[:, start:end] = ca_data
    return preds


def linear_draws_map(inputs):
    '''
    (list) -> array

    Helper function for parallel application of linear draw function across all
    knockout patterns.
    '''
    list_of_dics, df, linear_floor, response_list, draws = inputs
    return linear_draws(list_of_dics, df, linear_floor, response_list, draws)


def gpr_draws_map(inputs):
    '''
    (list) -> array

    Helper function for parallel application of gpr draw function across all
    knockout patterns.
    '''
    ko_gpr_data, df, ca_df, draws = inputs
    return new_gpr_draws(ko_gpr_data, df, ca_df, draws)


def new_gpr_draws(df2, response, amplitude, prior, scale, has_data, draws):
    '''
    (data frame, str, float, data frame, int, int) -> array

    Using the input parameters given above runs an instance of gaussian process
    smoothing in order to account for years where we do not have data. The data
    frame (df2) is specific to a location-age.
    '''
    all_indices = df2.index
    data_indices = df2[(df2.ix[:, -4])].index
    years = df2.loc[all_indices]["year"].values
    data = pd.DataFrame({"year":years, "prior":prior})
    data.sort_values("year", inplace=True)
    data.drop_duplicates(inplace=True)
    def mean_function(x):
        return np.interp(x, data.year.values, data.prior.values)
    M = gp.Mean(mean_function)
    C = gp.Covariance(eval_fun=gp.matern.euclidean, diff_degree=2,
                      amp=amplitude, scale=scale)
    df4 = df2.loc[data_indices]
    if has_data:
        gp.observe(M=M, C=C, obs_mesh=df4.year.values,
                   obs_vals=df4[response].values,
                   obs_V=df4[response + "_sd"].values +
                   df4[response + "_nsv"].values)
    ca_draws = np.array([gp.Realization(M, C)(years) for d in range(draws)]).T
    return ca_draws


def age_group_gpr_draw(ca_df, age_amplitude, preds, response_list, scale, draws):
    has_data = ca_df.ix[:, -4].sum() > 0
    var_type = ca_df.variance_type.values[0]
    preds = np.hstack([new_gpr_draws(ca_df, response_list[i],
                                     age_amplitude[var_type][i], preds[:, i],
                                     scale, has_data, draws[i])
                       for i in range(len(draws)) if draws[i] != 0])
    return preds


def age_group_gpr_draw_map(inputs):
    ca_df, age_amplitude, preds, response_list, scale, draws = inputs
    return age_group_gpr_draw(ca_df, age_amplitude, preds, response_list, scale, draws)


def new_ko_gpr_draws(df, ko, amplitude, preds, variance, response_list, scale, draws):
    df2 = pd.concat([df, ko, variance], axis=1)
    random_draws = np.zeros((len(df2), sum(draws))).astype("float32")
    ca_df = df2.loc[ko.ix[:, 2], ["location_id", "age"]].drop_duplicates()
    inputs = [{"df": df2.loc[(df2.age == ca_df.age[i]) &
                             (df2.location_id == ca_df.location_id[i])],
               "age": ca_df.age[i], "loc": ca_df.location_id[i]}
              for i in ca_df.index.values]
    inputs = [(inputs[i]["df"], amplitude[inputs[i]["age"]],
               preds[inputs[i]["df"].index.values, :],
               response_list, scale, draws)
              for i in range(len(inputs))]
    p = mp.Pool(30)
    new_draws = p.map(age_group_gpr_draw_map, inputs)
    p.close()
    p.join()
    for i in range(len(new_draws)):
        random_draws[inputs[i][0].index.values, :] = new_draws[i]
    return random_draws


def new_ko_gpr_draw_map(inputs):
    df, ko, amplitude, preds, variance, response_list, scale, draws = inputs
    return new_ko_gpr_draws(df, ko, amplitude, preds, variance, response_list, scale, draws)
