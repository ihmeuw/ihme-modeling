import numpy as np
import pandas as pd
from pymc import gp
from tqdm import tqdm
import multiprocessing as mp
import logging

logger = logging.getLogger(__name__)


def adjust_predictions(matrix, linear_floor, df_sub, response):
    """
    (array, float, data frame, str)

    Given an 2-D array "matrix" which is a set of predictions adjusts the
    upper and lower limits of the matrix so now value is outside of the range.
    linear_floor and df_sub are used to calculate wether a prediction is
    outside of the range.
    """
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
    """
    (dictionary, data frame, float, str, int) -> array

    Takes in two data frames, "df" is the full data frame used in the
    course of the analysis and ko is a data frame indicating which rows are
    used for training, and different phases of testing. In addition to
    returning an array of predicted values, the self.RMSE value is also
    updated to show the out of sample predictive validity on test set 1.
    """
    logger.info(f"Making {draw_count} draws for {response}.")
    beta_draws = np.array([np.dot(np.linalg.cholesky(dic["vcov"]),
                                  np.random.normal(size=len(dic["vcov"]))) +
                           dic["fix_eff"].values.ravel()
                           for i in range(draw_count)]).T
    preds = np.dot(df[dic["variables"][1:]], beta_draws[1:,:])
    beta_draws = beta_draws[0, :]
    preds = preds + beta_draws
    ran = np.array([dic["ran_eff"][x].loc[df[x]].values.ravel() for
                    x in list(dic["ran_eff"].keys())]).sum(0)[...,np.newaxis]
    preds = preds + ran
    preds = adjust_predictions(preds, linear_floor, df, response)
    return preds


def linear_draws(list_of_dics, df, linear_floor, response_list, draws):
    """
    (list, data frame, float, list, list) -> array

    Makes draws for all linear models and return the results in a single
    2-dimensional array where each row is an observation and each column
    is a draw from a particular linear model parameterization.
    """
    logger.info("Creating linear draws.")
    preds = [linear_draw_prediction(list_of_dics[i], df, linear_floor,
                                    response_list[i], draws[i])
             for i in range(len(draws)) if draws[i] != 0]
    if len(preds) != 0:
        logger.info("There are some linear draws.")
        return np.hstack(preds).astype("float32")
    else:
        logger.info("There are no linear draws.")
        return []


def linear_draws_map(inputs):
    """
    (list) -> array

    Helper function for parallel application of linear draw function across all
    knockout patterns.
    """
    list_of_dics, df, linear_floor, response_list, draws = inputs
    return linear_draws(list_of_dics, df, linear_floor, response_list, draws)


def new_gpr_draws(df2, response, amplitude, prior, scale, has_data, draws):
    """
    (data frame, str, float, data frame, int, int) -> array

    Using the input parameters given above runs an instance of gaussian process
    smoothing in order to account for years where we do not have data. The data
    frame (df2) is specific to a location-age.
    """
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
                   obs_V=df4[response + "_sd"].values**2 +
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
    logger.info("Creating GPR draws by knockouts.")
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
    new_draws = list(p.map(age_group_gpr_draw_map, tqdm(inputs)))
    p.close()
    p.join()
    for i in range(len(new_draws)):
        random_draws[inputs[i][0].index.values, :] = new_draws[i]
    return random_draws


def new_ko_gpr_draw_map(inputs):
    df, ko, amplitude, preds, variance, response_list, scale, draws = inputs
    return new_ko_gpr_draws(df, ko, amplitude, preds, variance, response_list, scale, draws)
