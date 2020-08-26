import logging
import multiprocessing as mp
import numpy as np
from tqdm import tqdm

from codem.ensemble.PV import rmse_out_map, trend_out_map

logger = logging.getLogger(__name__)


def rank_array(array):
    """
    (array) -> array
    Rank each value in an array from 1 to N where N is the number of elements in
    the array with the lowest value is one and the highest value is N.
    """
    temp = array.argsort()
    ranks = np.empty(len(array), int)
    ranks[temp] = np.arange(len(array))
    return ranks


def gangnam_weight(ranked_array, psi, cutoff):
    """
    (array of integers, float, int) -> array

    Given an array of ranks assigns a psi value to each element in the array
    corresponding to the rank of the element. The higher the rank (the closer to
    one the rank is) the higher the psi weight it will receive. Any value above
    teh cutoff value will receive a value of zero. The returned array should sum
    to one.
    """
    N = min([len(ranked_array), 100])
    denominator = float(sum(psi**(N - ranked_array)))
    psi_weighted = psi**(N-ranked_array) / denominator
    psi_weighted *= (ranked_array <= cutoff)
    return psi_weighted


def psi_weights(space_err, lin_err, psi, cutoff):
    """
    (array, array, float, int) -> tuple of 2 arrays

    Given two equal length arrays of values corresponding to the error of space
    time models and error of linear models alongside a psi value by which to
    weight the models returns two arrays with the corresponding psi weight for
    each model such that the sum of the two arrays should add to one. The first
    array should be applied to the space time models while the second should be
    applied to the linear models.
    """
    array = np.append(space_err, lin_err)
    ranks = rank_array(array)
    psi_values = gangnam_weight(ranks, psi, cutoff)
    M = int(len(psi_values) / 2)
    return psi_values[:M], psi_values[M:]


def psi_draws(space_err, lin_err, psi, cutoff):
    """
    (array, array, float, int) -> tuple of 2 arrays

    Given two equal length arrays of values corresponding to the error of space
    time models and error of linear models alongside a psi value by which to
    weight the models returns two arrays with the corresponding number of draws
    for each model such that the sum of the two arrays should add to one. The
    first array should be applied to the space time models while the second
    should be applied to the linear models.
    """
    array = np.append(space_err, lin_err)
    ranks = rank_array(array)
    psi_values = gangnam_weight(ranks, psi, cutoff)
    draws = (psi_values * 1000).astype(int)
    leftover = 1000 - sum(draws)
    draws[np.argmax(draws)] += leftover
    M = int(len(psi_values) / 2)
    ranks = 1 + np.array(ranks)
    return draws[:M], draws[M:], ranks[:M], ranks[M:]


def psi_predict(space_pred, lin_pred, space_err, lin_err, psi, cutoff):
    """
    (array, array, array, array, float, int) -> array

    Given the prediction estimations for models with the same knockout pattern
    apply weights to each model and use the ensemble of the models to make
    predictions. Just like Netflix.
    """
    logger.info(f"Make psi-predictions using means for psi value {psi}")
    space_weights, lin_weights = psi_weights(space_err, lin_err, psi, cutoff)
    space_weights = space_weights
    lin_weights = lin_weights
    new_pred = (space_pred * space_weights) + (lin_pred * lin_weights)
    return new_pred.sum(axis=1)


def psi_range(space_pred, lin_pred, space_err, lin_err, psi_values, cutoff):
    """
    (array, array, array, array, array, int) -> array

    Make ensemble predictions using a range of psi values. Returns an array of
    values with a number of rows equal to the number of observations in
    space_pred and lin_pred and a number of columns equal to the number of
    values in psi range.
    """
    psi_pred = np.array(
        [psi_predict(
            space_pred, lin_pred, space_err, lin_err, x, cutoff
        ) for x in psi_values]
    )
    return psi_pred.T


def psi_map(inputs):
    """
    (list) -> array

    Helper function that allows for a parallelized version of psi ensemble
    prediction across all knockout patterns.
    """
    space_pred, lin_pred, space_err, lin_err, psi_values, cutoff = inputs
    p = psi_range(space_pred, lin_pred, space_err, lin_err, psi_values, cutoff)
    return p


def ensemble_all(space_models, linear_models, psi_values, cutoff):
    """
    (all_model, all_model, array, int) -> list of arrays

    Generate the ensemble predictions across al knockouts for various values of
    psi.
    """
    logger.info("Generating ensemble predictions across all knockouts for various "
                "values of psi using just means * weights.")
    inputs = [(space_models.all_models[i].pred_mat,
               linear_models.all_models[i].pred_mat, space_models.RMSE +
               space_models.trend, linear_models.RMSE + linear_models.trend,
               psi_values, cutoff)
              for i in range(len(space_models.all_models) - 1)]
    p = mp.Pool(20)
    ensemble_predictions = np.array(list(p.map(psi_map, tqdm(inputs))))
    p.close()
    p.join()
    return ensemble_predictions


def rmse_ensemble_out(list_of_preds, data_frame, knockouts):
    """
    (list of model_lists, data frame, list of data frames) ->
    array

    Calculate the RMSE of all ensembles across all knockout patterns and
    average the results to be used for ranking at a later time. An array of
    length equal to the number of psi_values is is returned with the median
    RMSE.
    """
    logger.info("Calculating OOS RMSE for ensembles with different psi values.")
    inputs = [(data_frame, knockouts[i], list_of_preds[i])
              for i in range(len(list_of_preds))]
    p = mp.Pool(20)
    rmse_all = np.array(list(p.map(rmse_out_map, tqdm(inputs))))
    p.close()
    p.join()
    return np.median(rmse_all, axis=0)


def trend_ensemble_out(list_of_preds, data_frame, knockouts, window):
    """
    (list of model_lists, data frame, list of data frames, list of str) ->
    array

    Calculate the trend of all ensembles across all knockout patterns and
    average the results to be used for ranking at a later time. An array of
    length equal to the number of psi_values is is returned with the median
    trend.
    """
    logger.info("Calculating OOS trend for ensembles with different psi values.")
    inputs = [(data_frame, knockouts[i], list_of_preds[i], window)
              for i in range(len(list_of_preds))]
    p = mp.Pool(20)
    trend_all = np.array(list(p.map(trend_out_map, tqdm(inputs))))
    p.close()
    p.join()
    return np.median(trend_all, axis=0)


def best_psi(data_frame, knockouts, window, space_models, linear_models, psi_values, cutoff):
    """
    (data frame, list of dfs, int, all_model, all_model, array, int) -> float

    Get the best value of psi using the median value across all knockouts.
    """
    logger.info("Getting the best psi value using median rmse and trend error across all knockouts.")
    ens_preds = ensemble_all(space_models, linear_models, psi_values, cutoff)
    rmse = rmse_ensemble_out(ens_preds, data_frame, knockouts)
    trend = trend_ensemble_out(ens_preds, data_frame, knockouts, window)
    best_rmse = rmse[np.argmin(rmse + trend)]
    best_trend = trend[np.argmin(rmse + trend)]
    psi = psi_values[np.argmin(rmse + trend)]
    d1, d2, r1, r2 = psi_draws(space_models.RMSE + space_models.trend,
                               linear_models.RMSE + linear_models.trend, psi, cutoff)
    return psi, d1, d2, best_rmse, best_trend, r1, r2
