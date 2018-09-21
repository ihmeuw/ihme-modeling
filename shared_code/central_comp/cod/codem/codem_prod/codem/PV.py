import multiprocessing as mp
import numpy as np
from numexpr import evaluate as EV


def cartesian(arrays, out=None):
    '''
    (list of array_like, array) -> array

    2-D array of shape (M, len(arrays)) containing cartesian products formed of
    input arrays.
    '''

    arrays = [np.asarray(x) for x in arrays]
    dtype = arrays[0].dtype

    n = np.prod([x.size for x in arrays])
    if out is None:
        out = np.zeros([n, len(arrays)], dtype=dtype)

    m = n / arrays[0].size
    out[:,0] = np.repeat(arrays[0], m)
    if arrays[1:]:
        cartesian(arrays[1:], out=out[0:m,1:])
        for j in xrange(1, arrays[0].size):
            out[j*m:(j+1)*m,1:] = out[0:m,1:]
    return out


def year_trend_rmse(unit_df, ln_predictions, year, adjacent_years):
    '''
    (data frame, array, float, list of int) -> array

    For a given year and all adjacent years finds the RMSE of the slopes
    between years referred to as the trend. An array with a number of columns
    equal to the number of models in a given knockout. The unit data frame
    (unit_df) refers to a data frame for a specific location age.
    '''
    errors = np.vstack([slope_rmse(unit_df, ln_predictions, year, y2)
                        for y2 in adjacent_years])
    return errors


def slope_rmse(unit_df, ln_predictions, year1, year2):
    '''
    (data frame, array, float, float) -> array

    Given a location age data frame (unit_df) and two years finds the slope
    between the two years in log rate space and compares the expected to the
    observed values to generate a trend RMSE. The number of columns corresponds
    to the number of models for a given knockout while the rows correspond to
    the unique pair combination of data points for each year (year1 & year2).
    '''
    year1_df = unit_df[(unit_df.year == year1)]
    year2_df = unit_df[(unit_df.year == year2)]
    diffs = cartesian([year1_df.index.values, year2_df.index.values])
    obs_change = np.array([year2_df.loc[y[1], "ln_rate"] -
                           year1_df.loc[y[0], "ln_rate"] for y in diffs])
    obs_slope = obs_change / float(year2 - year1)
    est_change = np.array([ln_predictions[y[1], :] - ln_predictions[y[0], :]
                           for y in diffs])
    est_slope = est_change / float(year2 - year1)
    return (est_slope - obs_slope[..., np.newaxis])**2


def country_age_trend(unit_df, window, ln_predictions):
    '''
    (data frame, float, array) -> array

    Get the trend errors for a given country age. If no data within the window
    exists return an array of NaNs that we will get rid of later.
    '''
    years = sorted(unit_df.year.unique())
    adjacent_years = [[year + i + 1 for i in range(window) 
                        if year + i + 1 in years] for year in years]
    if any([len(a) > 0 for a in adjacent_years]):
        errors = np.vstack([year_trend_rmse(unit_df, ln_predictions,
                                            years[i], adjacent_years[i]) for i
                            in range(len(years)) if len(adjacent_years[i]) > 0])
    else:
        errors = np.array([np.nan for x in range(ln_predictions.shape[1])])
    return errors


def ko_rmse_out(df, ko, pred_mat):
    '''
    (data frame, data frame, array) -> array

    Get the RMSE of all models for a single knockout pattern. Returns the
    results in an array that is of length equal to the number of models
    present for a single knockout.
    '''
    indices = ko[ko.ix[:, 1]].index
    N = float(sum(ko.ix[:, 1]))
    ln_values = df.loc[indices, "ln_rate"].values[..., np.newaxis]
    return (((pred_mat[indices, :] - ln_values)**2).sum(0) / N)**.5


def rmse_out_map(inputs):
    '''
    (list) -> list of arrays

    Helper function that allows for a parallelized version of RMSE calculation
    across all knockout patterns.
    '''
    df, ko, pred_mat = inputs
    model_trend = ko_rmse_out(df, ko, pred_mat)
    return model_trend


def ko_trend_out(df, ko, pred_mat, window):
    '''
    (data frame, data frame, array, int) -> array

    Get the RMSE of trend of all models for a single knockout pattern. Returns
    the results in an array that is of length equal to the number of models
    present for a single knockout.
    '''
    df2 = df[ko.ix[:,1]]
    locations = df2.location_id.unique()
    ages = df2.age.unique()
    trend = []
    for age in ages:
        for location in locations:
            unit_df = df2[(df2.location_id == location) & (df2.age == age)]
            if len(unit_df) > 1:
                trend += [country_age_trend(unit_df, window, pred_mat)]
    trend = np.vstack(trend)
    trend = trend[~np.isnan(trend).any(axis=1)]
    return (trend.sum(0) / float(len(trend)))**.5


def trend_out_map(inputs):
    '''
    (list) -> list of arrays

    Helper function that allows for a parallelized version of trend calculation
    across all knockout patterns.
    '''
    df, ko, pred_mat, window = inputs
    model_trend = ko_trend_out(df, ko, pred_mat, window)
    return model_trend


def rmse_out(list_of_model_lists, data_frame, knockouts):
    '''
    (list of model_lists, data frame, list of data frames) ->
    array

    Calculate the RMSE of all models across al knockout patterns and average
    the results to be used for ranking at a later time. An array of length
    equal to the number of models in a single instance of class model_list
    is returned with the mean RMSE.
    '''
    inputs = [(data_frame, knockouts[i], list_of_model_lists[i].pred_mat)
              for i in range(len(knockouts) - 1)]
    p = mp.Pool(20)
    rmse_all = np.array(p.map(rmse_out_map, inputs))
    p.close()
    p.join()
    return np.median(rmse_all, axis=0)


def trend_out(list_of_model_lists, data_frame, knockouts, window):
    '''
    (list of model_lists, data frame, list of data frames, list of str) ->
    array

    Calculate the trend of all models across al knockout patterns and average
    the results to be used for ranking at a later time. An array of length
    equal to the number of models in a single instance of class model_list
    is returned with the mean trend.
    '''
    inputs = [(data_frame, knockouts[i], list_of_model_lists[i].pred_mat, 
               window) for i in range(len(knockouts) - 1)]
    p = mp.Pool(20)
    trend_all = np.array(p.map(trend_out_map, inputs))
    p.close()
    p.join()
    return np.median(trend_all, axis=0)


def single_coverage_out(draw_preds, df, ln_rate_nsv, ko_vector):
    '''

    :param model_list:
    :param df:
    :return:
    '''

    mean_draw = draw_preds.mean(axis=1)
    std_draw = draw_preds.std(axis=1)
    var = 1.96 * np.sqrt(df.ln_rate_sd**2 + (1.4826 * ln_rate_nsv)**2
                         + std_draw**2)
    values = (df.ln_rate <= mean_draw + var) & (df.ln_rate >= mean_draw - var)
    values = values[ko_vector]
    return values.sum() / float(len(values))


def coverage_map(inputs):
    '''

    :param inputs:
    :return:
    '''
    draw_preds, df, ln_rate_nsv, ko_vector = inputs
    return single_coverage_out(draw_preds, df, ln_rate_nsv, ko_vector)


def concat2(list_of_two_arrays):
    '''
    (list of arrays) -> array

    Utility function that will concat two arrays along the column axis unless
    one of the arrays is empty in which case just returns the other array.
    '''
    if len(list_of_two_arrays[0]) == 0:
        return list_of_two_arrays[1]
    elif len(list_of_two_arrays[1]) == 0:
        return list_of_two_arrays[0]
    else:
        return np.concatenate(list_of_two_arrays, axis=1)


def coverage_out(st_models, lin_models, knockouts, df):
    '''

    :param list_of_model_types:
    :param knockouts:
    :param data_frame:
    :return:
    '''
    coverage = [single_coverage_out(concat2([st_models.all_models[i].draw_preds,
                                             lin_models.all_models[i].draw_preds],),
                                    df[["ln_rate", "ln_rate_sd"]],
                                    st_models.all_models[i].nsv.ln_rate_nsv.values,
                                    knockouts[i].ix[:, 2].values)
                for i in range(len(knockouts) - 1)]
    return np.array(coverage).mean()


def final_preds(st_preds, lin_preds, st_psi_weights, lin_psi_weights):
    pred = ((st_preds * st_psi_weights) +
            (lin_preds * lin_psi_weights)).sum(axis=1)
    return pred


def rmse_in(pred, observed, ko):
    N = float(sum(ko))
    return (((pred[ko] - observed[ko])**2).sum() / N)**.5


def trend_in(df, ko, pred_vec, window):
    '''
    (data frame, data frame, array, int) -> array

    Get the RMSE of trend of all models for a single knockout pattern. Returns
    the results in an array that is of length equal to the number of models
    present for a single knockout.
    '''
    df2 = df[ko]
    pred_vec = pred_vec[:, np.newaxis]
    locations = df2.location_id.unique()
    ages = df2.age.unique()
    trend = []
    for age in ages:
        for location in locations:
            unit_df = df2[(df2.location_id == location) & (df2.age == age)]
            if len(unit_df) > 1:
                trend += [country_age_trend(unit_df, window, pred_vec)]
    trend = np.vstack(trend)
    trend = trend[~np.isnan(trend).any(axis=1)]
    return (trend.sum(0) / float(len(trend)))**.5
