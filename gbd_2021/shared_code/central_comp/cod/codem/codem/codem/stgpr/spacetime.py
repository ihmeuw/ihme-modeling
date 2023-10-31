import logging

import numexpr as ne
import numpy as np
import pandas as pd
from numexpr import evaluate as EV
from numpy.lib.stride_tricks import as_strided as stride

logger = logging.getLogger(__name__)


def location_depth(full, train):
    """
    (data frame, data frame) -> array

    Given two data frames ("full", "train") where train is a subset of full
    to train a model returns a numpy array with each element representing the
    greatest level of representation that an observation had in the
    training set. This value ranges from 4 to 0 based on whether an observation
    had representation in the training model to the sub-national, country,
    region, super region or no representation.
    """
    D = {d: train[d].unique() for d in ["level_1", "level_2",
                                        "level_3", "location_id"]}
    level = np.array([((full.location_id != full.level_3) &
                       (full.location_id.map(lambda x: x in D["location_id"]))).values]
                     + [full[d].map(lambda x: x in D[d]).values for d in
                        ["level_3", "level_2", "level_1"]]).astype(np.int8)
    return level.sum(axis=0)


def calc_xi_vec(depth, zeta_space_smooth, zeta_space_smooth_nodata):
    """
    (int, float, float) -> array

    Given an integer from 0 to 4 representing the level of representation
    an observation has in the training set ("depth"), and two float
    values for possible use of xi value ("zeta_space_smooth", "zeta_space_smooth_nodata") returns
    an array of length 4 showing the different weighting values to use for
    spatial weighting. If the depth value given is less than three
    then zeta_space_smooth is used else zeta_space_smooth_nodata is used. The first value of the
    array represents the weight to use for data with the same subnational
    value while the last represents the weight to use for data with the same
    super region. The returned array should always add up to 1.
    """
    xi = zeta_space_smooth**(1 - int(depth < 3)) * zeta_space_smooth_nodata**(int(depth < 3))
    xi_vec = [xi] + [xi * (1-xi)**n for n in range(1,depth)]
    xi_vec[-1] /= xi
    xi_vec = np.concatenate([[0 for x in range(4-len(xi_vec))], xi_vec])
    return xi_vec


def calculate_xi_matrix(full, train, zeta_space_smooth, zeta_space_smooth_nodata):
    """
    (data frame, data frame, float, float) -> array

    Given two data frames ("full", "train") where train is a subset of full used
    to train a model and two float values for possible use of xi value
    ("zeta_space_smooth", "zeta_space_smooth_nodata") returns a matrix of xi values with a number
    of rows equal to the number of observations in the training data and 5
    columns. Each cell is given a value depending on the weighting that should
    be used for each observation in the full set in comparison to training
    observations if they share the same sub_national, country, region or
    super region for columns 2 through 5. The first column is the weight of
    data that is to the most specific level for that observation but not
    representative. Each row sum should add up to 1.
    """
    depths = location_depth(full, train)
    f = lambda x : calc_xi_vec (x, zeta_space_smooth, zeta_space_smooth_nodata)
    base = np.array(list(map(f, depths)))
    train_copy = train.set_index("location_id")
    non_rep_loc = train[train.national != 1].location_id.unique()
    non_rep = full.location_id.map(lambda x: x in non_rep_loc)
    non_rep_vec = EV("zeta_space_smooth * (non_rep - non_rep * zeta_space_smooth)")
    # keep track of the place that only have no rep data so we can give them the full location weight
    only_non_rep_loc = np.setdiff1d(non_rep_loc, train[train.national == 1].location_id.unique())
    only_non_rep = full.location_id.map(lambda x: x in only_non_rep_loc)
    modify_SN = non_rep_vec * (base[:, 0] != 0).astype(int)
    modify_C = non_rep_vec * (base[:, 0] == 0).astype(int)
    base[:, 0] = base[:, 0] - modify_SN
    base[:, 1] = base[:, 1] - modify_C
    base = np.append(non_rep_vec.reshape(len(base), 1), base, 1)
    base[only_non_rep.values & (depths == 3), 0] = \
        base[only_non_rep.values & (depths == 3), :][:, [0, 2]].sum(axis=1)
    base[only_non_rep.values & (depths == 3), 2] = 0
    base[only_non_rep.values & (depths == 4), 0] = \
        base[only_non_rep.values & (depths == 4), :][:, [0, 1]].sum(axis=1)
    base[only_non_rep.values & (depths == 4), 1] = 0
    return base


def calculate_lambda_array(full, train, lambda_time_smooth, lambda_time_smooth_nodata):
    """
    (data frame, data frame, float, float) -> array

    Given two data frames ("full", "train") where train is a subset of full used
    to train a model and two float values for possible use of lambda values
    depending on whether data is represented at least to the country level.
    Returns an array of length equal to the number of rows in full with
    either lambda_time_smooth or lambda_time_smooth_nodata.
    """
    country = train.level_3.unique()
    have_country_data = full.level_3.map(lambda x: x in country).astype(int)
    lam = have_country_data.map(lambda x: lambda_time_smooth**(x) * lambda_time_smooth_nodata**(1-x))
    return lam.values


## Weighting Functions


def makeS(full, train, var):
    """
    Given 2 dataframes, "train" which is a subset of "full" used to train a
    model, and a variable name, makes a new matrix of a row length(n) equal to
    the row length of the training data frame and a column length(m) equal to
    the row length of the full data frame. Each column is a repeat of the
    values of the selected variable from the training data frame. The new
    matrix created however only takes up the memory space of what a single
    column does.
    """
    ncol = len(full)
    vec = train[var].values
    return stride(vec, shape=(len(vec), ncol), strides=(vec.itemsize, 0))


def timeW(full_sub, train_sub, omega_age_smooth, time_weight_method, lambda_time_smooth,
          lambda_time_smooth_nodata, year_start, year_end):
    """
    Gets the time age weight of a super region given a full and training data
    set. Returns a matrix of size equal to the row size of the training data set
    by the row size of the full data set each subset by the super region.
    Each cell represents the age by time weight for each observation
    (the column) for each residual (the row).
    """
    ageS = makeS(full_sub, train_sub, "ageC").astype("float32")  # make stride of age values
    i_age = full_sub.ageC.values.astype("float32")  # age vector
    age_weight_function = "1 / exp(omega_age_smooth * abs(ageS - i_age))"

    yearS = makeS(full_sub, train_sub, "year").astype("float32")  # stride of year values
    i_year = full_sub.year.values.astype("float32")  # year vector
    lambda_time = calculate_lambda_array(full_sub, train_sub, lambda_time_smooth,
                                         lambda_time_smooth_nodata).astype("float32")  # assign lambda
    if time_weight_method == "tricubic":
        aMax = np.maximum(EV("abs(i_year - year_start)"), EV("abs(year_end - i_year)")).astype("float32")  # argMax vector
        time_weight_function = "(1 - (abs(yearS - i_year) / (aMax + 1))**lambda_time)**3"
    elif time_weight_method == "exponential":
        time_weight_function = "1 / exp(lambda_time * abs(yearS - i_year))"

    return EV(f"({age_weight_function}) * ({time_weight_function})")


def matCRS(full_sub, train_sub):
    """
    For a designated super region returns 3 matrices where each column is an
    observation from the full data frame and each row is a residual from
    the training data set. The three matrices have either values of 1 or 0
    and designate whether the residual and the observation are in the same
    country(C), same region but not same country(R), and same super_region but
    not the same region or country(S).
    """
    sub_nat_S = makeS(full_sub, train_sub, "location_id")
    country_S = makeS(full_sub, train_sub, "level_3")
    region_S = makeS(full_sub, train_sub, "level_2")
    sub_nat_V = full_sub.location_id.values
    country_V = full_sub.level_3.values
    region_V = full_sub.level_2.values
    has_sub_nat_V = (train_sub.level_3 !=
                     train_sub.location_id).values.astype(np.int8)
    not_representitive_V = (0**train_sub.national.values).astype(np.int8)
    SN = EV("sub_nat_S == sub_nat_V").astype(np.int8).T
    NR = EV("SN * not_representitive_V").T
    SN = EV("SN * has_sub_nat_V").T
    C = EV("country_S == country_V").astype(np.int8)
    C = EV("C - SN")
    SN = EV("SN * 0**NR")
    C = EV("C * 0**NR")
    R = EV("region_S == region_V").astype(np.int8)
    R = EV("R - C - SN - NR")
    SR = EV("1 - R - C - SN - NR")
    return NR, SN, C, R, SR


def weight_matrix(valid_positions, xi_vector, weight_matrix):
    """
    (matrix, vector, matrix)

    Given a matrix of valid positions for an analytic region (valid_positions),
    a vector of appropriate xi weights to use for each column in that vector
    (xi_vector), and an age year weighted matrix generated by timeW will return
    a matrix re-weighted so that each column adds up to the corresponding xi
    value in the xi_vector.
    """
    weights = EV("valid_positions * weight_matrix")
    sum_of_weights = weights.sum(0)
    sum_of_weights[sum_of_weights == .0] = 1.
    return EV("(weights / sum_of_weights) * xi_vector")


def spacetime(indices, sReg, df, ko, omega_age_smooth, time_weight_method, lambda_time_smooth,
              lambda_time_smooth_nodata, zeta_space_smooth, zeta_space_smooth_nodata):
    """
    Compute the spacetime weight matrix for a super region. Full data set tells
    which values need weights, train data set are the residuals which need
    weighting.
    """
    full_sub = df.iloc[indices, :]
    train_sub = df[(df.level_1 == sReg) & (ko.iloc[:, 0])]
    year_start = np.min(df.year)
    year_end = np.max(df.year)
    Wat = timeW(full_sub, train_sub, omega_age_smooth, time_weight_method, lambda_time_smooth,
                lambda_time_smooth_nodata, year_start, year_end).astype("float32")
    NR, SN, C, R, SR = matCRS(full_sub, train_sub)
    xi_mat = calculate_xi_matrix(full_sub, train_sub, zeta_space_smooth,
                                 zeta_space_smooth_nodata).astype("float32")
    NR = weight_matrix(NR, xi_mat[:, 0], Wat).astype("float32")
    SN = weight_matrix(SN, xi_mat[:, 1], Wat).astype("float32")
    C = weight_matrix(C, xi_mat[:, 2], Wat).astype("float32")
    R = weight_matrix(R, xi_mat[:, 3], Wat).astype("float32")
    SR = weight_matrix(SR, xi_mat[:, 4], Wat).astype("float32")
    del Wat, xi_mat
    final = EV("NR + SN + C + R + SR").astype("float32")
    del NR, SN, C, R, SR
    account_missing = final.sum(0)
    account_missing[account_missing == .0] = 1.
    return EV("final / account_missing").astype("float32")


def to_numeric(series):
    """
    (series) -> series

    Convert a pandas series into a numeric series
    """
    unique = series.unique()
    encoding = {unique[i]: i for i in range(len(unique))}
    return series.apply(lambda x: encoding[x])


def make_2_dimensional(array):
    """
    :param array: either one or 2 dimensional array
    :return: array with 2 dimensions
    """
    if array.ndim >= 2:
        return array
    else:
        return np.atleast_2d(array).T


def spacetime2(reg, sReg, df, ko, omega_age_smooth, time_weight_method, lambda_time_smooth,
               lambda_time_smooth_nodata, zeta_space_smooth, zeta_space_smooth_nodata):
    """
    Compute the spacetime weight matrix for a super region. Full data set tells
    which values need weights, train data set are the residuals which need
    weighting.
    """
    full_sub = df[(df.level_2 == reg)]
    train_sub = df[(df.level_1 == sReg) & (ko)]
    year_start = np.min(df.year)
    year_end = np.max(df.year)
    Wat = timeW(full_sub, train_sub, omega_age_smooth, time_weight_method, lambda_time_smooth,
                lambda_time_smooth_nodata, year_start, year_end).astype("float32")
    NR, SN, C, R, SR = matCRS(full_sub, train_sub)
    xi_mat = calculate_xi_matrix(full_sub, train_sub, zeta_space_smooth,
                                 zeta_space_smooth_nodata).astype("float32")
    NR = weight_matrix(NR, xi_mat[:, 0], Wat).astype("float32")
    SN = weight_matrix(SN, xi_mat[:, 1], Wat).astype("float32")
    C = weight_matrix(C, xi_mat[:, 2], Wat).astype("float32")
    R = weight_matrix(R, xi_mat[:, 3], Wat).astype("float32")
    SR = weight_matrix(SR, xi_mat[:, 4], Wat).astype("float32")
    final = EV("NR + SN + C + R + SR").astype("float32")
    del NR, SN, C, R, SR
    account_missing = final.sum(0)
    account_missing[account_missing == .0] = 1.
    return EV("final / account_missing").astype("float32")


def super_region_positions(reg, sReg, df, ko):
    """
    (str, str, data frame, data frame)

    Given a string representing a valid super region, a data frame of cause
    data and a knock out data frame returns a dictionary with each position
    for that super region in the training and in the ful data set with null
    data removed.
    """
    full = df.copy()
    full = full.reset_index(drop=True)
    full_rows = full[full.level_2 == reg].index.values
    train = full[ko]
    train = train.reset_index(drop=True)
    train_rows = train[train.level_1 == sReg].index.values
    return {"full_rows": full_rows, "train_rows": train_rows}


def single_st(reg, sReg, df, res_mat, ko, omega_age_smooth, time_weight_method,
              lambda_time_smooth, lambda_time_smooth_nodata, zeta_space_smooth,
              zeta_space_smooth_nodata):
    """
    Applies a single instance of spacetime weighting for a single super
    region. Results are stored in the self.st_smooth_mat matrix in the
    appropriate rows for that super region.
    """
    pos = super_region_positions(reg, sReg, df, ko)
    st = spacetime2(reg, sReg, df, ko, omega_age_smooth, time_weight_method,
                    lambda_time_smooth, lambda_time_smooth_nodata,
                    zeta_space_smooth, zeta_space_smooth_nodata)
    residuals = res_mat[pos["train_rows"], :]
    return np.dot(st.T, residuals)
