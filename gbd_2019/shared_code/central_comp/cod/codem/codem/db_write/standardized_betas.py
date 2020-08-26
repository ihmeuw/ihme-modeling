import pandas as pd
import numpy as np
import logging
import re

from codem.ensemble.model import dic2df

logger = logging.getLogger(__name__)


def remove_unnecessary_coefficients(var_list):
    """
    Get rid of the intercept and age variables.
    :param var_list: list of strings
    :return: a list of the elements to keep
    """
    age_columns = [x for x in var_list if re.search('^age[0-9]', x)]
    good_vars = [x for x in var_list if x not in age_columns and x != '(Intercept)']
    return good_vars


def purge_fe(df, column):
    """
    Get rid of unwanted fixed effects from data frame:
    the age fixed effects and intercept.

    :param df: input data frame
    :param column: column with names for fixed effects
    :return df: data frame without rows for age fixed effects or intercept
    """
    keep_vars = remove_unnecessary_coefficients(df[column].values)
    df = df.loc[df[column].isin(keep_vars)]
    return df


def get_fe_and_var(d, key, mtype, all_data_holdout):
    """
    Get fixed effects and standard error for a model.

    :param d: dictionary with model type, model names, and knockouts
    :param key: model index in key
    :param mtype: model type -- mixed or spacetime
    :param all_data_holdout: the last holdout that contains all the data
    :return fix_eff: data frame with covariate fixed effects and standard error for a given submodel.
    """
    d_sub = d[mtype][key][all_data_holdout]
    fix_eff = dic2df(d_sub["fix_eff"]).reset_index()

    vcov = d_sub["vcov"]
    se = np.sqrt(np.diagonal(vcov))
    fix_eff["se"] = se

    fix_eff = purge_fe(df=fix_eff, column='_row')
    fix_eff.rename(columns={'_row': 'covariate'}, inplace=True)

    fix_eff["model"] = key
    fix_eff["type"] = mtype

    return fix_eff


def calc_sd(df, covariate_df, vartype, name):
    """
    Calculates the standard deviation of a variable in the mod inputs
    Either a covariate or an outcome variable.

    :param df: data frame for the codem model
    :param covariate_df: covariate df from codem model
    :param vartype: (str) one of "covariate" or "outcome"
    :param name: (str) name of the covariate or name of outcome
    :return std: (float) the standard deviation of the input variable
    """
    if vartype == "covariate":
        var = covariate_df[name]
    else:
        data = df.copy()
        if name == "rate":
            var = np.log(data["cf"] * data["envelope"] / data["pop"])
        else:
            var = np.log(data["cf"].map(lambda x: x / (1.0 - x)))
    std = np.std(var)
    return std


def get_sd_ratio(df, covariate_df, covariate, outcome):
    """
    Gets the ratio of the standard deviations. This is done in a separate
    function than the standardizing betas because it takes a while. So just saving
    the ratios cuts down so that we don't have to compute this for every draw.

    :param df: data frame for the codem model
    :param covariate_df: covariate df from codem model
    :param covariate: (str) name of covariate
    :param outcome: (str) outcome being predicted (so rate or cf)
    :return ratio: (float) ratio of the standard deviations
    """
    cov_sd = calc_sd(df=df, covariate_df=covariate_df,
                     vartype="covariate", name=covariate)
    out_sd = calc_sd(df=df, covariate_df=covariate_df,
                     vartype="outcome", name=outcome)
    ratio = cov_sd / out_sd
    return ratio


def make_ratio_dict(df, covariate_df, covariates):
    """
    Makes a dictionary of all of the standard deviation ratios for the list of covariates, by model type.

    :param df: data frame for the codem model
    :param covariate_df: covariate df from codem model
    :param covariates: list of covariates
    :return rdict: (dict) dictionary like {covariate: ratio}
    """
    rdict = {}
    for mtype in ["rate", "cf"]:
        covdict = {}
        for cov in covariates:
            covdict[cov] = get_sd_ratio(df=df, covariate_df=covariate_df,
                                        covariate=cov, outcome=mtype)
        rdict[mtype] = covdict
    return rdict


def standardize_beta(covariate, outcome, value, rat_dict):
    """
    Creates a standardized beta by multiplying the beta value by the ratio of the standard
    deviation of the outcome to the standard deviation of the covariate.

    :param covariate: (str) covariate name
    :param outcome: (str) outcome type (rate or cf)
    :param value: (float) un-standardized beta value
    :param rat_dict: (dict) dictionary with keys for model type and covariate with values for std. ratios
    :return standard: (float) standardized beta value
    """
    ratio = rat_dict[outcome][covariate]
    standard = value * ratio
    return standard


def weighted(x, cols, weight):
    """
    Get weighted average for each column based on weight col. Used in a groupby call.

    :param x: (dataframe) data frame from groupby
    :param cols: (list) column names to apply this over
    :param weight: (str) name of the weight column to use
    :return: series of weighted average
    """
    return pd.Series(np.average(x[cols], weights=x[weight], axis=0), cols)


def make_draw(fix_eff, vcov):
    """
    Make one draw from the beta mean + variance-covariance matrix.

    :param fix_eff: (array) fixed effects array
    :param vcov: (array) the variance-covariance matrix for the submodel
    :return: (array) beta draw for all fixed effects
    """
    return np.dot(np.linalg.cholesky(vcov), np.random.normal(size=len(vcov))) + fix_eff.values.ravel()


def make_ndraws_df(n_draws, fix_eff, vcov, response, rat_dict, scale):
    """
    Make n draws from the beta mean + variance-covariance matrix. Also standardizes them for an additional column.

    :param n_draws: (int) number of draws for this submodel
    :param fix_eff: (array) fixed effects array
    :param vcov: (array) variance-covariance matrix of this submodel
    :param response: (str) type of response (lt or cf)
    :param rat_dict: (dict) dictionary of ratios of standard deviations of outcome / covariate by model type
                            and covariate combination
    :param scale: (int) how much to scale up the draws by -- will take (n_draws * scale) number of draws
    :return df: data frame
    """
    n_draws = int(n_draws)
    df = [make_draw(fix_eff, vcov) for x in list(range(n_draws)) * scale]

    df = pd.DataFrame(df)
    df.columns = fix_eff.index
    df = pd.melt(df)

    df.columns = ['covariate', 'beta']

    df = purge_fe(df, "covariate")

    df["standard_beta"] = df.apply(lambda x: standardize_beta(covariate=x['covariate'], outcome=response,
                                                              value=x['beta'], rat_dict=rat_dict), axis=1)
    return df




