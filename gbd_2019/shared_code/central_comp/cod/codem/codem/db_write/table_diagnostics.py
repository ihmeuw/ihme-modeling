import pandas as pd
import logging

import codem.data.queryStrings as QS
from codem.ensemble.model import dic2df
from codem.reference import db_connect
import codem.db_write.standardized_betas as sb

logger = logging.getLogger(__name__)


def validation_table(submodel_summary_df, model_pv):
    """
    Predictive validity metrics table.

    :param submodel_summary_df:
    :param model_pv:
    :return:
    """
    logger.info("Validation table.")
    df = QS.validation_table.format(
        RMSE_in_ensemble=model_pv['pv_rmse_in'],
        RMSE_out_ensemble=model_pv['pv_rmse_out'],
        trend_in_ensemble=model_pv['pv_trend_in'][0],
        trend_out_ensemble=model_pv['pv_trend_out'],
        coverage_in_ensemble=model_pv['pv_coverage_in'],
        coverage_out_ensemble=model_pv['pv_coverage_out'],
        RMSE_out_sub=submodel_summary_df[submodel_summary_df["rank"] == 1]["rmse_out"][0],
        trend_out_sub=submodel_summary_df[submodel_summary_df["rank"] == 1]["trend_out"][0]
    )
    df = df.replace("\n", "")
    return df


def get_submodel_summary(model_version_id, db_connection):
    """
    Retrieves the summary submodel rank table for a particular model.
    """
    logger.info("Making submodel summary table.")
    call = QS.submodel_summary_query.format(model_version_id)
    df = db_connect.query(call, db_connection)
    return df


def get_submodel_summary_df(model_version_id, submodel_covariates,
                            submodel_rmse, submodel_trend,
                            db_connection):
    """
    Create submodel-specific summary table (1 row for each submodel) mostly with predictive validity metrics.

    :return df: data frame with submodel-specific summaries
    """
    logger.info("Making submodel summary data frame.")
    df = get_submodel_summary(model_version_id, db_connection)
    df = df.sort_values(['rank', 'submodel_version_id'], ascending=False)
    df = df.drop_duplicates(['rank'], keep="first")
    covs = submodel_covariates["mixed"]
    covs.update(submodel_covariates["space"])
    df["rmse_out"] = df.submodel_version_id.map(lambda x: submodel_rmse[x])
    df["trend_out"] = df.submodel_version_id.map(lambda x: submodel_trend[x])
    df["covariates"] = df.submodel_version_id.map(lambda x: ', '.join(covs[x]))
    df.sort_values("rank", inplace=True)
    df.reset_index(drop=True, inplace=True)
    return df


def get_covariate_summary_df(submodel_summary_df, draw_id):
    """
    Create summary of submodels with n-draws.
    submodel_summary must be made by submodel_summary_table()

    :return submodel_df: submodel data frame
    """
    logger.info("Making covariate summary data frame.")
    submodels = submodel_summary_df.copy()
    df = pd.DataFrame(
        pd.DataFrame(draw_id, columns=["id"]).groupby('id').size()
    ).reset_index()
    df.columns = ['submodel_version_id', 'n_draws']
    submodel_df = submodels.merge(df, how='left', on=['submodel_version_id'])
    return submodel_df


def get_model_type_df(covariate_summary_df):
    """
    Create table of types of models for modeler email.
    Counts the number of mixed/st cf/rate models.
    covariate_summary must be made by covariate_summary_df()

    :return df: data frame for model types
    """
    logger.info("Making model type data frame.")
    df = covariate_summary_df.copy()
    df = df.loc[df.n_draws > 0]
    df["Number"] = 1
    df.rename(columns={'n_draws': 'Draws'}, inplace=True)
    df = df[['Type', 'Dependent_Variable', 'Number', 'Draws']].groupby(
        ['Type', 'Dependent_Variable']
    ).sum()
    df = df.reset_index()
    df.columns = ['Type', 'Dependent Variable',
                  'Number of Models', 'Number of Draws']
    return df


def get_submodel_betas_df(linear_models_json,
                          st_models_json,
                          all_data_holdout):
    """
    Get covariate data frame for all submodels.
    Gives their fixed effects and their standard errors.

    :return df: covariate data frame for all submodels
    :return keys: model names (e.g. ln_rate_model001) in order
    """
    logger.info("Making submodel betas data frame.")
    master_json = {'mixed': linear_models_json,
                   'spacetime': st_models_json}

    keys = sorted(list(linear_models_json.keys()))
    covar_list = [
        [sb.get_fe_and_var(
            d=master_json, key=key, mtype=mtype,
            all_data_holdout=all_data_holdout
        ) for key in keys] for mtype in ['mixed', 'spacetime']
    ]

    df = pd.concat(map(pd.concat, covar_list))
    df.rename(columns={'values': 'beta'}, inplace=True)
    return df, keys


def get_all_submodel_info_df(data_frame, covariate_df, covariate_summary_df, submodel_betas_df,
                             lm_dict, all_data_holdout):
    """
    IMPORTANT: Merge the submodel metadata with predictive validity/n_draws/rank onto the covariate dataframe.
    USES the keys from the submodel. The sorted values are key -- the keys are sorted and the submodel table
    is sorted, and they line up row by row.

    Note: This is the only way to link the json with covariate betas/SE with the submodel metadata.  Do not
    unsort by submodel version ID whatever you do! If you do they keys will align with the wrong models.

    :return df: data frame
    """
    logger.info("Getting all submodel information in a table.")
    submodels = covariate_summary_df.copy().sort_values(['submodel_version_id'])
    beta_df, keys = submodel_betas_df

    submodels['model'] = keys + keys
    submodels.rename(columns={'Type': 'type'}, inplace=True)

    df = beta_df.merge(submodels, on=['model', 'type'])

    covlist = get_covlist(lm_dict, all_data_holdout=all_data_holdout)
    ratio_dict = sb.make_ratio_dict(df=data_frame, covariate_df=covariate_df, covariates=covlist)

    df.loc[df.n_draws.isnull(), 'n_draws'] = 0.0

    df.sort_values(['covariate', 'n_draws'], inplace=True)
    for mtype in ['ln_rate', 'lt_cf']:
        df.loc[df['model'].str.contains(mtype), 'mtype'] = mtype

    df["standard_beta"] = 0.
    for index, row in df.iterrows():
        standard = sb.standardize_beta(covariate=row["covariate"],
                                       outcome=row["Dependent_Variable"],
                                       value=row["beta"], rat_dict=ratio_dict)
        df.set_value(index, 'standard_beta', standard)
    return df


def create_weighted_beta_column(merged_submodels_df):
    """
    Create a data frame with a row for each covariate with weighted betas and standardized betas.

    :return df: data frame
    """
    logger.info("Creating weighted beta column.")
    df = merged_submodels_df.copy()
    df = df.loc[df.n_draws > 0]
    df = df.groupby(df.covariate).apply(
        sb.weighted, ["beta", "standard_beta"], weight="n_draws"
    ).reset_index()

    df.columns = ['name', 'beta', 'standard_beta']
    return df


def get_covlist(d, all_data_holdout):
    """
    Gets a unique list of covariates that is used across all submodels.

    :param d: dictionary of linear model or spacetime model (they have the same covariate lists in aggregate)
    :param all_data_holdout: the holdout for all of the data
    :return covlist: (list) list of covariate names
    """
    covlist = []
    for key in list(d.keys()):
        covs = d[key][all_data_holdout]["fix_eff"]["_row"]
        covlist = covlist + covs
    covlist = list(set(covlist))
    covlist = sb.remove_unnecessary_coefficients(covlist)
    return covlist


def get_submodel_covariate_draws(json, mtype, key, response, n_draws, rat_dict, scale, all_data_holdout):
    """
    Makes a data frame for all covariates in ONE submodel with (n_draws * scale) draws of the betas
    (Also standardizes the betas)

    :param json: (dict) master json dictionary -- both mixed and spacetime models put together
    :param mtype: (str) model type (mixed or spacetime)
    :param key: (str) name of the model (e.g. ln_rate_model001)
    :param response: (str) model response (rate or cf)
    :param n_draws: (int) number of draws for the submodel
    :param rat_dict: (dict) dictionary of ratios of standard deviations of outcome / covariate by model type
                            and covariate combination
    :param scale: (int) how much to scale up the draws by -- will take (n_draws * scale) number of draws
    :param all_data_holdout: the holdout for all of the data
    :return df: data frame
    """
    logger.info("Getting submodel covariate draws.")
    little_json = json[mtype][key][all_data_holdout]
    fix_eff = dic2df(little_json["fix_eff"])
    vcov = little_json["vcov"]

    df = sb.make_ndraws_df(
        n_draws=n_draws, fix_eff=fix_eff,
        vcov=vcov, response=response,
        rat_dict=rat_dict, scale=scale
    )
    df["model_type"] = mtype
    df["model_name"] = key
    df["dependent_variable"] = response
    df["n_draws"] = n_draws
    return df


def get_beta_draws(covariate_summary_df, submodel_betas_df, data_frame, covariate_df, scale, all_data_holdout,
                   lm_dict, st_dict):
    """
    Make beta draws (and standard beta draws) for ALL submodels.

    :param covariate_summary_df: the result of covariate_summary_df()
    :param submodel_betas_df: the result of get_submodel_beta_df()
    :param data_frame: the data
    :param covariate_df: the covariate data frame
    :param all_data_holdout: the last holdout of the data
    :param scale: (int) how much to scale up the draws by -- will take (n_draws * scale) number of draws
    :param lm_dict: linear model dictionary
    :param st_dict: spacetime model dictionary
    :return df: data frame
    """
    logger.info("Getting all beta draws.")
    submodels = covariate_summary_df.copy().sort_values(['submodel_version_id'])
    beta_df, keys = submodel_betas_df

    submodels['model'] = keys + keys
    submodels.rename(columns={'Type': 'type'}, inplace=True)

    master_json = {'mixed': lm_dict, 'spacetime': st_dict}
    submodels = submodels.loc[submodels.n_draws > 0]

    covlist = get_covlist(lm_dict, all_data_holdout=all_data_holdout)
    ratio_dict = sb.make_ratio_dict(df=data_frame, covariate_df=covariate_df, covariates=covlist)

    df = pd.concat(
        [
            get_submodel_covariate_draws(
                json=master_json,
                mtype=row['type'],
                key=row['model'],
                response=row['Dependent_Variable'],
                n_draws=row['n_draws'],
                rat_dict=ratio_dict,
                scale=scale,
                all_data_holdout=all_data_holdout
            ) for index, row in submodels.iterrows()
        ]
    )

    return df


def create_covariate_table(covariate_summary_df,
                           priors, submodel_weighted_betas_df):
    """
    Create table with one row per covariate with covariate metadata and also the standardized betas/weighted betas.

    At the end it also creates "relative" betas. These are to show the relative influence of the betas in the
    final ensemble because effectively, in submodels where this covariate doesn't exist, it should have a value of
    0 with weight sum(n_draws) for those submodels. Note: This is not taken into account with the weighted average
    because it is only a weighted average across submodels that have that covariate.

    :return info: data frame
    """
    logger.info("Creating covariate table w/ standardized betas.")
    df = covariate_summary_df[['covariates', 'n_draws', 'submodel_version_id']]
    num_models = len(df.submodel_version_id)
    df.loc[df.n_draws.isnull(), 'n_draws'] = 0.0

    covariates = df['covariates'].str.split(', ', expand=True)
    new_cols = ["covariate_{}".format(x) for x in range(1, len(covariates.columns) + 1)]
    df[new_cols] = covariates

    ids = ['n_draws', 'submodel_version_id']
    df = pd.melt(df[ids + new_cols], id_vars=ids)
    df = df.loc[df.value.notnull()]
    df.rename(columns={'value': 'name'}, inplace=True)
    df.drop('variable', inplace=True, axis=1)

    draws = pd.DataFrame(df.groupby('name')['n_draws'].sum()).reset_index()
    submodels = pd.DataFrame(df.groupby('name')['submodel_version_id'].count()).reset_index()
    submodels.rename(columns={'submodel_version_id': 'n_submodels'}, inplace=True)

    info = priors
    info = info.merge(draws, how='right', on='name').merge(submodels, how='right',
                                                           on='name')
    info = info[['name', 'lag', 'transform_type_short', 'level', 'direction', 'offset', 'n_draws', 'n_submodels']]
    info = info.sort_values(['n_draws', 'n_submodels'], ascending=False).reset_index(drop=True)

    beta_df = submodel_weighted_betas_df.copy()

    info = info.merge(beta_df, on=['name'])
    info["percent_included_ensemble_draws"] = info.n_draws / 1000
    info["percent_included_ensemble_submodel"] = info.n_submodels / num_models

    info["beta_relative"] = info.beta * info.percent_included_ensemble_draws
    info["standard_beta_relative"] = info.standard_beta * info.percent_included_ensemble_draws

    return info

