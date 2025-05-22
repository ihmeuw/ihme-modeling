"""Post-analysis of the GPR stage of ST-GPR model.


After GPR runs, aggregate locations up to global, calculate summaries and fit stats.
"""

import math
import os
import sys

import numpy as np
import pandas as pd

import stgpr_helpers
import stgpr_schema
from stgpr_helpers import columns, parameters

from stgpr.legacy.st_gpr import helpers as hlp
from stgpr.lib import constants, expansion, location_aggregation

######## Location aggregation and save GPR outputs ######################################


def run_post_gpr(run_id: int, holdout_num: int, param_set: int) -> None:
    """Post - GPR: read GPR results, location aggregate, and save summaries.

    If run has draws, read draws. Otherwise, read summaries. In either case,
    we only end up saving summaries as the aggregate draws are discarded.
    """

    file_utility = stgpr_helpers.StgprFileUtility(run_id)
    params = file_utility.read_parameters()

    data = (
        file_utility.read_draws(holdout_num, param_set)
        if params[parameters.GPR_DRAWS] > 0
        else file_utility.read_gpr_estimates_temp(holdout_num, param_set)
    )

    # Only run location aggregation if metric id is non-null
    if params[parameters.METRIC_ID]:
        data = location_aggregation.aggregate_locations(
            run_id, data, data_in_model_space=False
        )

    # Create summaries if we have draws. Otherwise, set upper and lower as equal to mean
    if params[parameters.GPR_DRAWS] > 0:
        draw_cols = [f"draw_{i}" for i in range(params[parameters.GPR_DRAWS])]
        data["gpr_mean"] = data[draw_cols].mean(axis=1)
        data["gpr_lower"] = data[draw_cols].quantile(
            q=constants.uncertainty.LOWER_QUANTILE, axis=1
        )
        data["gpr_upper"] = data[draw_cols].quantile(
            q=constants.uncertainty.UPPER_QUANTILE, axis=1
        )
    else:
        data["gpr_lower"] = data["gpr_mean"]
        data["gpr_upper"] = data["gpr_mean"]

    data = data[columns.DEMOGRAPHICS + ["gpr_mean", "gpr_lower", "gpr_upper"]]
    data = expansion.expand_results(data=data, params=params)

    hlp.model_save(
        data[columns.DEMOGRAPHICS + ["gpr_mean", "gpr_lower", "gpr_upper"]],
        run_id,
        "gpr",
        holdout=holdout_num,
        param_set=param_set,
    )


################ Calculate RMSE and post-modeling stats ##########################


def get_data(run_id, holdout_num=0, param_set=0):
    """Returns some useful data columns, stage1, st, and gpr.
    The modeling columns are in NORMAL space - ie backtransformed out of
    any log/logit space if the model ran in that space. Data and variance
    are provided in both transformed and non-transformed space."""

    # pull and subset prepped data to necessities
    data = hlp.model_load(run_id, "prepped")
    data_transform = hlp.model_load(run_id, "parameters", param_set=None)[
        "data_transform"
    ].iat[0]

    ko_col = ["ko_{}".format(holdout_num)]
    data_cols = ["data", "variance", "original_data", "original_variance"]
    data = data[columns.DEMOGRAPHICS + data_cols + ko_col]

    # pull linear estimates outputs
    stage1 = hlp.model_load(run_id, "stage1", holdout=holdout_num)
    if "location_id_count" in stage1.columns:
        stage1.drop(columns="location_id_count", inplace=True)

    # pull st and gpr outputs
    st = hlp.model_load(run_id, "st", holdout=holdout_num, param_set=param_set)
    st.drop(columns="scale", inplace=True)
    gpr = hlp.model_load(run_id, "gpr", holdout=holdout_num, param_set=param_set)

    # merge
    df = stage1.merge(data, on=columns.DEMOGRAPHICS, how="outer")
    df = df.merge(st, on=columns.DEMOGRAPHICS, how="left")
    df = df.merge(gpr, on=columns.DEMOGRAPHICS, how="left")

    # transform modeling columns
    df["stage1"] = stgpr_helpers.transform_data(df["stage1"], data_transform, reverse=True)
    df["st"] = stgpr_helpers.transform_data(df["st"], data_transform, reverse=True)

    return df


def rmse(error):
    return math.sqrt(np.mean([(x**2.0) for x in error]))


def calculate_rmse(df, holdout_num, var="gpr_mean", inv_variance_weight=False):

    tmp = df.copy()
    errs = pd.DataFrame(
        {"ko": holdout_num, "in_sample_rmse": np.nan, "oos_rmse": np.nan}, index=[0]
    )

    # calculate error (in-sample and out-of-sample) for each holdout requested
    data_is = tmp.loc[tmp["ko_{}".format(holdout_num)] == 1, "original_data"]
    data_oos = tmp.loc[tmp["ko_{}".format(holdout_num)] != 1, "original_data"]

    outvar_is = tmp.loc[tmp["ko_{}".format(holdout_num)] == 1, var]
    outvar_oos = tmp.loc[tmp["ko_{}".format(holdout_num)] != 1, var]

    if inv_variance_weight:
        variances_is = tmp.loc[tmp["ko_{}".format(holdout_num)] == 1, "original_variance"]
        variances_oos = tmp.loc[tmp["ko_{}".format(holdout_num)] != 1, "original_variance"]
        wt_is = variances_is / np.sum(variances_is)
        wt_oos = variances_oos / np.sum(variances_oos)
    else:
        wt_is = wt_oos = 1

    tmp["is_error"] = wt_is * (data_is - outvar_is)
    tmp["oos_error"] = wt_oos * (data_oos - outvar_oos)

    # calculate in-sample and out-of-sample RMSE (will automatically
    # recognize oos rmse is unavailable for runs with no kos)
    errs["in_sample_rmse"] = rmse(tmp.loc[tmp.is_error.notnull(), "is_error"].tolist())
    errs["oos_rmse"] = rmse(tmp.loc[tmp.oos_error.notnull(), "oos_error"].tolist())
    errs["var"] = var

    errs = errs[["var", "ko", "in_sample_rmse", "oos_rmse"]]

    return errs


def calculate_fit_stats(
    run_id, run_type, holdout_num, holdouts, param_groups, csv=True, inv_variance_weight=False
):

    rmse_list = []

    # pull data
    for param in param_groups:

        # pull data
        df = get_data(run_id, holdout_num, param)

        for var in ["stage1", "st", "gpr_mean"]:

            # calculate in-sample and (where relevant) out-of-sample RMSE
            rmse_table = calculate_rmse(df, holdout_num, var, inv_variance_weight)
            rmse_table["parameter_set"] = param

            # selection runs have multiple hyperparameter sets
            # out-of-sample evaluation runs use just need oos-rmse for one set of parameters
            if run_type in ["in_sample_selection", "oos_selection"]:
                # Loading params with a non-None param_set returns a dict
                hyperparams = hlp.model_load(
                    run_id, "parameters", holdout=holdout_num, param_set=param
                )
                rmse_table["zeta"] = hyperparams["zeta"]
                rmse_table["lambdaa"] = hyperparams["lambda"]
                rmse_table["omega"] = hyperparams["omega"]
                rmse_table["scale"] = hyperparams["scale"]

            else:
                hyperparams = hlp.model_load(run_id, "parameters", param_set=None)
                rmse_table["zeta"] = hyperparams[parameters.ST_ZETA].iat[0]
                rmse_table["lambdaa"] = hyperparams[parameters.ST_LAMBDA].iat[0]
                rmse_table["omega"] = hyperparams[parameters.ST_OMEGA].iat[0]
                rmse_table["scale"] = hyperparams[parameters.GPR_SCALE].iat[0]
                rmse_table["density_cutoffs"] = hyperparams.density_cutoffs.iat[0]

            rmse_list.append(rmse_table)

    rmses = pd.concat(rmse_list)
    rmses = rmses[
        [
            "var",
            "ko",
            "parameter_set",
            "zeta",
            "lambdaa",
            "omega",
            "scale",
            "in_sample_rmse",
            "oos_rmse",
        ]
    ]

    # set in order of 'best' ie lowest oos rmse if ko run
    if run_type == "oos_selection":
        rmses.sort_values(by=["oos_rmse"], ascending=True)

    settings = stgpr_schema.get_settings()
    output_root = settings.output_root_format.format(stgpr_version_id=run_id)
    if (run_type in ["in_sample_selection", "oos_selection"]) | (holdouts > 0):
        outpath = "{}/fit_stats_{}_{}.csv".format(output_root, holdout_num, param)
    else:
        outpath = "{}/fit_stats.csv".format(output_root)

    if csv:
        rmses.to_csv(outpath, index=False)
        print("Saved fit statistics for holdout {} to {}".format(holdout_num, outpath))

    return rmses


if __name__ == "__main__":
    run_id = int(sys.argv[1])
    holdout_num = int(sys.argv[2])
    run_type = sys.argv[3]
    holdouts = int(sys.argv[4])
    param_sets = sys.argv[5]

    for i in ["run_id", "holdout_num", "run_type", "holdouts", "param_sets"]:
        print("{} : {}".format(i, eval(i)))

    # split out param sets to list
    param_groups = hlp.separate_string_to_list(str(param_sets), typ=int)

    print("Run location aggregation and save summaries.")
    for param_set in param_groups:
        run_post_gpr(run_id, holdout_num, param_set)

    # calculate fit stats
    print("Calculating fit stats")
    calculate_fit_stats(run_id, run_type, holdout_num, holdouts, param_groups)

    print("Post-modeling calculations complete.")
