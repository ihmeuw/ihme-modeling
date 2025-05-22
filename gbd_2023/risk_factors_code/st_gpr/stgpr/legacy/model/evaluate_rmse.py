import os
import sys

import pandas as pd

import db_stgpr
import db_tools_core
import stgpr_schema

from stgpr.legacy.st_gpr.helpers import antijoin, equal_sets

if __name__ == "__main__":
    run_id = int(sys.argv[1])
    run_type = sys.argv[2]
    holdouts = int(sys.argv[3])
    n_params = int(sys.argv[4])

    for i in ["run_id", "run_type", "holdouts", "n_params"]:
        print("{} : {}".format(i, eval(i)))

    settings = stgpr_schema.get_settings()
    with db_tools_core.session_scope(settings.stgpr_db_conn_def) as scoped_session:
        db_stgpr.update_model_status(
            run_id, stgpr_schema.ModelStatus.evaluate, scoped_session
        )

    # pull all fit_stat files and make sure no parameter sets are missing stats
    output_root = settings.output_root_format.format(stgpr_version_id=run_id)
    rmse_files = [x for x in os.listdir(output_root) if "fit_stats" in x]
    rmse = pd.concat([pd.read_csv("{}/{}".format(output_root, f)) for f in rmse_files])

    # make sure that fit statistics are present for all parameter sets
    rmse_param_sets = rmse.parameter_set.unique()
    mi = antijoin(rmse_param_sets, list(range(0, n_params)))
    msg = "You are missing fit statistics for the following parameter sets: {}".format(mi)
    assert equal_sets(rmse_param_sets, list(range(0, n_params))), msg

    # take means - since each holdout contributes equivalent weight, should be
    df = (
        rmse.groupby(["parameter_set", "var"])["in_sample_rmse", "oos_rmse"]
        .mean()
        .reset_index()
    )

    if run_type == "in_sample_selection":
        min_gp_rmse = df.loc[df["var"] == "gpr_mean"]["in_sample_rmse"].min()
        df["best"] = (df["in_sample_rmse"] == min_gp_rmse).astype(int)
    elif run_type == "oos_selection":
        min_rmse = df.loc[df["var"] == "gpr_mean"]["oos_rmse"].min()
        df["best"] = (df["oos_rmse"] == min_rmse).astype(int)
    else:
        df["best"] = 1

    # also save RMSE values
    # df = df.reset_index()
    df = df.rename(columns={"in_sample_rmse": "is_mean", "oos_rmse": "oos_mean"})
    out = rmse.merge(df, on=["parameter_set", "var"])
    out.to_csv("{}/fit_stats.csv".format(output_root), index=False)
