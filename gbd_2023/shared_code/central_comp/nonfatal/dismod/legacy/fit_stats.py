import logging
import os

import numpy as np
import pandas as pd

from cascade_ode.legacy import io
from cascade_ode.legacy.settings import load as load_settings

# Set default file mask to readable-for all users
os.umask(0o0002)

# Path to this file
this_path = os.path.dirname(os.path.abspath(__file__))

# Get configuration options
settings = load_settings()


def calc_rmses(df: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["integrand", "cv_id", "hold_out"]
    rmses = pd.DataFrame(columns=key_cols + ["rmse"])

    # Drop 0s from adjusted data... these can't be reasonably handled
    # when computing RMSE
    thisdf = df[((df.adjust_median > 0) & (df.pred_median > 0))]
    if not thisdf.empty:
        rmses = thisdf.groupby(key_cols).apply(
            lambda x: np.sqrt(
                np.mean((np.log(x["adjust_median"]) - np.log(x["pred_median"])) ** 2)
            )
        )
        rmses = rmses.reset_index()
        rmses.rename(columns={0: "rmse"}, inplace=True)

    return rmses


def calc_mean_errors(df: pd.DataFrame) -> pd.DataFrame:
    key_cols = ["integrand", "cv_id", "hold_out"]
    means = pd.DataFrame(columns=key_cols + ["mean_error"])

    # Drop 0s from adjusted data... these can't be reasonably handled
    # when computing RMSE
    thisdf = df[((df.adjust_median > 0) & (df.pred_median > 0))]
    if not thisdf.empty:
        means = thisdf.groupby(key_cols).apply(
            lambda x: np.mean((np.log(x["adjust_median"]) - np.log(x["pred_median"])) ** 2)
        )
        means = means.reset_index()
        means.rename(columns={0: "mean_error"}, inplace=True)

    return means


def calc_coverage(df):
    unadj_bool = df.adjust_lower == df.adjust_upper
    df.loc[unadj_bool, "adjust_lower"] = (
        df.loc[unadj_bool, "meas_value"] - 1.96 * df.loc[unadj_bool, "meas_stdev"]
    )
    df.loc[unadj_bool, "adjust_upper"] = (
        df.loc[unadj_bool, "meas_value"] + 1.96 * df.loc[unadj_bool, "meas_stdev"]
    )
    adj_var = ((df.adjust_upper - df.adjust_lower) / (2 * 1.96)) ** 2
    pred_var = ((df.pred_upper - df.pred_lower) / (2 * 1.96)) ** 2
    cov_lower = df.pred_median - 1.96 * np.sqrt(adj_var + pred_var)
    cov_upper = df.pred_median + 1.96 * np.sqrt(adj_var + pred_var)
    df["covered"] = (df.adjust_median > cov_lower) & (df.adjust_median < cov_upper)
    covered_counts = df.groupby(["integrand", "covered", "cv_id", "hold_out"])[
        "pred_median"
    ].count()
    covered_counts = covered_counts.reset_index()
    covered_counts["pct"] = covered_counts.groupby(["integrand", "cv_id", "hold_out"])[
        "pred_median"
    ].apply(lambda x: x / x.sum())
    covered_counts.rename(columns={"pred_median": "adj_data_count"}, inplace=True)
    return covered_counts


def write_fit_stats(mvid, outdir, joutdir) -> pd.DataFrame:
    """Writes file to 

    Returns:
        fit stats dataframe with information on coverage by
        integrand, error stats for upload to database
        'epi.model_version_fit_stat'. Documented here
    """
    log = logging.getLogger(__name__)

    root_dir = settings["cascade_ode_out_dir"]
    submodel_dir = 

    df = io.file_storage.read_consolidated_data_pred_files(submodel_dir)
    rmses = calc_rmses(df)
    ms = calc_mean_errors(df)

    # If we can't compute rmses and mean errors because of no non-zero
    # rows, exit early
    if ms.empty:
        return

    fs_df = rmses.merge(ms, on=["integrand", "cv_id", "hold_out"])
    cov_df = calc_coverage(df)

    try:
        os.makedirs(joutdir)
    except:
        pass
    try:
        os.chmod(joutdir, 0o775)
    except:
        pass
    fs_df.to_csv(, index=False)
    cov_df.to_csv(, index=False)

    measure_map = 
    fs_df = fs_df.merge(measure_map, left_on="integrand", right_on="measure", how="left")
    cov_df = cov_df.merge(measure_map, left_on="integrand", right_on="measure", how="left")

    if len(fs_df.cv_id.unique()) > 1:
        fs_df = fs_df[fs_df.cv_id != "full"]
    fs_df = fs_df.groupby(["measure_id", "hold_out"])["rmse"].mean().reset_index()
    fs_df.loc[fs_df.hold_out == 0, "fit_stat_id"] = 1
    fs_df.loc[fs_df.hold_out == 1, "fit_stat_id"] = 2
    fs_df.rename(columns={"rmse": "fit_stat_value"}, inplace=True)

    cov_df = cov_df[cov_df.covered]
    if cov_df.empty:
        log.warning(
            "cov_df empty, so no integrands meet coverage conditions. "
            "Note will not contain coverage information."
        )
    else:
        if len(cov_df.cv_id.unique()) > 1:
            cov_df = cov_df[cov_df.cv_id != "full"]
        cov_df = cov_df.groupby(["measure_id", "hold_out"])["pct"].mean().reset_index()
        cov_df.loc[cov_df.hold_out == 0, "fit_stat_id"] = 3
        cov_df.loc[cov_df.hold_out == 1, "fit_stat_id"] = 4
        cov_df.rename(columns={"pct": "fit_stat_value"}, inplace=True)

    mv_fs = fs_df.append(cov_df)
    mv_fs["fit_stat_value"] = mv_fs.fit_stat_value.replace({np.inf: -9999})
    mv_fs[["measure_id", "fit_stat_id", "fit_stat_value"]].to_csv(
    )
    return mv_fs
