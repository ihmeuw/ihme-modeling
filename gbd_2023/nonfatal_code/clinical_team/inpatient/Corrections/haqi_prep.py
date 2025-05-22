"""
Set and get the HAQi correction factor for the inpatient pipeline. This module is used
at the beginning and end of the pipeline. At the beginning of a run the HAQi values are
cached (set) to disk within the run_id and then they're pulled, merged and applied at
the end in 5 year space

I'm also adding a few simple tools to compare haqi values between runs to
this module
NOTE: This will not work prior to run 16. We didn't cache haqi values at that time.
"""

import warnings
from typing import List

import pandas as pd
from clinical_functions import demographic, legacy_pipeline


def set_haqi_cf(
    run_id: int, bin_years: bool, min_treat: float = 0.1, max_treat: float = 0.75
) -> None:
    """
    A function to set the health access quality covariates data which we'll
    use to divide our mean_raw values by to adjust our estimates up. This func
    will now write a file to drive, towards the beginning of the pipeline that
    will be pulled in at the end.

    Parameters:
        min_treat: float
            minimum access. Sets a floor for the CF. If 0.1 then the lowest possible CF
            will be 0.1, in practice this is a 10x increase in the estimate
        max_treat: float or int
            maximum acess. Sets a cap for the CF. If 75 then any loc/year with a covariate
            above 75 will have a CF of 1 and the data will be unchanged
        bin_years: bool aggergates means to five year bins

    """

    base = f"/ihme/hospital/clinical_runs/run_{run_id}/estimates/corrections/haqi_corrections"

    # get a dataframe of haqi covariate estimates
    df = legacy_pipeline.get_covar_for_clinical(
        run_id=run_id, covariate_name_short="haqi", iw_profile="clinical"
    )

    if not all(df["age_group_id"] == 22):
        raise RuntimeError("not all age_group_ids are 22")
    if not all(df["sex_id"] == 3):
        raise RuntimeError("not all sex_ids are 3")

    df.to_csv(f"{base}/haqi_inputs.csv", index=False)
    df = df.rename(columns={"year_id": "year_start"})

    if df["mean_value"].mean() > 1 and max_treat < 1:
        warn_msg = """Increasing max_treat variable 100X. Mean of the HAQi column is larger
        than 1. We assume this means the range is from 0 to 100. Summary stats for the
        mean_value column in the haqi covar are \n {}""".format(
            df["mean_value"].describe()
        )
        warnings.warn(warn_msg)
        max_treat = max_treat * 100

    # set the max value
    df.loc[df["mean_value"] > max_treat, "mean_value"] = max_treat

    # get min df present in the data
    # Note, should this just be 0.1?
    min_df = df["mean_value"].min()

    # make the correction
    df["haqi_cf"] = min_treat + (1 - min_treat) * (
        (df["mean_value"] - min_df) / (max_treat - min_df)
    )

    # drop the years outside of hosp_data so year binner doesn't break
    df["year_end"] = df["year_start"]
    warnings.warn("Currently dropping HAQi values before 1988 and after 2023")
    df = df[(df["year_start"] > 1987)].copy()
    if bin_years:
        df = demographic.year_binner(df)
        # Take the average of each 5 year band
        df = (
            df.groupby(["location_id", "year_start", "year_end", "model_version_id"])
            .agg({"haqi_cf": "mean"})
            .reset_index()
        )

    if df["haqi_cf"].max() > 1:
        raise RuntimeError("The largest haqi CF is too big")
    if df["haqi_cf"].min() < min_treat:
        raise RuntimeError("The smallest haqi CF is too small")

    df = df[["location_id", "year_start", "year_end", "model_version_id", "haqi_cf"]]
    writepath = f"{base}/haqi_cfs.csv"
    df.to_csv(writepath, index=False)

    return


def get_haqi_cf(run_id: int) -> pd.DataFrame:
    """Read in given run's haqi dataframe

    Args:
        run_id: clinical run_id

    Returns:
        Given run's haqi df
    """
    if run_id < 16:
        raise RuntimeError("Cached HAQi CFs don't exist prior to run 16")

    rpath = (
        f"/ihme/hospital/clinical_runs/run_{run_id}/estimates/"
        "corrections/haqi_corrections/haqi_cfs.csv"
    )
    return pd.read_csv(rpath)


def merge_haqi(df: pd.DataFrame, run_id: int) -> pd.DataFrame:
    """Merge the HAQi adjustment onto a dataset"""
    haqi = get_haqi_cf(run_id)
    mvid = haqi["model_version_id"].unique()
    print((f"HAQi model_version_id {mvid} will be used"))
    haqi = haqi.drop("model_version_id", axis=1)

    pre = df.shape
    # merge on the id_cols for the haqi covar
    df = df.merge(
        haqi,
        how="left",
        on=["location_id", "year_start", "year_end"],
    )
    if pre[0] != df.shape[0]:
        raise RuntimeError(
            "DF row data is different. Pre shape {}. Post shape {}".format(pre, df.shape)
        )
    if df["haqi_cf"].isnull().sum() != 0:
        raise RuntimeError(
            "There are rows with a null haqi value. \n {}".format(df[df["haqi_cf"].isnull()])
        )

    return df


def apply_haqi(
    df: pd.DataFrame, apply_cols: List[str] = ["mean", "upper", "lower"]
) -> pd.DataFrame:
    """apply the haqi correction to a set of apply_columns
    Assumes that the adjustment should be applied across all estimate_ids
    and bundles
    """
    cols = df.columns.tolist()

    if "haqi_cf" not in cols:
        raise KeyError("The haqi Correction must be present")
    for col in apply_cols:
        if col in df.columns:
            df[col] = df[col] / df["haqi_cf"]
        else:
            raise KeyError(f"{col} does not exist in df!")

    return df


def review_haqi(new: int, old: int, subset: pd.DataFrame = None) -> pd.DataFrame:
    """Compare haqi CFs between two runs. Merge them together and calculate pct change

    new and old are run_ids > 15
    subset is a df with location, year start and year end to use to
    filter rows (inner merge)"""

    ndf = get_haqi_cf(new)
    odf = get_haqi_cf(old)

    id_cols = ["location_id", "year_start", "year_end"]
    m = ndf.merge(
        odf,
        how="outer",
        on=id_cols,
        validate="1:1",
        suffixes=(f"_run_{new}", f"_run_{old}"),
    )
    m["pct_chg"] = (
        (m[f"haqi_cf_run_{new}"] - m[f"haqi_cf_run_{old}"]) / m[f"haqi_cf_run_{old}"]
    ) * 100

    if subset:
        if len(subset) != len(subset[id_cols].drop_duplicates()):
            raise RuntimeError("There are duplicated rows in the subset df")
        m = subset.merge(m, how="left", on=id_cols, validate="1:1")
    else:
        pass

    return m
