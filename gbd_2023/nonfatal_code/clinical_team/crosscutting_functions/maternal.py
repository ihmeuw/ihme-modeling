from typing import List

import pandas as pd
from crosscutting_functions.clinical_metadata_utils import database
from db_queries import get_covariate_estimates

from crosscutting_functions.bundle_to_cause_hierarchy import BundleCause
from crosscutting_functions.pipeline import get_release_id


def get_maternal_bundles(
    map_version: int, run_id: int, override_bundles: List[int] = [10228]
) -> List[int]:
    """Pulls all the bundle_ids associated with maternal causes, based on
    the maternal parent cause_id 366 in the map_version

    Args:
        map_version: Clinical bundle map version.
        run_id: Clinical run_id found in 'run_metadata'.
        override_bundles: Spcific bundles to include as maternal bundles. The normally
            would not be included.
            The default value per map_version=33 is for one neonatal bundle [10228].

    Raises:
        ValueError: Function does not support runs without release_id in run_metadata.
        RuntimeError: Call returned no bundles.

    Returns:
        List[int]: List of maternal only bundles.
    """

    if run_id <= 36:
        raise ValueError("run_id must have a realease_id associated with it.")

    maternal_bundles = BundleCause(
        cause_id=366,
        map_version=map_version,
        release_id=get_release_id(run_id=run_id),
        cause_set_id=9,
    ).bundles_by_cause()

    if len(maternal_bundles) < 1:
        raise RuntimeError("No maternal bundles in list")

    maternal_bundles.extend(override_bundles)

    return maternal_bundles


def get_asfr_model_version_id(run_id: int) -> int:
    """Grab asfr_model_version_id from run_metadata.

    Args:
        run_id: Clinical run found in run_metadata.

    Raises:
        RuntimeError: If a run_id was issued more than 1 asfr_model_version_id.

    Returns:
        asfr_model_version_id value.
    """

    db = database.Database()
    db.load_odbc(odbc_profile="CONN_DEF")

    qu = "QUERY" 
    run_metadata = db.query(qu)

    if run_metadata["asfr_model_version_id"].nunique() > 1:
        msg = f"run={run_id} has more than 1 asfr version associated"
        raise RuntimeError(msg)

    return run_metadata["asfr_model_version_id"].unique()[0]


def get_asfr(mat_df: pd.DataFrame, run_id: int) -> pd.DataFrame:
    """The Stata script which applied the maternal adjustment is no longer being used
    We must now do this simple adjustment in Python. It's merely the sample size * asfr
    then cases / sample size

    Args:
        mat_df: Maternal only dataframe.
        run_id: Clinical run found in run_metadata.

    Returns:
        asfr covariate data.
    """
    locs = mat_df["location_id"].unique().tolist()
    age_groups = mat_df["age_group_id"].unique().tolist()
    years = mat_df["year_start"].unique().tolist()
    asfr_model_version_id = get_asfr_model_version_id(run_id=run_id)
    asfr = get_covariate_estimates(
        covariate_id=13,
        release_id=get_release_id(run_id=run_id),
        sex_id=2,
        age_group_id=age_groups,
        location_id=locs,
        year_id=years,
        model_version_id=asfr_model_version_id,
    )

    asfr = asfr[["location_id", "year_id", "age_group_id", "sex_id", "mean_value"]]
    asfr.rename(columns={"year_id": "year_start"}, inplace=True)

    return asfr


def merge_asfr(df: pd.DataFrame, asfr: pd.DataFrame) -> pd.DataFrame:
    """Merge the asfr covariate df onto the hospital data

    Args:
        df: Claims bundle estimate dataframe.
        asfr: asfr covariate data.

    Raises:
        RuntimeError: Row count changed
        RuntimeError: More than 1 column added during merge.

    Returns:
        Claims estimate df with asfr covariates data.
    """
    merge_cols = asfr.drop("mean_value", axis=1).columns.tolist()

    pre = df.shape
    df = df.merge(asfr, how="left", on=merge_cols, validate="m:1")
    if pre[0] != df.shape[0]:
        raise RuntimeError("Row count changed")
    if pre[1] + 1 != df.shape[1]:
        raise RuntimeError("Expected only one column to be added.")

    return df


def apply_asfr(df: pd.DataFrame, maternal_bundles: List[int], run_id: int) -> pd.DataFrame:
    """Takes all clinical claims data being processed, splits out maternal data then
    pulls in the get and merge functions above onto just maternal
    data and adjusts the sample size by multiply by asfr and then
    re-calcuating the mean from pre calculated cases (IMPORTANT) / new sample size
    basically you want the pre-adjusted cases to be adjusted by a smaller sample size

    Args:
        df: Table with bundle estimates and sample size column.
        maternal_bundles: Maternal bundles for a given map.
        run_id: Clinical run found in run_metadata.

    Raises:
        RuntimeError: Gained rows during processing.
        RuntimeError: Change in column count after processing.

    Returns:
        Claims bundle estiamtes with maternal estimates adjusted.
    """
    pre = df.shape

    mat_df = df.loc[df.bundle_id.isin(maternal_bundles), :]
    df = df.loc[~df["bundle_id"].isin(maternal_bundles), :]

    asfr = get_asfr(mat_df=mat_df, run_id=run_id)
    mat_df = merge_asfr(df=mat_df, asfr=asfr)

    # create new cases using existing mean and sample
    mat_df["cases"] = mat_df["mean"] * mat_df["sample_size"]

    mat_df = mat_df.loc[mat_df.mean_value != 0, :]

    # reduce to live births
    mat_df["sample_size"] = mat_df["sample_size"] * mat_df["mean_value"]

    # create new mean using previous cases and new sample size
    mat_df["mean"] = mat_df["cases"] / mat_df["sample_size"]

    mat_df.drop(["mean_value", "cases"], axis=1, inplace=True)

    df = pd.concat([df, mat_df], sort=False, ignore_index=True)

    if pre[0] < df.shape[0]:
        raise RuntimeError("Gained rows somehow.")
    if pre[1] != df.shape[1]:
        raise RuntimeError(f"Number of columns changed by {pre[1] - df.shape[1]}")

    return df
