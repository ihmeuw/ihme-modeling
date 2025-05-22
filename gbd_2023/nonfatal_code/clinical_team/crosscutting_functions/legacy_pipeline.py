import re
from typing import List, Optional, Union

import numpy as np
import pandas as pd
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import InpatientWrappers
from crosscutting_functions.clinical_metadata_utils.database import Database
from db_queries import get_covariate_estimates, get_population

from crosscutting_functions import bundle_to_cause_hierarchy as bch
from crosscutting_functions import demographic, pipeline
from crosscutting_functions.get-cached-population.cached_pop_tools import get_cached_pop
from crosscutting_functions.general_purpose import expandgrid
from crosscutting_functions.pipeline import get_release_id

INPATIENT_FACILITIES = ["inpatient unknown", "hospital"]


def get_covar_for_clinical(
    run_id: int, covariate_name_short: str, iw_profile: str, **kwargs
) -> pd.DataFrame:
    """Uses a clinical run_id to pull run_metadata and identify
    which gbd release_id to pull covariates from. Checks the
    results of this against the expected model version number. note it
    DOES NOT use model version id to pull results. That was producing
    some odd results.

    Args:
        run_id: Identifies which run to get the init pickle from.
        covariate_name_short: Matches the values of the get_covariate function. Used to
            identify which covariate to pull.
        **kwargs: Demographic info varies across covariates so pass over locations,
            years or whatever is needed.

    Raises:
        ValueError if multiple model versions are returned.

    Returns:
        A DataFrame of covariate estimates.
    """
    covariate_name_short = covariate_name_short.lower()
    central_lookup = {
        "haqi": {"covariate_id": 1099},
        "live_births_by_sex": {"covariate_id": 1106},
        "asfr": {"covariate_id": 13},
        "ifd_coverage_prop": {"covariate_id": 51},
    }

    # Extract cov lookup using short name.
    cov_lookup = central_lookup[covariate_name_short]
    run_metadata_col = f"{covariate_name_short}_model_version_id"

    iw = InpatientWrappers(run_id, iw_profile)
    # Pull data.
    df = get_covariate_estimates(
        covariate_id=cov_lookup["covariate_id"],
        release_id=pipeline.get_release_id(run_id),
        model_version_id=iw.pull_run_metadata()[run_metadata_col][0],
        **kwargs,
    )

    # Validate version ID.
    if df["model_version_id"].unique().size != 1:
        raise ValueError(
            f"We expect exactly 1 model version. {df['model_version_id'].unique()}"
        )
    obs = df["model_version_id"].iloc[0]
    exp = iw.pull_run_metadata()[run_metadata_col][0]
    if obs != exp:
        raise ValueError(
            (
                f"Unexpected model version ID was returned from "
                f"get_covar_estimates. Got {obs}, but expected {exp}"
            )
        )
    return df


def create_cause_fraction(
    df: pd.DataFrame, run_id: int, tol: float = 1e-7, write_cause_fraction: bool = False
) -> pd.DataFrame:
    """Takes a dataframe of hospital data which has been formatted, mapped
    from cause_code to icg_id, aggregated by demographic groups and creates
    a new cause_fraction column.
    A cause fraction (or admission fraction more accurately) is the proportion
    of admissions in a certain demographic that had a particular
    icg_id, out of all the admissions for any icg_id in that same demographic.
    Also, the function sums all cause fractions within a demographic group to
    make sure that they are close to 1.

    Args:
        df: Contains the data to be made into cause fractions. Should be at
            icg_id level.
        write_cause_fraction: Whether or not to write out this data to disk.
        run_id: The run_id to write cause fractions to. Only used when writing.
        tol: Positive floating-point number that determines how close the summed
             cause fractions must be to 1.

    Raises:
        ValueError if duplicates are found along a set of ID columns.
        ValueError if icg_id is not in the data.
        ValueError if tolerance is not greater than 0.
        ValueError if there are cause fractions greater than 1.
    """

    if "icg_id" not in df.columns:
        raise ValueError(
            "'icg_id' isn't a column of df; Data must be at the baby sequelae level"
        )

    if not tol > 0:
        raise ValueError("'tol' must be positive")

    if "year_start" in df.columns:
        year_cols = ["year_start", "year_end"]
    elif "year_id" in df.columns:
        year_cols = ["year_id"]

    # For maternal data that was age-split, sometimes there are zeros in higher
    # ages. They should be zero anyways cuz of age sex restrictions
    # but 0 / 0 = NaN, and np.allclose throws an error when there are
    # nulls.
    df = df[df.val >= 0]

    # Check if there are duplicated identifier columns.
    df_shape = df.shape[0]
    id_cols = [
        "age_group_id",
        "diagnosis_id",
        "facility_id",
        "location_id",
        "nid",
        "icg_id",
        "representative_id",
        "sex_id",
        "source",
        "age_group_unit",
        "metric_id",
        "outcome_id",
    ] + year_cols
    id_shape = df[~df[id_cols].duplicated(keep="first")].shape[0]

    if df_shape != id_shape:
        raise ValueError(
            f"There is duplicated data in the input df. We observed {df_shape} "
            f"rows and expected {id_shape} rows."
        )

    # Select groupby features for the denominator.
    denom_list = ["age_group_id", "sex_id", "location_id"] + year_cols

    # Select groupby features for the numerator.
    numer_list = [
        "age_group_id",
        "sex_id",
        "location_id",
        "source",
        "nid",
        "icg_id",
    ] + year_cols

    # Compute cause fractions.
    df["numerator"] = df.groupby(numer_list)["val"].transform("sum")
    df["denominator"] = df.groupby(denom_list)["val"].transform("sum")
    df["cause_fraction"] = df["numerator"] / df["denominator"]

    if write_cause_fraction:
        # Write the cause fraction to /share for review, viz.
        df.to_hdf(
            "FILEPATH/cause_fraction.H5".format(run_id),
            key="df",
            mode="w",
        )

    # Check proportions sum to 1.
    test_sum = (
        df[df["val"] > 0].groupby(denom_list).agg({"cause_fraction": "sum"}).reset_index()
    )

    # Validate that the cause_fractions sum to 1 within tolerance.
    np.testing.assert_allclose(
        actual=test_sum["cause_fraction"],
        desired=1,
        rtol=tol,
        atol=0,
        err_msg=("Proportions do not sum to 1 within tolerance of {}.".format(tol)),
        verbose=True,
    )

    # No cause fractions above 1.
    if not (df["cause_fraction"].max() <= 1):
        raise ValueError("There are fractions that are bigger than 1 which is not valid.")

    return df


def create_hosp_denom(df: pd.DataFrame, denom_col: str, run_id: int) -> None:
    """Write sample size (sum of all admissions) to the run.

    Args:
        df: A DataFrame of clinical inpatient data.
        denom_col: The column to use when summing counts after grouping by a set of
            hardcoded demographic columns.
        run_id: Standard clinical run id. Used to write the hospital denominators in the run.
    """
    samp = (
        df.groupby(["age_group_id", "sex_id", "location_id", "year_id"])
        .agg({denom_col: "sum"})
        .reset_index()
    )
    denom_path = (
        "FILEPATH/hospital_denominators.csv".format(run_id)
    )
    samp.to_csv(denom_path, index=False)


def apply_merged_nids(
    df: pd.DataFrame, assert_no_nulls: bool = True, fillna: bool = False
) -> pd.DataFrame:
    """Function that attaches merged NIDs. Merged NIDs are attached via NID.
    When creating five-year-bands, we need new NIDs to fit with these new bands.

    Args:
        df: Data with single year NIDs. Should be at five-year
            year_start and year_end, i.e., has had hosp_prep.year_binner()
            ran on it, but could probably work on year-by-year data too since
            years are not used in the merge.
        assert_no_nulls: If True, and it should always be True, will
            assert that there are no nulls in the NID column at the end.
            Only exists for development purposes
        fillna: If True, will fill nulls in NID column with -1. Should
            always be false but exists for development purposes.

    Returns:
        Dataframe df with the new NIDs.

    Raises:
        RuntimeError if the columns or number of now change.
        ValueError if our lookup table has missing NIDs.
        KeyError if this function changes column in the data.
        ValueError if assert_no_nulls is true and null NIDs are observed.
    """

    columns_before = df.columns

    q = """
    QUERY
    """
    db = Database()
    db.load_odbc("DATABASE")
    nids = db.query(q)

    if nids[["nid", "merged_nid"]].isnull().any().any():
        missing_nids = nids[(nids["nid"].isnull()) | (nids["merged_nid"].isnull())]
        raise ValueError(f"There are missing NIDs.\n\n{missing_nids}")

    # Rename nid so this is clear.
    nids = nids.rename(columns={"nid": "old_nid"})
    df = df.rename(columns={"nid": "old_nid"})

    # Some data sources only have one year of data, so don't
    # need a merged NID.
    single_years = []
    for source, a_df in df.groupby(["source"]):
        if a_df["year_start"].unique().size == 1:
            single_years.append(source)

    pre_shape = df.shape[0]
    df = df.merge(nids, how="left", on="old_nid")
    if df.shape[0] != pre_shape:
        raise RuntimeError("Extra rows were added.")

    # If a source has only 1 unique year_start/year_end value, then we can just
    # use the standard NID.
    for asource in single_years:
        if df.loc[df["source"] == asource, "merged_nid"].isnull().all():
            df.loc[df["source"] == asource, "merged_nid"] = df.loc[
                df["source"] == asource, "old_nid"
            ]

    # Copy over single-year NIDs where the source also has merged NIDs
    df.loc[df["merged_nid"].isnull(), "merged_nid"] = df[df["merged_nid"].isnull()].old_nid

    # Rename merged_nid.
    df = df.rename(columns={"merged_nid": "nid"})

    # Drop old NID.
    df = df.drop("old_nid", axis=1)

    col_diff = set(df.columns).symmetric_difference(set(columns_before))
    if col_diff:
        raise KeyError(f"Columns have changed. The difference is {col_diff}.")

    if fillna:
        df.loc[df["nid"].isnull(), "nid"] = -1

    if assert_no_nulls:
        if df["nid"].isnull().sum() != 0:
            raise ValueError("Some values in the nid column are null.")

    return df


def full_coverage_sources() -> List[str]:
    """Retrieve source names that do not use the envelope.

    Returns:
        List of source names that do not use the envelope.
    """
    db = Database()
    db.load_odbc("DATABASE")
    dql = (
        "QUERY"
    )
    return db.query(dql).source_name.tolist()


def drop_data(df: pd.DataFrame, gbd_start_year: int, verbose: bool = False) -> pd.DataFrame:
    """Function that drops the data that we don't want.

    Reasons are outlined in comments.

    Raises:
        KeyError if a year-type column is missing.
        RunTimeError if all rows are dropped.

    Returns:
        Dataframe with dropped rows.
    """

    # facility type, exclusion stats
    # hospital : inpatient
    # day clinic : EXCLUDED
    # emergency : EXCLUDED
    # clinic in hospital : outpatient, but has different pattern than other Outpatient
    # outpatient clinic : outpatient
    # inpatient unknown : inpatient
    # outpatient unknown : outpatient

    # Print how many rows we're starting with.
    print("STARTING NUMBER OF ROWS = " + str(df.shape[0]))
    print("\n")

    # Drop everything but inpatient.
    # Our envelope is only inpatient so we only keep inpatient, for now.
    df = df[df["facility_id"].isin(INPATIENT_FACILITIES)]
    if verbose:
        print("DROPPING OUTPATIENT")
        print("NUMBER OF ROWS = " + str(df.shape[0]))
        print("\n")

    # Drop secondary diagnoses.
    df = df[df["diagnosis_id"] == 1]  # Envelope only works with primary admits.
    if verbose:
        print("DROPPING ALL SECONDARY DIAGNOSES")
        print("NUMBER OF ROWS = " + str(df.shape[0]))
        print("\n")

    # Drop Canada data. They mix up inpatient/outpatient.
    df = df[df["source"] != "CAN_NACRS_02_09"]
    df = df[df["source"] != "CAN_DAD_94_09"]
    if verbose:
        print("DROPPING CANADA DATA")
        print("NUMBER OF ROWS = " + str(df.shape[0]))
        print("\n")

    # Drop data that is at national level for HCUP.
    df = df[(df.location_id != 102) | (df.source != "USA_HCUP_SID")]
    if verbose:
        print("DROPPING HCUP NATIONAL LEVEL USA DATA")
        print("NUMBER OF ROWS = " + str(df.shape[0]))
        print("\n")

    # Drop data that is from before 1990, because the envelope starts at 1990.
    if verbose:
        print(f"DROPPING DATA FROM BEFORE YEAR {gbd_start_year}")
    if "year_id" in df.columns:
        df = df[df["year_id"] >= gbd_start_year]
    elif "year_start" in df.columns:
        df = df[df["year_start"] >= gbd_start_year]
    else:
        raise KeyError("The data must have year_id or year_start.")
    print("FINAL NUMBER OF ROWS = " + str(df.shape[0]))
    print("\n")

    # Check that we didn't somehow drop all rows.
    if not df.shape[0] > 0:
        raise RuntimeError("All data was dropped, there are zero rows!")
    return df


def make_zeros(
    df: pd.DataFrame,
    cols_to_square: Union[str, List[str]],
    ages: Optional[List[int]] = None,
    etiology: str = "bundle_id",
) -> pd.DataFrame:
    """Takes a dataframe and returns the square of every age/sex/cause type (bundle or ICG)
    which **exists** in the given dataframe, but only the years available
    for each location ID.

    Args:
        df: Clinical data.
        cols_to_square: The columns to add rows and fill with zeros.
        ages: Age group ids to use.
        etiology: Disease grouping type to square across.

    Raises:
        ValueError if ages is not a list.
        ValueError if the data has been year-binned.

    Returns:
        DataFrame with additional rows for missing combinations of demographic groups.
    """
    if isinstance(cols_to_square, str):
        cols_to_square = [cols_to_square]

    sqr_df_list = []

    # Determine which set of ages to use.
    if ages is None:
        ages = df["age_group_id"].unique().tolist()

    if not isinstance(ages, list):
        raise ValueError("The object ages must be a list")

    sexes = df["sex_id"].unique().tolist()

    # Get all the unique bundles by source and location_id.
    src_loc = df[["source", "location_id"]].drop_duplicates()
    src_bundle = df[["source", etiology]].drop_duplicates()
    loc_bundle = src_bundle.merge(src_loc, how="outer", on="source")

    for loc in df["location_id"].unique():
        cause_type = (
            loc_bundle.loc[loc_bundle["location_id"] == loc, etiology].unique().tolist()
        )

        dat = pd.DataFrame(
            expandgrid(
                ages,
                sexes,
                [loc],
                df[df["location_id"] == loc]["year_start"].unique(),
                cause_type,
            )
        )

        dat.columns = pd.core.indexes.base.Index(
            ["age_group_id", "sex_id", "location_id", "year_start", etiology]
        )
        sqr_df_list.append(dat)

    sqr_df = pd.concat(sqr_df_list)
    del sqr_df_list
    sqr_df["year_end"] = sqr_df["year_start"]
    if (df["year_start"] != df["year_end"]).any():
        raise ValueError("This function will not work with binned data.")

    # Get a key to fill in missing values for newly created rows.
    keep_cols = [
        c
        for c in [
            "location_id",
            "year_start",
            "facility_id",
            "nid",
            "representative_id",
            "source",
        ]
        if c in df.columns
    ]
    missing_col_key = df[keep_cols].drop_duplicates().copy()

    # Inner merge sqr and missing col key to get all the column info we lost.
    final_df = sqr_df.merge(missing_col_key, how="inner", on=["location_id", "year_start"])

    # Left merge on our built out template df to create zero rows.
    merge_cols = [
        "age_group_id",
        "sex_id",
        "location_id",
        "year_start",
        "year_end",
        etiology,
        "facility_id",
        "nid",
        "representative_id",
        "source",
    ]
    merge_cols = [c for c in merge_cols if c in df.columns]
    final_df = final_df.merge(df, how="left", on=merge_cols)

    # Fill missing values of the col of interest with 0.
    for col in cols_to_square:
        final_df[col] = final_df[col].fillna(0)

    return final_df


def report_if_merge_fail(
    df: pd.DataFrame,
    check_col: str,
    id_cols: List[str],
    store: bool = False,
    filename: str = "",
) -> None:
    """Report a merge failure if there is one.

    Args:
        df: Df that you want to check for nulls created in the merge.
        check_col: Name of the column that was merged on.
        id_cols: List containing the names of the columns that were used to merge on
            the check_col.
        store: If True, will save a CSV copy of the rows with nulls in check_col at
            FILEPATH/
        filename: Filename used to save the copy of null rows if store is True.
    """
    merge_fail_text = """
        Could not find {check_col} for these values of {id_cols}:

        {values}
    """
    if df[check_col].isnull().any():

        # 'missing' can be df or series.
        missing = df.loc[df[check_col].isnull(), id_cols]

        if store:
            missing.to_csv(
                "FILEPATH/{}.csv".format(filename),
                index=False,
                encoding="utf-8",
            )

        raise AssertionError(
            merge_fail_text.format(check_col=check_col, id_cols=id_cols, values=missing)
        )
    else:
        pass


def fix_col_dtypes(df: pd.DataFrame, errors: str = "raise") -> pd.DataFrame:
    """Takes a dataframe with mixed types and returns them with numeric type
    This is something that has been happening a lot in our data, probably
    due to some merges.

    Args:
        df: DataFrame of hospital data.
        errors: A string to pass to pd.numeric to raise, coerce or ignore errors.

    Raises:
        KeyError if none of the columns to "fix" are present in the data.

    Returns:
        DataFrame with numeric columns.
    """

    num_cols = [
        "sex_id",
        "location_id",
        "year_start",
        "year_end",
        "bundle_id",
        "nid",
        "representative_id",
    ]
    # Drop the cols that aren't in the data.
    num_cols = [n for n in num_cols if n in df.columns]

    if len(num_cols) < 1:
        fail_msg = """
        There are not any of our expected columns in the dataframe. The columns in
        df are {}
        """.format(
            df.columns
        )
        raise KeyError(fail_msg)

    # Do the actual casting.
    for col in num_cols:
        # mypy wants Literal["raise", "coerce"], but it's Literal["ignore", "raise", "coerce"]
        df[col] = pd.to_numeric(df[col], errors=errors)  # type:ignore[call-overload]

    return df


def apply_inj_corrections(
    df: pd.DataFrame, run_id: int, bin_years: bool, iw_profile: str
) -> pd.DataFrame:
    """Apply output of `prop_code_for_hosp_team.py` to data aggregated into five-year bands.

    Args:
        df: A DataFrame of hospital data containing injuries.
        bin_years: Identifies whether data is in five-year bins or not.

    Raises:
        ValueError if the injury correction factors are null.
        RuntimeError if there's more than a single estimate_id in the input data.
        ValueError if row or column counts change unexepectedly.

    Returns:
        Pandas DataFrame with injury correction factors applied.
    """
    pre_tuple = df.shape
    # Get injury bundles.
    pc_injuries = bch.BundleCause(
        cause_id=687,  # _inj cause_id
        map_version=pipeline.get_map_version(run_id),
        release_id=pipeline.get_release_id(run_id),
    ).bundles_by_cause()

    # Create a new df with only injuries bundles for the CFs.
    inj_df = df[(df["bundle_id"].isin(pc_injuries)) & (df["estimate_id"] == 1)].copy()

    iw = InpatientWrappers(run_id, iw_profile)
    cf_dir = iw.pull_version_dir("inj_cf_en")

    merge_cols = ["location_id", "year_start", "year_end"]
    inj_cf: pd.DataFrame
    if bin_years:
        inj_cf = pd.DataFrame(pd.read_hdf(f"{cf_dir}/inj_factors_collapsed_years.H5"))
    else:
        merge_cols.append("nid")
        inj_cf = pd.DataFrame(pd.read_hdf(f"{cf_dir}/inj_factors.H5"))
    # Prep inj cf data. Facility_id used to identify inp vs. outpatient from the en prop code.
    inj_cf = inj_cf[inj_cf["facility_id"].isin(INPATIENT_FACILITIES)]
    inj_cf = inj_cf.drop(["facility_id", "prop", "remove"], axis=1)

    inj_cf = inj_cf.rename(columns={"factor": "correction_factor"})

    # Merge scalars on by loc and year.
    pre = inj_df.shape[0]
    inj_df = inj_df.merge(inj_cf, how="left", on=merge_cols)
    if pre != inj_df.shape[0]:
        raise ValueError(f"The merge changed rows. From {pre} to {inj_df.shape[0]}")

    no_inj_srcs = ["IDN_SIRS", "UK_HOSPITAL_STATISTICS", "IRN_MOH"]

    if inj_df[~inj_df.source.isin(no_inj_srcs)].correction_factor.isnull().sum() != 0:
        raise ValueError(
            "There shouldn't be null correction factors in source(s) {} \n{}".format(
                [
                    x
                    for x in inj_df[inj_df["correction_factor"].isnull()].source.unique()
                    if x not in no_inj_srcs
                ],
                inj_df[
                    (inj_df["correction_factor"].isnull()) & (~inj_df.source.isin(no_inj_srcs))
                ],
            )
        )

    # Apply the scalar to mean/lower/upper.
    levels = ["mean", "lower", "upper", "median_CI_team_only"]
    for level in levels:
        inj_df[level] = inj_df[level] * inj_df["correction_factor"]

        # Test that the inj CFs aren't null.
        if level in ["mean"]:
            if (
                inj_df.loc[
                    (~inj_df["source"].isin(no_inj_srcs)) & (inj_df["mean"] != 0), level
                ]
                .isnull()
                .sum()
                != 0
            ):
                raise ValueError(
                    "There shouldn't be null correction factors in source(s) {} \n{}".format(
                        [
                            x
                            for x in df[df[level].isnull()].source.unique()
                            if x not in no_inj_srcs
                        ],
                        inj_df[(inj_df[level].isnull()) & (~inj_df.source.isin(no_inj_srcs))],
                    )
                )
        else:
            if (
                inj_df.loc[
                    (~inj_df["source"].isin(no_inj_srcs)) & (inj_df["mean"] != 0), level
                ]
                .isnull()
                .sum()
                != 0
            ):
                raise ValueError(
                    "There shouldn't be null correction factors in source(s) {} \n{}".format(
                        [
                            x
                            for x in df[df[level].isnull()].source.unique()
                            if x not in no_inj_srcs
                        ],
                        inj_df[(inj_df[level].isnull()) & (~inj_df.source.isin(no_inj_srcs))],
                    )
                )

    pre_cf_estimates = inj_df.estimate_id.unique().size
    if pre_cf_estimates != 1:
        raise RuntimeError(
            "We're overwriting too many unique estimate types or data "
            f"went missing. There were {pre_cf_estimates} unique estimate "
            "IDs before correcting."
        )
    # Manually set the estimate ID after the correction is made.
    inj_df["estimate_id"] = 22

    df = pd.concat([df, inj_df], ignore_index=True, sort=False)
    if df.shape[0] != pre_tuple[0] + inj_df.shape[0]:
        raise ValueError("The number of rows have changed unexpectedly.")
    if df.shape[1] != pre_tuple[1] + 1:
        raise ValueError("The number of columns have changed unexpecedly.")
    return df


def remove_injuries_under_cutoff(
    df: pd.DataFrame, run_id: int, bin_years: bool, iw_profile: str
) -> pd.DataFrame:
    """Removes rows in Injury bundles (besides 271) that do not meet a cutoff
    criterion based on the percentage of E-Codes. This doesn't apply to bundle
    271, medical injuries.

    The percentage cutoff is set to 15%, and is documented further in the
    script that makes is Injuries/prop_code_for_hosp_team.py.

    Args:
        df: Data with collapsed years.
        run_id: The run_id to pull the map version and release ID from.
        bin_years: Identifies whether data is in five-year bins or not.
        iw_profile: The profile to use for the InpatientWrappers.

    Raises:
        ValueError if certain estimates are present in the data.
        ValueError if row counts change.
        ValueError if null values exceed a certain threshold.
        KeyError if columns change.

    Returns:
        Data with rows that met the cutoff criterion dropped.
    """

    starting_columns = df.columns.tolist()

    invalid_estimate_ids = [17, 21]  # Don't want claims data.

    if set(invalid_estimate_ids) <= set(df.estimate_id.unique()):
        raise ValueError("Unsupported estimate_ids are in the data.")

    # Get injury bundle IDs.
    inj_bundles = bch.BundleCause(
        cause_id=687,  # _inj cause_id
        map_version=pipeline.get_map_version(run_id),
        release_id=pipeline.get_release_id(run_id),
    ).bundles_by_cause()

    # Do not apply this to 271 medical injuries.
    if 271 in inj_bundles:
        inj_bundles.remove(271)

    # Create a new df with only injuries bundles for the CFs.
    # Discard inj from other df. Will append later.
    is_injury_mask = df["bundle_id"].isin(inj_bundles)
    inj_df = df[is_injury_mask].copy()
    df = df[~is_injury_mask].copy()  # drop from main df

    # There may not be any injuries data in the DF.
    # Return the df object.
    if len(inj_df) == 0:
        print("There wasn't any injuries data")

    # Get the injuries cutoff info. It's the same file that has inj cfs.
    iw = InpatientWrappers(run_id, iw_profile)
    en_dir = iw.pull_version_dir("inj_cf_en")

    merge_cols = ["location_id", "year_start", "year_end"]
    if bin_years:
        inj_cf = pd.read_hdf(f"{en_dir}/inj_factors_collapsed_years.H5")
    else:
        merge_cols.append("nid")
        inj_cf = pd.read_hdf(f"{en_dir}/inj_factors.H5")

    # Facility_id is used to identify inp vs outpatient from the en prop code.
    inj_cf = inj_cf[inj_cf["facility_id"].isin(INPATIENT_FACILITIES)]

    # Don't need these columns.
    inj_cf = inj_cf.drop(["facility_id", "prop", "factor"], axis=1)

    # Merge scalars on by loc and year.
    pre = inj_df.shape[0]
    inj_df = inj_df.merge(inj_cf, how="left", on=merge_cols)
    assert_msg = "The merge changed rows. Review this {} vs {}".format(pre, inj_df.shape[0])
    if pre != inj_df.shape[0]:
        raise ValueError(assert_msg)

    # Kind of an arbitrary test that might catch some issues.
    percent_null = inj_df[inj_df.remove.isnull()].shape[0] / inj_df.shape[0] * 100
    if not percent_null < 25:
        raise ValueError("There are too many missing values.")

    # Drop rows under cutoff.
    inj_df = inj_df[inj_df.remove != 1].copy()

    # Prepare to put back together.
    inj_df = inj_df.drop("remove", axis=1)

    # Make sure columns match.
    if set(df.columns) != set(inj_df.columns):
        raise KeyError("The columns of the two dataframes don't match.")

    # Put back together.
    df = pd.concat([inj_df, df], ignore_index=True, sort=False)

    # Make sure we have everything.
    if set(df.columns) != set(starting_columns):
        raise KeyError("Current columns don't match starting columns.")

    return df


def fix_src_names(df: pd.DataFrame) -> pd.DataFrame:
    """The database has stripped trailing years in our source names, but they're
    present in the data. This function will remove trailing years from a source
    name.

    Args:
        df: A pandas dataframe with a 'source' column of
            clinical source names.

    Returns:
        Df with the updated source names.
    """

    if "source" not in df.columns:
        raise ValueError("The source column must be present.")
    else:
        if df[["source"]].head().select_dtypes(include=["category"]).shape[1] == 1:
            print("Casting the source column to string.")
            df["source"] = df["source"].astype(str)
        else:
            pass
        srcs = df.source.unique().tolist()

    # Regex to remove the trailing year values.
    srcs_dict = {re.sub("[^A-Z, a-z]*$", "", e): e for e in srcs}

    for k, v in srcs_dict.items():
        if "SWE" in k:
            k = "SWE_PATIENT_REGISTRY"
        if k != v:
            df.loc[df.source == v, "source"] = k
        else:
            continue

    return df


def get_sample_size(
    df: pd.DataFrame, clinical_age_group_set_id: int, run_id: int, use_cached_pop: bool = False
) -> pd.DataFrame:
    """This function attaches sample size to hospital data.

    It's for sources that should have fully covered populations, so sample size
    is just population. Checks if age_group_id is a column that exists, and if not,
    it attaches it.

    Args:
        df: Contains the data that you want to add sample_size to. Will add
            pop to every row.
    """
    if "year_id" in df.columns:
        df.rename(columns={"year_id": "year_start"}, inplace=True)
        df["year_end"] = df["year_start"]

    # Process:
    # Merge age_group_id onto data if not present.
    # Run get_pop with the age_group_ids in the data.
    # Attach pop by age_group_id.
    if "age_group_id" not in df.columns:
        # Pull age_group to age_start/age_end map.
        age_group = demographic.get_hospital_age_groups(
            clinical_age_group_set_id=clinical_age_group_set_id
        )

        # Merge age group ID on.
        pre = df.shape[0]
        df = df.merge(age_group, how="left", on=["age_start", "age_end"])
        if df.shape[0] != pre:
            raise ValueError("Number of rows changed during merge.")
        if not df.age_group_id.notnull().all():
            raise ValueError("age_group_id is missing for some rows.")

    # Get population.
    if use_cached_pop:
        if run_id is None:
            raise ValueError("run_id cannot be None if you want to pull cached population.")
        sum_under1 = False
        pop = get_cached_pop(run_id=run_id, sum_under1=sum_under1)
        # Retain expected col name.
        pop.rename(columns={"pop_run_id": "run_id"}, inplace=True)

    else:
        pop = get_population(
            age_group_id=list(df.age_group_id.unique()),
            location_id=list(df.location_id.unique()),
            sex_id=[1, 2, 3],
            year_id=list(df.year_start.unique()),
            release_id=get_release_id(run_id=run_id),
        )

    # Rename pop columns to match hospital data columns.
    pop.rename(columns={"year_id": "year_start"}, inplace=True)
    pop["year_end"] = pop["year_start"]
    pop.drop("run_id", axis=1, inplace=True)

    demography = ["location_id", "year_start", "year_end", "age_group_id", "sex_id"]

    # Merge on population.
    pre_shape = df.shape[0]
    df = df.merge(pop, how="left", on=demography)  # Attach pop info to hosp.
    if pre_shape != df.shape[0]:
        raise ValueError("Number of rows don't match after merge.")
    if not df.population.notnull().all():
        raise ValueError(
            "Population is missing for some rows. Look at this df! \n {}".format(
                df.loc[df.population.isnull(), demography].drop_duplicates()
            )
        )

    return df
