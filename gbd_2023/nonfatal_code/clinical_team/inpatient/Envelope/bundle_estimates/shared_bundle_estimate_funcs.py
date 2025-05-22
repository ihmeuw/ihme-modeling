import itertools
import time
import warnings
from pathlib import Path
from typing import Dict, List, Tuple, Union

import pandas as pd
from crosscutting_functions import (
    demographic,
    general_purpose,
    legacy_pipeline,
)
from crosscutting_functions.get-cached-population import cached_pop_tools
from crosscutting_functions.validations.decorators import clinical_typecheck
from crosscutting_functions.clinical_mapping import clinical_mapping


def read_split_icg(read_path: str, year_start: int, bin_years: bool) -> pd.DataFrame:
    """Read in the two draw files and then keep only the year we want

    Args:
        read_path: file path to the split_icg file
        year_start: year start of the particular file
        bin_years: whether the run is in 5-year bin or not

    Returns:
        Filtered split_icg dataframe
    """
    year_end = year_start + 4 if bin_years else year_start
    df = []

    # when running only a handful of soruces there might only be
    # one type of denominator (e.g. envelope or GBD pop) and
    # years / ages might be missing
    age_sex = Path(read_path).stem.split("_")
    sex = int(age_sex.pop())
    age_group = int(age_sex.pop())

    ftypes = [
        ext
        for ext in ["_df_env.H5", "_df_fc.H5"]
        if Path(read_path).with_name(f"{age_group}_{sex}{ext}").is_file()
    ]

    assert ftypes, "missing files for demo"

    for ftype in ftypes:
        tmp = pd.read_hdf(f"{read_path}{ftype}", key="df")
        tmp = tmp.query(
            f"age_group_id == {age_group} & sex_id == {sex} & "
            f"year_id >= {year_start} & year_id <= {year_end}"
        )
        df.append(tmp)
    df = pd.concat(df, sort=False, ignore_index=True)

    assert df["year_id"].unique().size <= 5, "too many years"

    # should only contain these but just in case
    df = df.loc[df["estimate_id"].isin([1, 6])]

    return df


def create_helpers(df: pd.DataFrame) -> Dict[str, Union(List[str], pd.DataFrame)]:
    """
    Create a collection of helpers that are used to vaildate data
    after transformation steps

    Args:
        df: dataframe to create helpers from

    Returns:
        Collection of helpers of list+dfs
    """

    full_coverage_sources = legacy_pipeline.full_coverage_sources()

    FC_LOC_YEARS = df[["location_id", "year_id"]].drop_duplicates()
    DRAWS_LOC_YEARS = df.loc[
        ~df["source"].isin(full_coverage_sources), ["location_id", "year_id"]
    ].drop_duplicates()

    MISSING_COL_KEY = df[
        [
            "location_id",
            "year_id",
            "source_type_id",
            "nid",
            "representative_id",
            "source",
            "run_id",
            "diagnosis_id",
        ]
    ].copy()
    MISSING_COL_KEY.drop_duplicates(inplace=True)

    return {
        "full_coverage_sources": full_coverage_sources,
        "FC_LOC_YEARS": FC_LOC_YEARS,
        "DRAWS_LOC_YEARS": DRAWS_LOC_YEARS,
        "MISSING_COL_KEY": MISSING_COL_KEY,
    }


def prep_data(config_args) -> Tuple[pd.DataFrame, Dict[str, Union(List[str], pd.DataFrame)]]:
    """
    Return the split_icg file and a collection of helper list and dfs

    Args:
        config_args: dataclass made in cli.py

    Returns:
        dataframe at the icg level and also collection of helpers of list+dfs
    """

    read_path = (FILEPATH
    )
    df = read_split_icg(read_path, config_args.year_start, config_args.bin_years)
    return df, create_helpers(df)


def drop_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    icg id and name are dropped because the data has been mapped to bundle.
    Sample size and cases are dropped because they will be re-made when
    aggregating to 5 years. They also contain Null values which lose rows
    in a groupby

    Args:
        df: dataframe after mapping to bundle

    Returns:
        dataframe at the bundle level with extra columns dropped
    """
    drop_cols = ["icg_id", "icg_name", "cases", "sample_size"]

    drop_cols.append("mean")

    to_drop = [c for c in drop_cols if c in df.columns]
    df = df.drop(to_drop, axis=1)

    if df.empty:
        raise RuntimeError("All the data has been lost in this function")

    return df


def agg_to_bundle(df: pd.DataFrame, map_version: int) -> pd.DataFrame:
    """
    maps to bundle id
    drops columns that will cause trouble in the merge
    sums mean values together to get from ICG to bundle rates

    Args:
        df: dataframe at the icg level
        map_version: clinical map_version

    Returns:
        dataframe mapped to the bundle level and aggregated
    """

    df = expand_bundles(df, map_version)

    df = drop_cols(df)

    sum_cols = df.filter(regex="draw_").columns.tolist()

    # use all columns except those which we sum to aggregate to bundle
    grouper = df.columns.drop(sum_cols).tolist()

    sum_dict = dict(list(zip(sum_cols, ["sum"] * len(sum_cols))))

    df = df.groupby(grouper).agg(sum_dict).reset_index()

    if df.empty:
        raise RuntimeError("All the data has been lost in this function")

    return df


def expand_bundles(
    df: pd.DataFrame, map_version: int, drop_null_bundles: bool = True
) -> pd.DataFrame:
    """
    This Function maps ICGs to Bundles.
    When our data is at the baby seq level there are no duplicates, one icd code
    goes to one baby sequela. At the bundle level there are multiple bundles
    going to the same ICD code so we need to duplicate rows to process every
    bundle. This function maps bundle ID onto baby sequela and duplicates rows.
    Then it can drop rows without bundle ID.  It does this by default.

    Args:
        df: Must have icg_id column
        map_version: clinical map_version
        drop_null_bundles: If true, will drop the rows of data with ICGs that do not
            map to a bundle.

    Returns:
        dataframe mapped to the bundle level
    """

    if "bundle_id" in df.columns:
        raise KeyError("bundle_id has already been attached")
    if "icg_id" not in df.columns:
        raise KeyError("icg_id must be a column")

    df = clinical_mapping.map_to_gbd_cause(
        df,
        input_type="icg",
        output_type="bundle",
        write_unmapped=False,
        truncate_cause_codes=False,
        extract_pri_dx=False,
        prod=True,
        groupby_output=False,
        retain_active_bundles=True,
        map_version=map_version,
    )

    if drop_null_bundles:
        # drop rows without bundle id
        df = df[df["bundle_id"].notnull()]

    if df.empty:
        raise RuntimeError("All the data has been lost in this function")

    return df


def expandgrid(*itrs):
    """create a template df with every possible combination of passed in
    lists."""
    product = list(itertools.product(*itrs))
    return {"Var{}".format(i + 1): [x[i] for x in product] for i in range(len(itrs))}


def pooled_square_builder(
    loc: int,
    est_types: List[int],
    ages: List[int],
    sexes: List[int],
    loc_bundle: pd.DataFrame,
    loc_years: pd.DataFrame,
    eti_est_df: pd.DataFrame,
    etiology: str,
) -> pd.DataFrame:
    """Build a squared dataframe with the given variables and values

    Args:
        loc: a single location_id
        est_types: list of estimate_ids
        ages: list of age_group_ids
        sexes: list of sex_ids
        loc_bundle: pd.DataFrame of unique bundles by source and location_id
        loc_years: pd.DataFrame of all available location years
        eti_est_df: pd.DataFrame of estimate/bundle_id that our pipeline creates
        etiology: currently only str of bundle_id

    Returns:
        Fully built out square pd.DataFrame
    """

    # list of values in the etiology column for the given location
    cause_type = loc_bundle.loc[loc_bundle.location_id == loc, etiology].unique().tolist()

    dat = pd.DataFrame(
        expandgrid(
            ages,
            sexes,
            [loc],
            loc_years[loc_years["location_id"] == loc]["year_id"].unique(),
            est_types,
            cause_type,
        )
    )
    dat.columns = [
        "age_group_id",
        "sex_id",
        "location_id",
        "year_id",
        "estimate_id",
        etiology,
    ]
    dat = dat.merge(eti_est_df, how="left", on=[etiology, "estimate_id"])
    dat = dat[dat["keep"] == 1]
    dat = dat.drop("keep", axis=1)

    return dat


@clinical_typecheck
def make_zeros(
    df: pd.DataFrame,
    cols_to_square: List[str],
    run_id: int,
    helpers: Dict[str, Union(List[str], pd.DataFrame)],
    etiology: str = "bundle_id",
) -> pd.DataFrame:
    """Takes a dataframe and returns the square of every
    age/sex/cause type(bundle or ICG) which
    exists in a bundle/location map made in the 'apply_env' step
    but only the years available for each location id

    Args:
        df: pre-squared dataframe
        cols_to_square: all draw columns that we want to fill in
        run_id: clinical run_id
        helpers: Collection of helpers of list+dfs from create_helpers.
        etiology: currently only str of bundle_id. Defaults to "bundle_id"

    Returns:
        post-squared dataframe at the etiology level
    """
    if not isinstance(cols_to_square, list):
        raise TypeError("Expected cols_to_square to be a list of draw_cols")

    ages = df["age_group_id"].unique().tolist()
    sexes = df["sex_id"].unique().tolist()
    est_types = df["estimate_id"].unique().tolist()

    # get all the unique bundles by source and location_id
    loc_bundle_path = (FILEPATH
    )
    loc_bundle = pd.read_csv(loc_bundle_path)

    # check that this file has all the bundles we need it to
    if set(df["bundle_id"].unique()) != set(loc_bundle["bundle_id"].unique()):
        raise RuntimeError("The loc_bundle dataframe is missing bundle(s) in df")

    loc_years = pd.concat(
        [helpers["DRAWS_LOC_YEARS"], helpers["FC_LOC_YEARS"]], sort=False, ignore_index=True
    )

    # Pull in the estimate/bundle id dataframe that our pipeline creates
    eti_est_df = pd.read_csv(FILEPATH
    )

    sqr_df_list = []

    # get unique locations where a bundle is present
    locs = loc_years["location_id"].unique().tolist()

    for loc in locs:
        temp_sqr_df = pooled_square_builder(
            loc=loc,
            ages=ages,
            sexes=sexes,
            loc_years=loc_years,
            loc_bundle=loc_bundle,
            eti_est_df=eti_est_df,
            etiology=etiology,
            est_types=est_types,
        )
        sqr_df_list.append(temp_sqr_df)
    sqr_df = pd.concat(sqr_df_list, ignore_index=True, sort=False)

    # inner merge sqr and missing col key to get all the column info we lost
    pre = sqr_df.shape[0]
    sqr_df = sqr_df.merge(
        helpers["MISSING_COL_KEY"], how="inner", on=["location_id", "year_id"]
    )
    if pre != sqr_df.shape[0]:
        raise RuntimeError(
            "Should cases have increased from {} to {}?".format(pre, sqr_df.shape[0])
        )

    # left merge on our built out template df to create zero rows
    sqr_df = sqr_df.merge(
        df,
        how="left",
        on=[
            "age_group_id",
            "sex_id",
            "location_id",
            "year_id",
            "estimate_id",
            etiology,
            "source_type_id",
            "nid",
            "representative_id",
            "source",
            "run_id",
            "diagnosis_id",
        ],
    )
    if pre != sqr_df.shape[0]:
        raise RuntimeError(
            "Rows should not have changed from {} to {}?".format(pre, sqr_df.shape[0])
        )

    # fill missing values of the col of interest with zero
    for col in cols_to_square:
        sqr_df[col] = sqr_df[col].fillna(0)

    return sqr_df


def make_square(
    df: pd.DataFrame, config_args, helpers: Dict[str, Union(List[str], pd.DataFrame)]
) -> pd.DataFrame:
    """
    Function that inserts zeros for demographic and etiolgy groups that were not
    present in the source.  A zero is inserted for every age/sex/etiology
    combination for each location and for only the years available for that
    location. If a location has data from 2004-2005 we will create explicit
    zeroes for those years but not for any others.

    Age and sex restrictions are applied after the data is made square.

    Args:
        df: Must be aggregated and collapsed to the bundle level
        config_args: dataclass made in cli.py
        helpers: collection of helpers of list+dfs from create_helpers

    Returns:
        post-squared dataframe at the etiology level
    """
    start_square = time.time()
    if "bundle_id" not in df.columns:
        raise KeyError("bundle_id must exist")
    if "icg_id" in df.columns:
        raise KeyError("Data must not be at the icg_id level")

    draw_cols = [col for col in df.columns if col.startswith("draw_")]

    # create a series of sorted, non-zero values to make sure
    # the func doesn't alter anything
    check_non_zero_draws = df.loc[df[draw_cols].sum(axis=1) != 0, draw_cols].reset_index(
        drop=True
    )

    # square the dataset
    df = make_zeros(
        df,
        run_id=config_args.run_id,
        etiology="bundle_id",
        cols_to_square=draw_cols,
        helpers=helpers,
    )

    # check if the non-zero draws are unchanged
    if len(check_non_zero_draws) != len(
        df.loc[df[draw_cols].sum(axis=1) != 0, draw_cols].reset_index(drop=True)
    ):
        raise RuntimeError("Make_zero() changed non-zero draws!")

    print("Data squared in {} min".format((time.time() - start_square) / 60))

    p = pd.read_pickle(FILEPATH
    )
    # re-apply age sex restrictions after making data square
    print("\n\nThis is using age group set 1 for restricts while data has set 2\n")
    df = clinical_mapping.apply_restrictions(
        df,
        age_set="age_group_id",
        cause_type="bundle",
        clinical_age_group_set_id=p.clinical_age_group_set_id,
        map_version=config_args.map_version,
    )

    print(
        "Data made square and age/sex restrictions applied in {} min".format(
            (time.time() - start_square) / 60
        )
    )

    return df


def merge_denominator(
    df: pd.DataFrame, run_id: int, full_coverage_sources: List[str]
) -> pd.DataFrame:
    """
    Merges denominator stored from when cause fractions were run.  Returns df
    with a column named "denominator".  Meant to be run after data has been
    made square.  There WILL be null values of denominator after the merge.
    This would happen whenever there was a demographic that literally had no
    admissions whatsoever in a data source.  At the moment it is an open
    question whether or not we should drop these null rows, or give them a
    'denominator' value of zero.

    This is a function susceptible to problems. If create_cause_fractions was
    ran a while ago, the information may not match or be up to date anymore.
    Making the denominator should happen right before this function is used.

    The purpose of this is so that the rows that are inserted as zero to make
    the data square can have information for uncertainty for the inserted rows.

    Args:
        df: A pandas dataframe of clinical data.
        run_id: Standard clinical run_id used to pull denominators generated earlier in
                the pipeline run.
        full_coverage_sources: A list of full coverage source names used to identify rows
                               which should use GBD population denominators rather than
                               cause fraction denominators.

    Returns:
        dataframe with valid denominator merged in
    """
    chk1 = df.filter(regex="draw_0").columns[0]

    denom_path = (FILEPATH
    )
    denoms = pd.read_csv(denom_path)
    denoms = demographic.year_id_switcher(denoms)

    pre = df.shape[0]
    df = df.merge(denoms, how="left", on=["age_group_id", "sex_id", "year_id", "location_id"])

    df = df.rename({"val": "denominator"}, axis=1)

    if pre != df.shape[0]:
        raise RuntimeError("number of rows changed after merge")

    # denom for full coverage sources is pop
    df.loc[df["source"].isin(full_coverage_sources), "denominator"] = df.loc[
        df["source"].isin(full_coverage_sources), "population"
    ]

    # check that data is zero when denominator is null
    assert (df.loc[df["denominator"].isnull(), chk1] == 0).all(), (chk1, " should be 0")

    # This removes rows that were inserted for demographic groups that never
    # existed in hospital data sources.
    print("pre null denom drop shape", df.shape)
    df = df[df["denominator"].notnull()]
    print("post null denom", df.shape)

    return df


def merge_population(df: pd.DataFrame, run_id: int) -> pd.DataFrame:
    """
    Function that attaches population info to the DataFrame.  Checks that there
    are no nulls in the population columns.  This has to be ran after the data
    has been made square!

    Args:
        df: Pandas DataFrame
        run_id: clinical run_id

    Returns:
        dataframe with valid population merged in
    """
    chkcol = "draw_3"  # not draw0 to make sure 0's are getting propogated

    zero_msg = """There are no rows with zeros, implying
        that the data has not been made square.  This function should be ran
        after the data is square"""
    if not (df[chkcol] == 0).any():
        warnings.warn(zero_msg)

    # pull population
    # if this ever needs to be True refactor based on that need
    sum_under1 = False
    pop = cached_pop_tools.get_cached_pop(
        run_id=run_id, sum_under1=sum_under1, drop_duplicates=True
    )

    demography = ["location_id", "year_id", "sex_id", "age_group_id"]
    # keep only merge cols and pop
    pop = pop[demography + ["population"]]

    pre_shape = df.shape[0]  # store for before comparison
    # then merge population onto the hospital data
    df = df.merge(pop, how="left", on=demography)  # attach pop info to hosp

    if pre_shape != df.shape[0]:
        raise RuntimeError("number of rows don't match after merge")

    return df


def write_draws(df: pd.DataFrame, config_args) -> pd.DataFrame:
    """Write draws to disk

    Args:
        df: post-squared dataframe
        config_args: dataclass made in cli.py

    Returns:
        post-squared dataframe with mixed type cols fixed
    """
    # convert mixed type cols to numeric
    df = legacy_pipeline.fix_col_dtypes(df, errors="raise")

    out_dir = "agg_5_years"
    if not config_args.bin_years:
        out_dir = "single_years"

    print("Writing the df file...")
    file_path = (FILEPATH.format(
            r=config_args.run_id,
            a=config_args.age_group,
            s=config_args.sex,
            y=config_args.year_start,
            o=out_dir,
        )
    )
    general_purpose.write_hosp_file(df, file_path, backup=False)
    return df


def make_square_bundle_shape(
    df: pd.DataFrame, config_args, helpers: Dict[str, Union(List[str], pd.DataFrame)]
) -> pd.DataFrame:
    """Map to bundle and square the data

    Args:
        df: dataframe at the icg level pre-squared
        config_args: dataclass made in cli.py
        helpers: collection of helpers of list+dfs from create_helpers

    Returns:
        post-squared dataframe
    """
    df = agg_to_bundle(df, config_args.map_version)

    df = make_square(df, config_args=config_args, helpers=helpers)

    return df
