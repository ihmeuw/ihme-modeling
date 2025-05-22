"""
Functions related to merging on the denominators
"""
from typing import List, Tuple

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import cms as constants
from crosscutting_functions.clinical_constants.shared import mapping as mapping_constants
from crosscutting_functions.maternal import maternal_functions

from cms.src.pipeline.lib import col_reqs
from cms.utils import logger
from cms.utils.process_types import Deliverable


def filter_by_code_system(
    df: pd.DataFrame, code_system_ids: List[int], logger: logger
) -> pd.DataFrame:
    """In edge cases where a bundle maps to only 1 code system we must remove any years that are not
    completely coded to that system to avoid bias.

    Args:
        df (pd.DataFrame): Table of bundle estimate being processed.
        code_system_ids (List[int]): IDs for cause code coding system.
        logger (logger): CMS logger instance.

    Returns:
        pd.DataFrame: Input dataframe with false 0 counts removed.
    """

    cms_code_systems = [s for s in code_system_ids if s in [1, 2]]  # cms only has icd 9 and 10
    if len(cms_code_systems) == 1:
        code_sys = cms_code_systems[0]
        logger.info(
            f"There is only a single code system present, code_system_id: {cms_code_systems}"
        )
        years = mapping_constants.US_CODE_SYSTEM_YEARS[code_sys]
        df = df[df.year_id.isin(years)]
        logger.info(
            f"Retaining only years with full code system bundle mapping. These are {years}"
        )
    return df


def zfill_fips_6_4_ids(df: pd.DataFrame, deliverable: Deliverable) -> pd.DataFrame:
    """Align the 6-4 fips codes by casting them to str and adding leading zeros

    Args:
        df (pd.DataFrame): Table of bundle estimate being processed.
        deliverable (Deliverable): Dataclass that holds deliverable specific
            processing parameters.

    Raises:
        RuntimeError: Function applied to unexpected column.
        RuntimeError: Null fips codes found.

    Returns:
        pd.DataFrame: Input dataframe with fips codes corrected with leading zeros.
    """

    loc_col = deliverable.location_col

    if "fips" not in loc_col:
        raise RuntimeError("This should only be used to zfill fips codes")

    null_location_codes = df[loc_col].isnull().sum()
    if null_location_codes != 0:
        raise RuntimeError(
            f"There are {null_location_codes} null values in {loc_col} which will be converted to 'nan' string"
        )
    df[loc_col] = df[loc_col].astype(str).str.zfill(5)
    return df


def apply_maternal_adjustment(df: pd.DataFrame, run_id: int) -> pd.DataFrame:
    """Applies the maternal denominator adjustment to cms data.

    Args:
        df (pd.DataFrame): CMS data with non-adjusted denominators.
        run_id (int): Clinical run_id found in DATABASE.

    Returns:
        pd.DataFrame: Input df with maternal bundle sample_size limited.
    """

    pre = df.shape

    # add all year col types
    df["year_start"], df["year_end"] = df.year_id, df.year_id
    # get asfr covar and merge it onto the data
    asfr = maternal_functions.get_asfr(df=df, run_id=run_id)
    df = maternal_functions.merge_asfr(df=df, asfr=asfr)
    # remove rows where mean value is zero- it will create nulls
    df = df.loc[df.mean_value != 0, :]
    # reduce to live births
    df["sample_size"] = df["sample_size"] * df["mean_value"]

    df.drop(["mean_value", "year_start", "year_end"], axis=1, inplace=True)

    if pre[1] != df.shape[1]:
        RuntimeError(f"Column count change from {pre[1]} to {df.shape[1]}")

    return df


def validate_merge_denoms(df: pd.DataFrame, deliverable: Deliverable) -> None:
    """Run a few validations after merging the denominators onto the data

    Args:
        df (pd.DataFrame): Table of bundle estimate being processed after
            denominators have been attached.
        deliverable (Deliverable): Dataclass that holds deliverable specific
            processing parameters.

    Raises:
        ValueError: Any failures encountered during validation.
            - Any null sample_size, val, or nid.
            - Any negative sample_size or val.
            - Any duplicates present.
    """

    failures = []
    for col in ["sample_size", "val"]:
        if deliverable.name != "ushd":  # this test is not appropriate at county level
            if df[col].isnull().any():
                m = df[col].isnull().sum()
                failures.append(f"There are {m} missing {col} values")
        if (df[col] < 0).any():
            failures.append(f"There are negative {col} values present")
    if len(df) != len(df.drop_duplicates()):
        dups = len(df) - len(df.drop_duplicates())
        failures.append(f"There are {dups} duplicate rows present")
    if df["nid"].isnull().any():
        null_nid = df["nid"].isnull().sum()
        failures.append(f"There are {null_nid} null NID rows")

    if failures:
        raise ValueError("\n --".join(failures))


def merge_denoms(
    df: pd.DataFrame,
    denoms: pd.DataFrame,
    deliverable: Deliverable,
    code_system_ids: List[int],
    logger: logger,
    cms_system: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Merge the numerator data (df) and the denominator data (denoms) together then test merge.

    Args:
        df (pd.DataFrame): Table of bundle estimate being processed.
        denoms (pd.DataFrame): Table of CMS system specific denominators.
        deliverable (Deliverable): Dataclass that holds deliverable specific
            processing parameters.
        code_system_ids (List[int]): IDs for cause code coding system.
        logger (logger): CMS logger instance.
        cms_system (str): Clinical CMS system abbreviation.
            ex: 'mdcr'

    Raises:
        RuntimeError: No data to perform denom merge onto.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: [0] Input dataframe, filtered to allowed
            GBD location and denomiator data attached.  [1] All year denomiator data.
    """

    if len(df) == 0:
        raise RuntimeError("There is no data present to merge denoms onto")

    if deliverable.zfill_fips:  # unify fips dtypes and leading zeros
        logger.info("Casting fips_6_4_code to str and adding leading zeros")
        df = zfill_fips_6_4_ids(df=df, deliverable=deliverable)
        denoms = zfill_fips_6_4_ids(df=denoms, deliverable=deliverable)

    denoms["estimate_id"] = df.estimate_id.iloc[0]
    denoms["bundle_id"] = df.bundle_id.iloc[0]
    denoms["is_identifiable"] = df.is_identifiable.iloc[0]

    pre_drop_na = df.shape[0]
    df = df.loc[df["location_id"].notnull()]
    post_drop = df.shape[0]

    if (pre_drop_na - post_drop) > (pre_drop_na * 0.05):
        raise RuntimeError("Dropping null locations lost more than 5% of data")

    merge_cols = denoms.drop("sample_size", axis=1, inplace=False).columns.tolist()
    df = df.merge(denoms, on=merge_cols, validate="m:1", how="outer")

    # 'square' the data based on the denoms. Add zeros where val is null and sample size is not
    cond = "(df.sample_size.notnull()) & (df.val.isnull())"
    df.loc[eval(cond), "val"] = 0

    df = filter_by_code_system(df=df, code_system_ids=code_system_ids, logger=logger)
    # attach nids
    df = attach_cms_nids(df=df, cms_system=cms_system)

    df = col_reqs.remove_locs(df=df, deliverable=deliverable)

    validate_merge_denoms(df=df, deliverable=deliverable)

    return df, denoms


def attach_cms_nids(df: pd.DataFrame, cms_system: str) -> pd.DataFrame:
    """Merge the record level NIDs onto CMS bundle-mart data by year.

    Args:
        df (pd.DataFrame): Table of bundle estimate being processed.
        cms_system (str): Clinical CMS system abbreviation.
            ex: 'mdcr'

    Raises:
        RuntimeError: Null NID present after merge.

    Returns:
        pd.DataFrame: Input table with 'nid' column mapped into it.
    """

    nid_lookup = get_nid(cms_system)
    df = df.merge(nid_lookup, how="left", on="year_id", validate="m:1")

    if df["nid"].isnull().any():
        raise RuntimeError("Something went wrong, there shouldn't be missing NIDs")

    return df


def get_nid(cms_system: str) -> pd.DataFrame:
    """Pull the cms_system -> year -> nid values from constants.

    Args:
        cms_system (str): Clinical CMS system abbreviation.
            ex: 'mdcr'

    Returns:
        pd.DataFrame: Columns are year_id and nid.
    """
    nid_lookup = pd.DataFrame.from_dict(
        constants.nid_year[cms_system], orient="index"
    ).reset_index()
    nid_lookup.columns = ["year_id", "nid"]
    return nid_lookup
