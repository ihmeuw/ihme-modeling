from typing import List

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline.marketscan import (
    DENOMINATOR_MERGE_COLS,
    FULL_CODE_SYSTEM_YEARS,
    PREVALENCE_CAP,
    YEAR_NID_DICT,
)
from crosscutting_functions.clinical_constants.shared.mapping import CODE_SYSTEMS
from gbd.constants import measures


def merge_denominators(df: pd.DataFrame, denominator_df: pd.DataFrame) -> pd.DataFrame:
    """Merge a dataframe with denominators, aka sample size onto a dataframe of aggregated
    claims encounter records and then attach NIDs"""
    denominator_df["bundle_id"] = df["bundle_id"].iloc[0]
    denominator_df["estimate_id"] = df["estimate_id"].iloc[0]

    df = df.merge(denominator_df, how="right", on=DENOMINATOR_MERGE_COLS, validate="1:1")

    df = attach_nids(df)
    return df


def attach_nids(df: pd.DataFrame) -> pd.DataFrame:
    """Attach merged source (CCAE + MDCR) NIDs to the Marketscan data by year"""
    df["nid"] = df["year_id"].map(YEAR_NID_DICT)

    return df


def fill_missing_encounters(df: pd.DataFrame) -> pd.DataFrame:
    """The type of merge that we're performing effectively squares the encounter data,
    adding rows where there is a sample size but not encounter values. Any null vals after
    merging on denominators mean there were zero encounters for that demographic group. This
    makes that explicit."""
    df.loc[df.eval("sample_size.notnull() and val.isnull()", engine="python"), "val"] = 0
    return df


def handle_code_system_year_missingness(
    df: pd.DataFrame, bundle_system_ids: List[int]
) -> pd.DataFrame:
    """Each bundle can map to 1 or more ICG, which maps to 1 or more ICD code. These can be ICD
    9 codes, ICD 10 codes, or most often both. Depending on the properties of the underlying
    disease(bundle) there are some bundles which do not map to an entire ICD version. This will
    remove any years of data that a bundle is not expected to map to.

    The year 2015 is unique in this regard. The U.S. switched code-systems mid-year, on
    Oct 1st, 2015. Codes before this date are ICD 9, codes on or after are ICD 10. Additional
    documentation can be found here-
    "FILEPATH"
    on Pages 18 and 28. A CMS fact sheet on the topic is here-
    https://www.cms.gov/newsroom/fact-sheets/transitioning-icd-10
    """
    # retain only code systems that are present in Marketscan
    bundle_system_ids = [id for id in bundle_system_ids if id in FULL_CODE_SYSTEM_YEARS.keys()]

    bundle_years = []
    for code_system_id in bundle_system_ids:
        bundle_years += FULL_CODE_SYSTEM_YEARS[code_system_id]

    # manually adjust for the year 2015, which must have both ICD 9 and 10 codes for the bundle
    if not set([CODE_SYSTEMS["ICD9"], CODE_SYSTEMS["ICD10"]]) - set(bundle_system_ids):
        bundle_years.append(2015)

    return df[df["year_id"].isin(bundle_years)]


def create_rates(df: pd.DataFrame, measure_id: int) -> pd.DataFrame:
    """Create a column for encounter estimates in rate space. This is simply done by dividing
    the count of encounters in the val column by the sample size column."""
    df["mean"] = df["val"] / df["sample_size"]

    if measure_id == measures.PREVALENCE:
        df.loc[df["mean"] > PREVALENCE_CAP, "mean"] = PREVALENCE_CAP

    return df