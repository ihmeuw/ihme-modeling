import pandas as pd

import db_queries

from ihme_cc_paf_aggregator.lib import constants

UNSAFE_SEX_REI_ID = 170
SYPHILIS_CAUSE_ID = 394


def unsafe_sex_syphilis_age_filter(
    df: pd.DataFrame, age_metadata: pd.DataFrame
) -> pd.DataFrame:
    """Syphilis due to unsafe sex should be limited to ages >= 10.

    Notes:

    - In the original PAF Compiler, this was achieved by limiting any entry with cause
      syphilis. Implementing here specifically on the risk-cause pair, in case there is
      a different future risk for syphilis.
    - We can't limit just by the risk, since unsafe-sex can lead to genital herpes, which
      is transmissble at birth and thus can be prevalent and attributable in early age groups.
    """
    is_risk = df[constants.REI_ID] == UNSAFE_SEX_REI_ID
    is_cause = df[constants.CAUSE_ID] == SYPHILIS_CAUSE_ID
    age_under_ten = age_metadata.query("age_group_years_start < 10").age_group_id.unique()
    is_under_ten = df["age_group_id"].isin(age_under_ten)
    df = df[~(is_risk & is_cause & is_under_ten)]
    return df


def apply_special_restrictions(df: pd.DataFrame, age_metadata: pd.DataFrame) -> pd.DataFrame:
    """
    Apply restrictions not defined in the database.

    Arguments:
        df: data to restrict
        age_metadata: as returned by db_queries.get_age_metadata() or a cached copy

    Returns:
        data possibly subsetted to fewer demographics
    """
    df = unsafe_sex_syphilis_age_filter(df, age_metadata)
    return df


def apply_restrictions_from_db(df: pd.DataFrame, release_id: int) -> pd.DataFrame:
    """
    Apply cause level age/sex/measure restrictions to input data.

    Arguments:
        df: data to restrict
        release_id: release to get restriction mapping for

    Returns:
        data possibly subsetted to fewer demographics
    """
    for restriction_type in ["age", "measure", "sex"]:
        df = _restrict_by_type(df, release_id, restriction_type)
    return df


def _restrict_by_type(
    df: pd.DataFrame, release_id: int, restriction_type: str
) -> pd.DataFrame:
    """Drop any rows that are specified by get_restrictions."""
    orig_columns = df.columns.tolist()
    restrictions = db_queries.get_restrictions(
        restriction_type=restriction_type,
        age_group_id=df.age_group_id.unique().tolist(),
        cause_id=df.cause_id.unique().tolist(),
        sex_id=df.sex_id.unique().tolist(),
        measure_id=constants.MEASURE_IDS,
        release_id=release_id,
        cause_set_id=constants.COMPUTATION_CAUSE_SET_ID,
    ).assign(drop=1)
    if restriction_type == "age":
        restrictions = restrictions[restrictions.is_applicable == 1]
    df = df.merge(restrictions, how="left")
    df = df[df["drop"].isna()]
    return df[orig_columns]
