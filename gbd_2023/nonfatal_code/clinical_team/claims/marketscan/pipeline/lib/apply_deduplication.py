"""Applies the deduplication packages code to the Marketscan data"""
import pandas as pd
from crosscutting_functions.deduplication.dedup import ClinicalDedup


def deduplicate_data(df: pd.DataFrame, estimate_id: int, map_version: int) -> pd.DataFrame:
    """Given a df of Marketscan claims data, an estimate_id and a map_version return
    deduplicated data, depending on processing requirements of the estimate_id and
    measure/duration limits defined by the Clinical map_version.

    Args:
        df (pd.DataFrame): Marketscan claims data for a single bundle. Must have a
                           patient/enrollee/beneficiary identifier column and a date of service
        estimate_id (int): Standard clinical estimate_id. Used to determine deduplication
                           method
        map_version (int): Standard clinical map_version.

    Returns:
        pd.DataFrame: Claims data deduplicated to meet requirements with an encounter column
    """
    dedup_estimate = ClinicalDedup(
        enrollee_col="bene_id",
        service_start_col="service_start",
        year_col="year_id",
        estimate_id=estimate_id,
        map_version=map_version,
    )
    df = dedup_estimate.main(df=df)

    # Data is now in the correct space matching the estimate_id so we can add a val unit to
    # aggregate encounters
    df["val"] = 1

    return df