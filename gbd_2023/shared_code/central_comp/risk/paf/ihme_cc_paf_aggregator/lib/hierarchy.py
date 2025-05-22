import pathlib
from typing import List, Optional

import pandas as pd

import db_queries

from ihme_cc_paf_aggregator.lib import constants, logging_utils


def subset_risk_outcome_pairs(
    df: pd.DataFrame, hierarchy: pd.DataFrame, all_paf_causes: List[int]
) -> pd.DataFrame:
    """Subset data to only risk/cause pairs specified in a given hierarchy, and
    only expected PAF causes.

    The hierarchy can either be a risk hierarchy or a risk/cause hierarchy.

    If cause_id is missing from the hierarchy, then we assume no risk/cause
    subsetting will occur because we were given a risk hierarchy instead of
    a risk/cause hierarchy, and risks have already been filtered to a given
    hierarchy during the gathering of best input models.

    In addition to cause filtering due to a risk/cause hierarchy being provided,
    input pafs can contain causes not in our computation cause hierarchy, or
    aggregate cause models. These causes should be removed, except for those
    specified in constants.AGGREGATE_OUTCOMES, even if no other cause subsetting
    is desired. Downstream consumers (Burdenator) will perform cause aggregation
    so they cannot receive most aggregate causes.

    Arguments:

        df: data to subset.
        hierarchy: hierarchy that specified risk/cause pairs to keep (or just risks)
        all_paf_causes: used to ensure no invalid causes are propagated

    Returns:
        dataframe of subsetted results
    """
    if constants.CAUSE_ID in hierarchy:
        index_cols = [constants.REI_ID, constants.CAUSE_ID]
        df = df.set_index(index_cols)
        hierarchy = hierarchy.set_index(index_cols)
        df = df[df.index.isin(hierarchy.index)].reset_index()

    # also filter out invalid causes
    df = df[df[constants.CAUSE_ID].isin(all_paf_causes)]

    # only allow All-cause to be an outcome for Low educational attainment
    df = df[
        (df[constants.CAUSE_ID] != constants.ALL_CAUSE_ID)
        | (df[constants.REI_ID] == constants.LOW_EDUCATIONAL_ATTAINMENT_REI_ID)
    ]
    return df


def hierarchy_from_db(
    rei_set_id: int, release_id: int, rei_set_version_id: Optional[int] = None
) -> pd.DataFrame:
    """Use get_rei_metadata to return a risk hierarchy."""
    df = db_queries.get_rei_metadata(
        rei_set_id=rei_set_id, rei_set_version_id=rei_set_version_id, release_id=release_id
    )
    df = df[[constants.REI_ID, constants.PARENT_ID]]
    return df


def hierarchy_from_path(fpath: pathlib.Path) -> pd.DataFrame:
    """Read and validate a hierarchy filepath."""
    df = pd.read_csv(fpath)
    required_cols = [constants.REI_ID, constants.PARENT_ID]
    if not all(col in df for col in required_cols):
        raise RuntimeError(f"Hierarchy file at {fpath} missing some of {required_cols}")

    if constants.CAUSE_ID in df:
        keep_cols = required_cols + [constants.CAUSE_ID]
        if df[constants.CAUSE_ID].dtype not in [int, float]:
            all_types = {type(i) for i in df[constants.CAUSE_ID]}
            raise RuntimeError(
                f"File {fpath} has mixed type in {constants.CAUSE_ID} column: "
                f"{all_types}. Maybe you have a stray space character for a null value."
            )
    else:
        logger = logging_utils.module_logger(__name__)
        logger.info(
            "No cause_id column provided in hierarchy -- no risk/cause "
            "subsetting will occur"
        )
        keep_cols = required_cols

    return df[keep_cols]
