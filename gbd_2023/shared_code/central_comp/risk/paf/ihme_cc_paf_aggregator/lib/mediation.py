import pathlib
from typing import List

import numpy as np
import pandas as pd

from ihme_cc_risk_utils.lib import constants as risk_utils

from ihme_cc_paf_aggregator.lib import constants, logging_utils


def expand_mediation_matrix_subcauses(
    mediation_matrix: pd.DataFrame,
    parent_cause_id: int,
    child_cause_ids: List[int],
    set_mediation_to_one: bool,
) -> pd.DataFrame:
    """In-memory expansion of the mediation matrix.

    A parent cause ID rows are copied into child cause ID rows, optionally
    setting mean mediation to 1.0

    Note: Implemented for CKD. If in future this function gets used
    in non-CKD contexts, take care that it is working as expected.
    """
    parent_rows = mediation_matrix.query(f"cause_id == {parent_cause_id}")
    new_child_df = parent_rows.merge(pd.DataFrame({"cause_id": child_cause_ids}), how="cross")
    new_child_df = new_child_df.drop(columns=["cause_id_x"])
    new_child_df = new_child_df.rename(columns={"cause_id_y": "cause_id"})
    if set_mediation_to_one:
        new_child_df = new_child_df.assign(mean_mediation=1.0)
    amended = pd.concat([mediation_matrix, new_child_df]).reset_index(drop=True)
    if mediation_matrix.empty and amended.empty:
        # special case, so far only in tests, where the index type changes, and cache
        # round-trip validation fails.
        amended.index = mediation_matrix.index
    return amended


def default_mediation_matrix(release_id: int) -> pd.DataFrame:
    """Retrieve the default mediation matrix (parametrized by release)."""
    default_mediation_matrix_path = pathlib.Path(
        risk_utils.MEDIATION_MATRIX.format(release_id=release_id)
    )
    return mediation_matrix_from_path(default_mediation_matrix_path)


def mediation_matrix_from_path(fpath: pathlib.Path) -> pd.DataFrame:
    """Read and validate a mediation matrix csv."""
    logger = logging_utils.module_logger(__name__)
    logger.info(f"Reading mediation input from {fpath.resolve()}")
    df = pd.read_csv(fpath)
    expected_cols = [
        constants.REI_ID,
        constants.MED_ID,
        constants.CAUSE_ID,
        constants.MEAN_MEDIATION,
    ]
    if not all(col in df for col in expected_cols):
        raise RuntimeError(
            f"Mediation matrix file at {fpath} missing some of {expected_cols}"
        )

    return df[expected_cols]


def validate_mediation_matrix(mediation_matrix: pd.DataFrame) -> None:
    """Checks the mediation matrix validity.

    - expected columns
    - multiple mediation pathways can't include a 100% mediation

    """
    # check correct columns
    expected_columns = {
        constants.REI_ID,
        constants.MED_ID,
        constants.CAUSE_ID,
        constants.MEAN_MEDIATION,
    }
    columns = set(mediation_matrix.columns)
    if columns != expected_columns:
        raise ValueError(
            "Expected the mediation matrix to have columns "
            f"{expected_columns} but got {columns}"
        )

    # check for valid mean_mediation values
    for risk_cause, group in mediation_matrix.groupby([constants.REI_ID, constants.CAUSE_ID]):
        if (len(group) > 1) and (group.mean_mediation == 1.0).any():
            raise ValueError(
                f"The risk-cause pair {risk_cause} can not have "
                "multiple mediation pathways when one pathway "
                f"is a mediation of 100%. Got:\n{group}."
            )


def get_risks_with_unmediated_pafs(mediation_matrix: pd.DataFrame) -> List[int]:
    """Finds the list of REI IDs that are expected to have unmediated PAF models
    as defined by the mediation matrix. These are the risks where the mediation
    factor, across all mediators, is less than 100% for one or more causes
    """
    mediation_by_risk_cause = (
        mediation_matrix.groupby([constants.REI_ID, constants.CAUSE_ID])[
            constants.MEAN_MEDIATION
        ]
        .agg([("mf", lambda x: 1 - np.prod(1 - x))])
        .reset_index()
    )
    return (
        mediation_by_risk_cause[mediation_by_risk_cause["mf"] < 1][constants.REI_ID]
        .unique()
        .tolist()
    )
