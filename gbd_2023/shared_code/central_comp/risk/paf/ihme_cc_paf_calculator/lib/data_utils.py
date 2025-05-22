from typing import List, Tuple

import numpy as np
import pandas as pd

from ihme_cc_paf_calculator.lib import constants, logging_utils

logger = logging_utils.module_logger(__name__)


def convert_to_measure_id(df: pd.DataFrame) -> pd.DataFrame:
    """Convert from separate morbidity/mortality columns to a single measure_id column.
    Assumes that data has already been expanded so that morbidity and mortality indicators
    are on separate rows.

    If df has already been converted to measure_id, this is a no-op.
    """
    if "measure_id" not in df.columns:
        morb_mort_same_row = df[df[constants.MORBIDITY] == df[constants.MORTALITY]]
        if not morb_mort_same_row.empty:
            raise ValueError(
                "To convert to measure IDs, exactly one of morbidity or mortality must be "
                "equal to 1."
            )

        df[constants.MEASURE_ID] = np.where(df[constants.MORTALITY], 4, 3)

    return df.drop(columns=[constants.MORBIDITY, constants.MORTALITY], errors="ignore")


def enforce_paf_in_range(df: pd.DataFrame, draw_columns: List[str]) -> pd.DataFrame:
    """Enforce that all PAF draws are non-null, finite, and <= 1 by setting any that aren't to
    the mean of those that are. In the special case where all PAF draws in a given row are > 1
    and finite, they will all be set to 1.

    Returns an updated DataFrame where invalid PAF draws have been replaced with valid PAF
    draws as described above. Raises a ValueError if any row has no valid PAF draws, excepting
    the case where all PAF draws are > 1 and finite.
    """
    index_columns = [col for col in df.columns if col not in draw_columns]

    # Ensure a unique index.
    df = df.reset_index(drop=True)

    # Special case: if all PAF draws in a row are > 1 and finite, set them all to 1.
    rows_all_above_1 = (df[draw_columns].gt(1) & df[draw_columns].lt(np.inf)).all(axis=1)
    if rows_all_above_1.any():
        logger.warning(
            "Some rows have all PAF draws > 1 and finite. Setting these PAF draws to 1. "
            "Example rows:\n"
            f"{df.loc[rows_all_above_1, index_columns].head().to_string(index=False)}"
        )
        df.loc[rows_all_above_1, draw_columns] = 1

    rows_with_nulls = df.loc[df[draw_columns].isnull().any(axis=1)]
    if not rows_with_nulls.empty:
        logger.warning(
            "Some PAF draws are NaN. Example rows:\n"
            f"{rows_with_nulls[index_columns].head().to_string(index=False)}"
        )

    rows_with_inf = df.loc[df[draw_columns].isin([np.inf, -np.inf]).any(axis=1)]
    if not rows_with_inf.empty:
        logger.warning(
            "Some PAF draws are infinite. Example rows:\n"
            f"{rows_with_inf[index_columns].head().to_string(index=False)}"
        )
        df[draw_columns] = df[draw_columns].where(~df[draw_columns].isin([np.inf, -np.inf]))

    # PAF draws are bounded (-Inf, 1]. We disallow anything above 1 but draws below -1 are ok.
    rows_above_1 = df.loc[df[draw_columns].gt(1).any(axis=1)]
    if not rows_above_1.empty:
        logger.warning(
            "Some PAF draws are > 1. Example rows:\n"
            f"{rows_above_1[index_columns].head().to_string(index=False)}"
        )
        df[draw_columns] = df[draw_columns].where(~df[draw_columns].gt(1))

    # Check that no row has all null PAF draws.
    rows_all_null = df.loc[df[draw_columns].isnull().all(axis=1)]
    if not rows_all_null.empty:
        raise ValueError(
            "Some rows have no valid PAF draws. Example rows:\n"
            f"{rows_all_null[index_columns].head().to_string(index=False)}"
        )

    # Compute the mean PAF draw by row, explicitly skipping any nulls.
    mean_pafs = df[draw_columns].mean(axis=1, skipna=True)

    # Replace nulls with means.
    df[draw_columns] = df[draw_columns].where(~df[draw_columns].isnull(), mean_pafs, axis=0)

    return df


def get_index_draw_columns(df: pd.DataFrame) -> Tuple[List[str], List[str]]:
    """
    Return a list of index columns and draw columns in this dataframe.

    Any column beginning with 'draw_' is assumed to be a non-index column,
    everything else is an index. Draw columns are returned in numerical order.

    Args:
      df (dataframe): The dataframe on which to extract the index column names

    Returns:
      index_columns, draw_columns: The column names split into index and draw type.

    """
    draw_columns = [col for col in df.columns if col.startswith("draw_")]
    draw_columns.sort(key=lambda X: int(X.rsplit("_", 1)[1]))
    index_columns = [X for X in df.columns if X not in draw_columns]
    return index_columns, draw_columns
