from typing import List

import numba
import numpy as np
import pandas as pd


def deduplicate_single_entity(
    individual_df: pd.DataFrame,
    enrollee_col: str,
    service_start_col: str,
    duration_col: str = "bundle_duration",
) -> pd.DataFrame:
    """
    Processes identifiable, individual level incidence data to remove any
    duplicate rows within a defined duration window.
    Assumes data contains a single entity (eg bundle_id, icg_id, cause_id, etc)

    Args:
        individual_df: Contains rows representing some type of individual level encounters
                       or records.
        enrollee_col: The name of the column containing personal identifiers.
        service_start_col: The name of the column containing dates of service. Used in
                        tandem with the duration column to identify windows within
                        which all duplicates are removed.
        duration_col: The name of the column containing the duration length, in days.
                         There must be only a single unique value in this column.

    Returns:
        A DataFrame where all duplicate rows within the duration window were removed.
    """
    duration_values = individual_df[duration_col].unique()
    if duration_values.size != 1:
        raise ValueError(
            "This function assumes there is a single unique duration "
            f"value in the column {duration_col}. We are seeing "
            f"the values {duration_values}."
        )
    duration = individual_df[duration_col].iloc[0]

    # re-map enrollee ids to integers to avoid typing issues
    all_bene_ids = individual_df[enrollee_col].unique()
    bene_id_map = dict(zip(all_bene_ids, range(len(all_bene_ids))))
    individual_df["mapped_enrollee_id"] = individual_df[enrollee_col].map(bene_id_map)

    # run the helper function
    idx = _numba_helper(
        individual_df["mapped_enrollee_id"].values,
        individual_df[service_start_col].values.astype(int),
        duration,
    )

    # drop the mapped integer id before returning
    individual_df.drop("mapped_enrollee_id", axis=1, inplace=True)
    return individual_df.iloc[idx]


@numba.njit
def _numba_helper(ids: np.ndarray, dates: np.ndarray, duration: int) -> List[int]:
    """Loops over arrays of enrollee IDs and service dates to remove duplicate rows
    within a provided bundle-duration window. Note that the date elements are integers which
    are created after converting a Pandas date-time series to integer. These represent
    nanoseconds from 01-01-1970, which is why the results of creating the delta between 2
    service start dates is converted to days using the nanoseconds per day value.
    Also note the decorator marking this as a no-Python numba function.

    Args:
        ids: An array of enrollee IDs corresponding to a row in the input DataFrame.
        dates: An array representing the date of service.
        duration: The window of time, in days, to use to identify duplicate rows.

    Returns:
        The index of all non-duplicated rows.
    """
    bene_id, service_start = ids[0], dates[0]
    final_idx = [0]  # Always include the first row
    nanoseconds_per_day = 86_400_000_000_000
    for row in range(1, len(ids)):
        new_bene_id, new_service_start = ids[row], dates[row]
        test_duration_days = (new_service_start - service_start) // nanoseconds_per_day
        if new_bene_id == bene_id and test_duration_days < duration:
            # We have a duplicate row, so do nothing
            continue
        else:
            final_idx.append(row)
            bene_id, service_start = new_bene_id, new_service_start
    return final_idx
