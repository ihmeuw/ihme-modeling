"""Helpers for working with square data."""
import itertools
from typing import List

import pandas as pd

from stgpr_helpers.lib.constants import columns


def get_square(
    years: List[int], locations: List[int], sexes: List[int], ages: List[int]
) -> pd.DataFrame:
    """Makes the outline of the square using a cartesian product of demographics.

    Data is square iff it has data for every combination of location, year, age, and sex.

    Args:
        locations: list of prediction location IDs.
        years: list of prediction year IDs.
        ages: list of prediction age group IDs.
        sexes: list of prediction sex IDs.

    Returns:
        Square dataframe of year_id, location_id, sex_id, age_group_id.
    """
    square = pd.DataFrame(
        itertools.product(years, locations, sexes, ages),
        columns=columns.DEMOGRAPHICS,
        dtype=int,
    )
    return square
