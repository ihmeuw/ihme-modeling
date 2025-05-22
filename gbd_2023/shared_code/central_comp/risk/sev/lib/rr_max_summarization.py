from typing import List

import numpy as np
import pandas as pd

from gbd import constants as gbd_constants
from ihme_dimensions import dimensionality, gbdize

RRMAX_COLS = [
    "measure_id",
    "rei_id",
    "cause_id",
    "age_group_id",
    "sex_id",
    "metric_id",
    "val",
    "lower",
    "upper",
]


def format_summaries(summaries: pd.DataFrame) -> pd.DataFrame:
    """Format summaries for upload/handoff to FHS.

    Namely:
        * Add columns for measure and metric IDs
        * Subset to only columns necessary
        * Set all ID columns to ints
    """
    summaries = summaries.copy()

    summaries["measure_id"] = gbd_constants.measures.RR_MAX
    summaries["metric_id"] = gbd_constants.metrics.RATE
    summaries = summaries[RRMAX_COLS]

    id_cols = [c for c in summaries if c.endswith("_id")]
    summaries[id_cols] = summaries[id_cols].astype(int)

    return summaries


def make_square(
    summaries: pd.DataFrame, age_group_ids: List[int], sex_ids: List[int]
) -> pd.DataFrame:
    """Make summaries square by age group and sex, adding NAs for missing combinations.

    Args:
        summaries: dataframe with RRmax summaries
        age_group_ids: age group IDs used in the run, expected to include all most detailed
            age groups
        sex_ids: sex IDs used in the run, expected to include all most detailed sexes
    """
    to_fill_cols = ["age_group_id", "sex_id"]
    id_cols = [col for col in summaries if col.endswith("_id")]
    index_cols = [col for col in id_cols if col not in to_fill_cols]
    data_cols = [col for col in summaries if col not in id_cols]

    # Set up index_dict and data_dict to expand summaries
    index_dict = {
        tuple(index_cols): list(set(tuple(x) for x in summaries[index_cols].values))
    }
    index_dict.update({"age_group_id": age_group_ids, "sex_id": sex_ids})
    data_dict = {"data_cols": data_cols}

    dimensions = dimensionality.DataFrameDimensions(
        index_dict=index_dict, data_dict=data_dict
    )
    gbdizer = gbdize.GBDizeDataFrame(dimensions)
    return gbdizer.fill_empty_indices(summaries, np.nan)
