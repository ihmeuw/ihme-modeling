import json
from typing import Any, Dict, List

import pandas as pd

from stgpr_helpers import columns, parameters

from stgpr.lib import utils


def expand_results(data: pd.DataFrame, params: Dict[str, Any]) -> pd.DataFrame:
    """Expands a set of results for an STGPR step.

    Arguments:
        data: Dataframe containing estimates or draws which contains demographics that will
            be expanded
        params: parameters for a specific STGPR version used to determine what value column(s)
            will be copied

    Checks if params includes specifying age or sex expansion with
    generate_expansion_dimensions(), if not, returns data without transform. If so, expands
    value cols in data to expansion dimensions and drops the expanded age/sex aggregate ids
    from dataframe.
    """
    expansion_dimensions = generate_expansion_dimensions(params=params)
    if not expansion_dimensions:
        return data

    data_dimensions = {}
    value_cols = utils.get_value_cols(data=data, params=params)
    for col in columns.DEMOGRAPHICS:
        data_dimensions.update({col: list(data[col].unique())})

    # Update with expansion dimensions and create multiindex.
    # Any keys already in the dictionary will be replaced.
    data_dimensions.update(expansion_dimensions)
    demographic_index = pd.MultiIndex.from_product(
        list(data_dimensions.values()), names=list(data_dimensions.keys())
    )
    index_df = pd.DataFrame(index=demographic_index).reset_index()

    # Infer keys we can merge on to replicate requested dimensions
    merge_on = list(set(data_dimensions.keys()) - set(expansion_dimensions.keys()))
    expanded = pd.merge(index_df, data[merge_on + value_cols], how="left", on=merge_on)

    # Expanded results should have dimensions equal to product of index
    if expanded.shape[0] != (
        len(data_dimensions[columns.LOCATION_ID])
        * len(data_dimensions[columns.AGE_GROUP_ID])
        * len(data_dimensions[columns.YEAR_ID])
        * len(data_dimensions[columns.SEX_ID])
    ):
        raise RuntimeError(
            "Something went wrong expanding data from a sex and/or age aggregate, expanded "
            "data has the wrong shape."
        )

    return expanded


def generate_expansion_dimensions(params: Dict[str, Any]) -> Dict[str, List[int]]:
    """Generates an expansion dimensions dictionary based on passed STGPR version params.

    Checks if age/sex expand parameters are NULL, if not fill expansion dimensions with
    the IDs in the expand parameters' values. Age/sex expand parameters are stored as strings
    in the database deserialized from a json blob to a dictionary.
    """
    expansion_dimensions = {}
    if params[parameters.AGE_EXPAND]:
        expansion_dict = json.loads(params[parameters.AGE_EXPAND])
        age_group_id = str(params[parameters.PREDICTION_AGE_GROUP_IDS][0])
        expansion_dimensions[columns.AGE_GROUP_ID] = expansion_dict[age_group_id]

    if params[parameters.SEX_EXPAND]:
        expansion_dict = json.loads(params[parameters.SEX_EXPAND])
        sex_id = str(params[parameters.PREDICTION_SEX_IDS][0])
        expansion_dimensions[columns.SEX_ID] = expansion_dict[sex_id]
    return expansion_dimensions
