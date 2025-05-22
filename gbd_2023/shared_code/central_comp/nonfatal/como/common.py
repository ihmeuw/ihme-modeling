from typing import Dict, List, Optional, Union

import MySQLdb
import pandas as pd
from loguru import logger

from como.legacy import common as legacy_common
from como.lib.types import InputCollectionResult

_DERIVATION_FILTERS = ["modelable_entity_id", "population_group_id"]


def get_cached_population(
    pop_hdf_path: str,
    location_id: Optional[List[int]] = None,
    year_id: Optional[List[int]] = None,
    age_group_id: Optional[List[int]] = None,
    sex_id: Optional[List[int]] = None,
) -> pd.DataFrame:
    """Utility for reading cached population file with demographic filters."""
    pop_df = pd.read_hdf(pop_hdf_path)
    for key, value in zip(
        ["location_id", "year_id", "age_group_id", "sex_id"],
        [location_id, year_id, age_group_id, sex_id],
    ):
        if value:
            pop_df = pop_df.query(f"{key} in {value}")
    return pop_df


def compile_multiprocessed_results(
    result_list: List[Union[pd.DataFrame, legacy_common.ExceptionWrapper]],
) -> List[pd.DataFrame]:
    """Collects multiprocessed results and removes non-DFs."""
    output_dataframes = []
    for result in result_list:
        if isinstance(result, legacy_common.ExceptionWrapper):
            if isinstance(result.ee, MySQLdb._exceptions.OperationalError):
                result.re_raise()
        elif isinstance(result, pd.DataFrame):
            output_dataframes.append(result)
        else:
            raise ValueError(f"Unexpected multiprocessing result: {result}")
    return output_dataframes


def compile_collection_results(
    result_list: List[InputCollectionResult],
) -> List[pd.DataFrame]:
    """Collects multiprocessed results and removes non-DFs and empty DFs."""
    output_dataframes = []
    missed_models = []
    collected_models = []
    for result in result_list:
        if result.df is None:
            missed_models.append((result.modelable_entity_id, result.model_version_id))
        else:
            collected_models.append((result.modelable_entity_id, result.model_version_id))
            output_dataframes.append(result.df)
    logger.info(
        f"Collected {len(collected_models)} (modelable_entity_id, model_version_id): "
        f"{collected_models}"
    )
    logger.info(
        f"Did not collect (empty get_draws return) {len(missed_models)} (modelable_entity_id,"
        f" model_version_id): {missed_models}"
    )
    return output_dataframes


def remove_derivation_filters(filters: Dict[str, List[int]]) -> Dict[str, List[int]]:
    """Removes derivation filters from internal COMO draw source filters."""
    return {key: value for key, value in filters.items() if key not in _DERIVATION_FILTERS}
