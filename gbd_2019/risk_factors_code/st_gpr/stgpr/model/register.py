from typing import Optional

import orm_stgpr

from stgpr.common import logging_utils


def register_stgpr_model(
        path_to_config: str,
        model_index_id: Optional[int] = None
) -> int:
    """
    Creates an ST-GPR version associated with a set of input parameters
    specified in a config.

    Args:
        path_to_config: path to config containing input parameters
        model_index_id: optional ID used to select a parameter set if the
            config contains multiple sets of parameters

    Returns:
        Created ST-GPR version ID (AKA run ID)
    """
    logging_utils.configure_logging()

    return orm_stgpr.create_stgpr_version(path_to_config, model_index_id)
