import os
from typing import Optional

from orm_stgpr.db import session_management
from orm_stgpr.lib import stgpr_version


def create_stgpr_version(
        path_to_config: str,
        model_index_id: Optional[int] = None,
        output_path: Optional[str] = None
) -> int:
    """
    Creates a new ST-GPR version (and model version) and adds it to
    the database.

    Args:
        path_to_config: path to config CSV containing model parameters
        model_index_id: index of config parameters to use, if config contains
            multiple sets of model parameters
        output_path: where to save files

    Returns:
        Created ST-GPR version ID

    Raises:
        ValueError: for config validation errors
    """
    # Validate input.
    config_exists = os.path.isfile(path_to_config)
    config_is_csv = path_to_config.lower().endswith('.csv')
    if not config_exists or not config_is_csv:
        raise ValueError(
            f'{path_to_config} is not a valid path to a config CSV'
        )

    with session_management.session_scope() as session:
        # Call library function to create ST-GPR version.
        return stgpr_version.create_stgpr_version(
            session, path_to_config, model_index_id, output_path
        )
