import pandas as pd

from orm_stgpr.db import session_management
from orm_stgpr.lib import covariates
from orm_stgpr.lib.validation import stgpr_version_validation


def get_custom_covariates(stgpr_version_id: int) -> pd.DataFrame:
    """
    Pulls custom covariate data associated with an ST-GPR version ID.

    Args:
        stgpr_version_id: the ID with which to pull custom covariate data

    Returns:
        Dataframe of custom covariates for an ST-GPR run or None if there are
        no custom covariates for the given ST-GPR version ID

    Raises:
        ValueError: if stgpr_version_id is not in the database
    """
    with session_management.session_scope() as session:
        # Validate input.
        stgpr_version_validation.validate_stgpr_version_exists(
            stgpr_version_id, session
        )

        # Call library function to get parameters.
        return covariates.get_custom_covariates(session, stgpr_version_id)
