from typing import Optional

from orm_stgpr.db import session_management
from orm_stgpr.lib import model_version
from orm_stgpr.lib.validation import stgpr_version_validation


def create_model_version(
        stgpr_version_id: int,
        decomp_step: str,
        gbd_round_id: int,
        modelable_entity_id: Optional[int] = None,
        covariate_id: Optional[int] = None,
        epi_model_version_id: Optional[int] = None,
        covariate_model_version_id: Optional[int] = None
) -> int:
    """
    Creates a new ST-GPR model version and adds it to the database.

    Args:
        stgpr_version_id: the ST-GPR version ID to link to this model version
        decomp_step: the decomp step associated with this ST-GPR
        gbd_round_id: the ID of the GBD round associated with this ST-GPR model
        modelable_entity_id: the ME ID associated with this ST-GPR model
        covariate_id: the covariate ID associated with this ST-GPR model
        epi_model_version_id: the epi model version ID associated with this
            ST-GPR model
        covariate_model_version_id: the covariate model version ID associated
            with this ST-GPR model

    Returns:
        Created model version ID
    """
    with session_management.session_scope() as session:
        # Validate input.
        stgpr_version_validation.validate_stgpr_version_exists(
            stgpr_version_id, session
        )

        # Call library function to create model version.
        return model_version.create_model_version(
            session,
            stgpr_version_id,
            decomp_step,
            gbd_round_id,
            modelable_entity_id,
            covariate_id,
            epi_model_version_id,
            covariate_model_version_id
        )
