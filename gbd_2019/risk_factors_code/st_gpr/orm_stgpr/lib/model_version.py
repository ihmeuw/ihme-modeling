from typing import Optional

from sqlalchemy import orm

import gbd

from orm_stgpr.db import models


def create_model_version(
        session: orm.Session,
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
        session: a session with the epi database
        stgpr_version_id: the ST-GPR version ID to link to this model version
        decomp_step: the decomp step associated with this ST-GPR model
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
    model_version = models.ModelVersion(
        stgpr_version_id=stgpr_version_id,
        modelable_entity_id=int(
            modelable_entity_id) if modelable_entity_id else None,
        covariate_id=int(covariate_id) if covariate_id else None,
        decomp_step_id=gbd.decomp_step.decomp_step_id_from_decomp_step(
            decomp_step, gbd_round_id),
        gbd_round_id=gbd_round_id,
        epi_model_version_id=epi_model_version_id,
        covariate_model_version_id=covariate_model_version_id
    )
    session.add(model_version)
    session.flush()
    return model_version.surrogate_id
