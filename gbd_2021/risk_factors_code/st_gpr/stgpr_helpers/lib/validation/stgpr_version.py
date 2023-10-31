from sqlalchemy import orm

import db_stgpr

from stgpr_helpers.lib.constants import exceptions as exc


def validate_stgpr_version_exists(stgpr_version_id: int, session: orm.Session) -> None:
    """Validates that an ST-GPR version ID exists and has only one entry.

    Args:
        stgpr_version_id: ID of the ST-GPR version to check.
        session: active session with the epi database.

    Raises:
        ValueError: if ST-GPR version ID is not present in the database.
    """
    if not db_stgpr.stgpr_version_exists(stgpr_version_id, session):
        raise exc.NoStgprVersionFound(f"ST-GPR version {stgpr_version_id} does not exist")


def validate_model_iteration_matches_stgpr_version(
    stgpr_version_id: int, model_iteration_id: int, session: orm.Session
) -> None:
    """Validates that a model iteration ID matches an ST-GPR version ID.

    Args:
        stgpr_version_id: ID of the ST-GPR version to check.
        model_iteration_id: ID of the model iteration to check.
        session: active session with the epi database.
    """
    linked_model_iteration_ids = db_stgpr.get_model_iterations(stgpr_version_id, session)
    if model_iteration_id not in linked_model_iteration_ids:
        raise exc.ModelIterationDoesNotMatchStgprVersion(
            f"Model iteration ID {model_iteration_id} does not match ST-GPR version ID "
            f"{stgpr_version_id}"
        )


def validate_only_one_model_iteration(stgpr_version_id: int, session: orm.Session) -> int:
    """Validates that an ST-GPR version has only one linked model iteration.

    Args:
        stgpr_version_id: ID of the ST-GPR version to validate.
        session: active session with the epi database.

    Returns:
        ID of the linked model iteration.
    """
    linked_model_iteration_ids = db_stgpr.get_model_iterations(stgpr_version_id, session)
    if len(linked_model_iteration_ids) != 1:
        raise ValueError(
            f"Expected one model iteration linked to ST-GPR version {stgpr_version_id}. "
            f"Found {len(linked_model_iteration_ids)} linked model iterations"
        )
    return linked_model_iteration_ids[0]
