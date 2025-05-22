"""Functions related to model quotas."""

import logging
import warnings
from typing import Dict, List

from sqlalchemy import orm

import db_stgpr
import stgpr_schema
from db_stgpr import columns

from stgpr_helpers.lib.constants import draws
from stgpr_helpers.lib.constants import exceptions as exc

# Default model quotas: MEs not considered special cases (primarily those that need space for
# more models) are assigned the default quota
_DEFAULT_MODEL_QUOTA: Dict[str, int] = {
    columns.QUOTA_MAX_DRAWS: 15,
    columns.QUOTA_LESS_THAN_MAX_DRAWS: 50,
}

_EXCLUDED_MODEL_STATUSES: List[stgpr_schema.ModelStatus] = [
    stgpr_schema.ModelStatus.best,
    stgpr_schema.ModelStatus.delete,
    stgpr_schema.ModelStatus.soft_shell_delete,
]


def get_model_quota(modelable_entity_id: int, session: orm.Session) -> Dict[str, int]:
    """Returns dictionary with model quota information for the given ME."""
    model_quota = db_stgpr.get_special_case_model_quota(modelable_entity_id, session)
    return model_quota if model_quota else _DEFAULT_MODEL_QUOTA


def get_model_count(
    modelable_entity_id: int, release_id: int, session: orm.Session
) -> Dict[str, int]:
    """Returns dict with number of models for the given ME/release by draw category."""
    # Pull non-deleted ST-GPR models for the ME/release
    model_status_ids = [
        status.value
        for status in stgpr_schema.ModelStatus
        if status not in _EXCLUDED_MODEL_STATUSES
    ]
    fields = [columns.STGPR_VERSION_ID, columns.GPR_DRAWS]
    filters = {
        columns.MODELABLE_ENTITY_ID: modelable_entity_id,
        columns.RELEASE_ID: release_id,
        columns.MODEL_STATUS_ID: model_status_ids,
    }

    # Note: best models are excluded but best model status is pulled from stgpr db, not epi
    stgpr_versions = db_stgpr.get_stgpr_versions(
        session, fields=fields, filters=filters, epi_best=False
    )

    # Count number of models per draw category: max draws or less than max draws
    max_draw_count = 0
    less_than_max_draw_count = 0
    for stgpr_version in stgpr_versions.values():
        if stgpr_version[columns.GPR_DRAWS] == draws.MAX_GPR_DRAWS:
            max_draw_count += 1
        else:
            less_than_max_draw_count += 1

    return {
        columns.QUOTA_MAX_DRAWS: max_draw_count,
        columns.QUOTA_LESS_THAN_MAX_DRAWS: less_than_max_draw_count,
    }


def validate_model_quota_not_met(
    modelable_entity_id: int, gpr_draws: int, release_id: int, session: orm.Session
) -> None:
    """Validates that the model quota for the ME/release/draw category has not been met.

    If quota has not been met, log how many models are left. If the quota has been met
    and model quotas are being enforced, raise an error. If model quotas aren't being
    enforced, raise a warning.

    Raises:
        ModelQuotaMet: if the model quota has been met and quotas are being enforced.
    """
    quota_key = (
        columns.QUOTA_MAX_DRAWS
        if gpr_draws == draws.MAX_GPR_DRAWS
        else columns.QUOTA_LESS_THAN_MAX_DRAWS
    )
    model_quota = get_model_quota(modelable_entity_id, session)[quota_key]
    model_count = get_model_count(modelable_entity_id, release_id, session)[quota_key]

    models_remaining = model_quota - model_count

    # Build message to inform users of quota status
    draw_str = (
        f"max draws ({draws.MAX_GPR_DRAWS})"
        if gpr_draws == draws.MAX_GPR_DRAWS
        else f"less than max draws (<{draws.MAX_GPR_DRAWS})"
    )
    msg = (
        f"{model_count}/{model_quota} of model quota used for modelable "
        f"entity {modelable_entity_id} in release {release_id} with {draw_str}."
    )

    # If quota has been met, error (or warn if quota not yet enforced). If not,
    # let modelers know how much space they have left.
    if models_remaining <= 0:
        settings = stgpr_schema.get_settings()
        if settings.enforce_model_quota:
            msg += (
                " Cannot register more models until space is freed by deleting existing "
                "models."
            )
            raise exc.ModelQuotaMet(msg)
        else:
            msg += (
                " Once model quotas are enforced, you will not be able to register more "
                "models until space is freed by deleting existing models."
            )
            warnings.warn(msg)
    else:
        logging.info(msg)
