try:
    from typing import Final
except ImportError:
    from typing_extensions import Final


SOFT_DELETE_MODEL: Final[str] = (
    """UPDATE epi.model_version
    SET model_version_status_id = 3
    AND best_end = NOW()
    WHERE gbd_round_id = :gbd_round_id
    AND description LIKE "\%\EPICv:version_id"
    AND (best_user = "svcepic" OR best_user = NULL)
    AND inserted_by = "epi_custom_model"
    AND tool_type_id = 18
    """
)

SELECT_MODEL: Final[str] = (
    """SELECT * FROM epi.model_version
    WHERE gbd_round_id = :gbd_round_id
    AND description LIKE "\%\EPICv:version_id"
    AND (best_user = "svcepic" OR best_user = NULL)
    AND inserted_by = "epi_custom_model"
    AND tool_type_id = 18
    """
)

COMO_CONFIG: Final[str] = (
    """SELECT modelable_entity_id
    FROM epic.sequela_hierarchy_history
    WHERE sequela_set_version_id = (
        SELECT sequela_set_version_id
        FROM sequela_set_version_active
        WHERE gbd_round_id = :gbd_round_id
        AND decomp_step_id = :decomp_step_id)
    AND most_detailed = 1
    AND healthstate_id != 639
    """
)

SEV_SPLIT_CONFIG: Final[str] = (
    """SELECT DISTINCT parent_meid, child_meid, split_id
    FROM severity_splits.split_version
    JOIN severity_splits.split_proportion USING (split_version_id)
    WHERE is_best = 1
    AND gbd_round_id=:gbd_round_id
    """
)

ME_NAME: Final[str] = (
    """SELECT me.modelable_entity_id, me.modelable_entity_name
    FROM epi.modelable_entity me
    WHERE modelable_entity_id = :meid
    """
)