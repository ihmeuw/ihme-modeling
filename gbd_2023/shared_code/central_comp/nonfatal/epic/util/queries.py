from typing import Final

UNBEST_MODELS: Final[
    str
] = """UPDATE epi.model_version
    SET best_end = CASE
        WHEN best_start IS NULL THEN NULL
        ELSE NOW()
        END,
    model_version_status_id = 2
    WHERE release_id = :release_id
    AND description LIKE "{epic_version}"
    AND (best_user LIKE "svc%%epic" OR best_user IS NULL)
    AND inserted_by = "svc_save_results"
    AND tool_type_id = 18
    """

BEST_MODELS: Final[
    str
] = """UPDATE epi.model_version
    SET best_start = CASE
        WHEN best_start IS NULL THEN NOW()
        ELSE best_start
        END,
    model_version_status_id = 1, best_end = NULL
    WHERE release_id = :release_id
    AND description LIKE "{epic_version}"
    AND (best_user LIKE "svc%%epic" OR best_user IS NULL)
    AND inserted_by = "svc_save_results"
    AND tool_type_id = 18
    """

SOFT_DELETE_MODEL: Final[
    str
] = """UPDATE epi.model_version
    SET model_version_status_id = 3, best_end = NOW()
    WHERE release_id = :release_id
    AND description LIKE "{epic_version}"
    AND (best_user LIKE "svc%%epic" OR best_user IS NULL)
    AND inserted_by = "svc_save_results"
    AND tool_type_id = 18
    """

SELECT_MODEL: Final[
    str
] = """SELECT * FROM epi.model_version
    WHERE release_id = :release_id
    AND description LIKE "{epic_version}"
    AND (best_user LIKE "svc%%epic" OR best_user IS NULL)
    AND inserted_by = "svc_save_results"
    AND tool_type_id = 18
    """

# healthstate_id 639 is
# 'Post-COMO calculation for residuals (YLL/YLD ratio, other methods)'
# and should not be used in initial COMO calculations.
COMO_CONFIG: Final[
    str
] = """SELECT modelable_entity_id
    FROM epic.sequela_hierarchy_history
    WHERE sequela_set_version_id = (
        SELECT sequela_set_version_id
        FROM sequela_set_version_active
        WHERE release_id = :release_id)
    AND most_detailed = 1
    AND healthstate_id != 639
    """

SEV_SPLIT_CONFIG: Final[
    str
] = """SELECT DISTINCT parent_meid, child_meid, split_id, split_version_id
    FROM severity_splits.split_version
    JOIN severity_splits.split_proportion USING (split_version_id)
    WHERE is_best = 1
    AND release_id=:release_id
    """

ME_NAME: Final[
    str
] = """SELECT me.modelable_entity_id, me.modelable_entity_name
    FROM epi.modelable_entity me
    WHERE modelable_entity_id = :meid
    """
