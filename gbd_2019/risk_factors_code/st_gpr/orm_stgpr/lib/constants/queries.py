GET_BEST_MODEL: str = (
    """SELECT stgpr_version_id
    FROM stgpr.stgpr_version sv
    WHERE sv.epi_model_version_id IN (
        SELECT model_version_id
        FROM epi.model_version
        WHERE modelable_entity_id = :modelable_entity_id
        AND decomp_step_id = :decomp_step_id
        AND model_version_status_id = 1
    )"""
)
GET_BUNDLE_FROM_CROSSWALK: str = (
    """SELECT DISTINCT bundle_id
        FROM crosswalk_version.crosswalk_version
        JOIN bundle_version.bundle_version USING (bundle_version_id)
        WHERE crosswalk_version_id = :crosswalk_version_id"""
)
GET_COVARIATE_IDS: str = (
    """SELECT covariate_id, covariate_name_short
    FROM shared.covariate
    WHERE last_updated_action != 'DELETE'"""
)
GET_ACTIVE_LOCATION_SET_VERSION: str = (
    """SELECT shared.active_location_set_decomp_step_version(
        :location_set_id, :gbd_round_id, :decomp_step_id
    )"""
)
GET_LINKED_CAUSE: str = (
    """SELECT cause_id
    FROM epi.modelable_entity_cause
    WHERE modelable_entity_id = :modelable_entity_id"""
)
GET_LINKED_REI: str = (
    """SELECT rei_id
    FROM epi.modelable_entity_rei
    WHERE modelable_entity_id = :modelable_entity_id"""
)
GET_LOCATION_SET_ID: str = (
    """SELECT location_set_id
    FROM shared.location_set_version
    WHERE location_set_version_id = :location_set_version_id"""
)
GET_NUM_COVARIATE_ID: str = (
    """SELECT COUNT(*)
    FROM shared.covariate
    WHERE covariate_id = :covariate_id
    AND last_updated_action != 'DELETE'"""
)
GET_NUM_MODELABLE_ENTITY_ID: str = (
    """SELECT COUNT(*)
    FROM epi.modelable_entity
    WHERE modelable_entity_id = :modelable_entity_id
    AND last_updated_action != 'DELETE'"""
)
GET_NUM_PREVIOUS_STEP_MODELS: str = (
    """SELECT COUNT(*)
    FROM stgpr.stgpr_version
    WHERE modelable_entity_id = :modelable_entity_id
    AND decomp_step_id = :decomp_step_id"""
)
