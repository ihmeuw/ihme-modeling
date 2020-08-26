class Methods:
    """
    Descriptions of each of these methods are detailed in decomp module
    """
    ENABLE_HYBRID_CV = 'enable_hybrid_cv'
    ENABLE_EMR_CSMR_RE = 'enable_emr_csmr_re'
    DISABLE_NON_STANDARD_LOCATIONS = 'disable_nonstandard_locations'
    FIX_CSMR_RE = 'fix_csmr_re'
    ENABLE_CSMR_AVERAGE = 'enable_csmr_average'
    ENABLE_CSMR_FAUXCORRECT = 'enable_csmr_fauxcorrect'


class Queries:
    GET_CAUSE_OR_REI_ID = (
        """Select rei_id, cause_id, decomp_step_id, gbd_round_id
        from epi.model_version
        left join epi.modelable_entity_cause using (modelable_entity_id)
        left join epi.modelable_entity_rei using (modelable_entity_id)
        where
            model_version_id = :model_version_id
        """
    )
    BEST_MODEL_LAST_STEP = (
        """
        SELECT model_version_id
        FROM epi.model_version
        WHERE modelable_entity_id = (
            SELECT modelable_entity_id
            FROM epi.model_version
                WHERE model_version_id = :model_version_id)
        AND
        decomp_step_id = :decomp_step_id
        AND
        model_version_status_id = 1
        """
    )
    CSMR_INFO_OF_MODEL = (
        """
        SELECT csmr_cod_output_version_id
        FROM epi.model_version_mr
        WHERE model_version_id = :model_version_id
        """
    )
