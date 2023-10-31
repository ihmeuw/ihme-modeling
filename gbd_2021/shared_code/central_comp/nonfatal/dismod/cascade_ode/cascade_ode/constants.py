class ConnectionDefinitions:
    COD = 'cod'
    EPI = 'epi'
    GBD = 'gbd'
    VALID_CONN_DEFS = [COD, EPI, GBD]
    READ_ONLY = [COD, GBD]


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
    PROCESS_VERSION_OF_CODCORRECT = (
        """
        SELECT
        val AS codcorrect_version,
        gbd_process_version_id,
        gbd_process_version_status_id,
        gbd_round_id,
        decomp_step_id
        FROM gbd_process_version_metadata
        JOIN
        gbd_process_version USING (gbd_process_version_id)
        JOIN
        metadata_type USING (metadata_type_id)
        WHERE
        metadata_type = 'CodCorrect Version'
        and gbd_process_id = 3
        and gbd_process_version_status_id = 1
        and val = :codcorrect_version
        ORDER BY gbd_process_version_id DESC;
        """
    )
    CSMR_INFO_OF_MODEL = (
        """
        SELECT csmr_cod_output_version_id
        FROM epi.model_version_mr
        WHERE model_version_id = :model_version_id
        """
    )
