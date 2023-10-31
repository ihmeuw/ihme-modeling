import logging

import hybridizer.utilities as utilities
from hybridizer.database import upload_hybrid_metadata

logger = logging.getLogger(__name__)


def hybrid_metadata(user, global_model_version_id, datarich_model_version_id, conn_def):
    logger.info("Creating hybrid metadata.")
    (cause_id, sex_id, age_start, age_end, refresh_id, envelope_proc_version_id,
     population_proc_version_id, decomp_step_id, gbd_round_id) = utilities.get_params(
        global_model_version_id=global_model_version_id,
        datarich_model_version_id=datarich_model_version_id,
        conn_def=conn_def,
        user=user
    )
    model_version_id = upload_hybrid_metadata(
        global_model_version_id=global_model_version_id,
        datarich_model_version_id=datarich_model_version_id,
        cause_id=cause_id,
        sex_id=sex_id,
        age_start=age_start,
        age_end=age_end,
        refresh_id=refresh_id,
        envelope_proc_version_id=envelope_proc_version_id,
        population_proc_version_id=population_proc_version_id,
        decomp_step_id=decomp_step_id,
        conn_def=conn_def,
        user=user,
        gbd_round_id=gbd_round_id)

    return model_version_id
