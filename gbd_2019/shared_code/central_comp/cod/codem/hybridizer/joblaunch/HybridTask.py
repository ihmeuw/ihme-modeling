from hybridizer.database import gbd_round_from_id
import hybridizer.metadata as metadata
from codem.joblaunch.CODEmTask import CODEmBaseTask

import logging

logger = logging.getLogger(__name__)


class HybridTask(CODEmBaseTask):
    def __init__(self, user, developed_model_version_id,
                 global_model_version_id,
                 conn_def,
                 upstream_tasks=None,
                 parameter_dict=None,
                 max_attempts=15,
                 cores=1,
                 gb=20,
                 runtime_min=60*2):
        """
        Creates a hybrid task for a CODEm Workflow.
        """
        gbd_round_id = gbd_round_from_id(global_model_version_id, conn_def)
        model_version_id = metadata.hybrid_metadata(user, global_model_version_id,
                                                    developed_model_version_id, conn_def,
                                                    gbd_round_id)
        self.gbd_round_id = gbd_round_id
        self.model_version_id = model_version_id
        self.user = user
        self.global_model_version_id = global_model_version_id
        self.developed_model_version_id = developed_model_version_id

        logger.info("New Hybrid Model Version ID: {}".format(model_version_id))

        super().__init__(model_version_id=model_version_id,
                         parameter_dict=parameter_dict, max_attempts=max_attempts,
                         upstream_tasks=upstream_tasks,
                         conn_def=conn_def, hybridizer=True,
                         cores=cores,
                         gb=gb, minutes=runtime_min
                         )
        command = 'FILEPATH {} {} {} {} {}'. \
            format(user, model_version_id, global_model_version_id, developed_model_version_id, conn_def)

        self.setup_task(
            command=command,
            resource_scales={'m_mem_free': 0.5,
                             'max_runtime_seconds': 0.5}
        )
