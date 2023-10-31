import logging

from codem.joblaunch.CODEmTask import CODEmBaseTask

import hybridizer.metadata as metadata

logger = logging.getLogger(__name__)


class HybridTask(CODEmBaseTask):
    def __init__(self, user, datarich_model_version_id,
                 global_model_version_id,
                 conn_def,
                 upstream_tasks=None,
                 parameter_dict=None,
                 max_attempts=3,
                 cores=1,
                 gb=20,
                 runtime_min=60*2):
        """
        Creates a hybrid task for a CODEm Workflow.
        """
        model_version_id = metadata.hybrid_metadata(user, global_model_version_id,
                                                    datarich_model_version_id, conn_def)
        self.model_version_id = model_version_id
        self.user = user
        self.global_model_version_id = global_model_version_id
        self.datarich_model_version_id = datarich_model_version_id

        logger.info("New Hybrid Model Version ID: {}".format(model_version_id))

        super().__init__(model_version_id=model_version_id,
                         parameter_dict=parameter_dict, max_attempts=max_attempts,
                         upstream_tasks=upstream_tasks,
                         conn_def=conn_def, hybridizer=True,
                         cores=cores,
                         gb=gb, minutes=runtime_min
                         )
        # TODO: make a prediction model for GB and minutes. Right now relying on past model stats.
        command = 'FILEPATH {} {} {} {} {}'. \
            format(user, model_version_id, global_model_version_id, datarich_model_version_id, conn_def)

        self.setup_task(
            command=command,
            resource_scales={'m_mem_free': 0.5,
                             'max_runtime_seconds': 0.5}
        )
