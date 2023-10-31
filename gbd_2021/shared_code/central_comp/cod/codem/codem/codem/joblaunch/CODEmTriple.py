from typing import List

import gbd.constants as gbd
from hybridizer.joblaunch.HybridTask import HybridTask
from jobmon.client.swarm.workflow.bash_task import BashTask

from codem.joblaunch.CODEmTask import CODEmBaseTask, CODEmTask
from codem.joblaunch.run_utils import new_models


class CODEmTriple:
    def __init__(self, global_model_version_id,
                 datarich_model_version_id,
                 db_connection, description,
                 gbd_round_id=gbd.GBD_ROUND_ID,
                 decomp_step_id=None,
                 sleep_time=0) -> None:
        """
        Class that creates a list of tasks for a
        global, data rich, and hybrid model.

        Args:
            global_model_version_id: (int) global model
            datarich_model_version_id: (int) data-rich model
            db_connection: (str) database connection
            description: (str) description of the model versions
            gbd_round_id: (int) gbd round ID
            decomp_step_id: (int) decomp step ID
            sleep_time (int): time to sleep before running tasks
        """
        self.old_global_model_version_id = global_model_version_id
        self.old_datarich_model_version_id = datarich_model_version_id
        self.db_connection = db_connection
        self.description = description
        self.gbd_round_id = gbd_round_id
        self.decomp_step_id = decomp_step_id
        self.user = 'codem'

        self.datarich_model_version_id = None
        self.global_model_version_id = None
        self.datarich_task = None
        self.global_task = None
        self.hybrid_task = None
        self.sleep_time = sleep_time
        self.sleep_task = None

        if self.db_connection == 'ADDRESS':
            self.conn_def = 'codem'
        elif self.db_connection == 'ADDRESS':
            self.conn_def = 'codem-test'
        else:
            raise RuntimeError("Must pass a valid database connection.")

    def task_list(self) -> List[CODEmBaseTask]:
        """
        Creates a task list of a global, data-rich, and hybrid model tasks
        and uploads a new model version for each
        """

        self.datarich_model_version_id = new_models(
            self.old_datarich_model_version_id,
            db_connection=self.db_connection,
            gbd_round_id=self.gbd_round_id,
            decomp_step_id=self.decomp_step_id,
            desc=self.description
        )[0]
        self.global_model_version_id = new_models(
            self.old_global_model_version_id,
            db_connection=self.db_connection,
            gbd_round_id=self.gbd_round_id,
            decomp_step_id=self.decomp_step_id,
            desc=self.description
        )[0]

        if self.sleep_time:
            self.sleep_task = BashTask(
                name=f"sleep_{self.global_model_version_id}_{self.datarich_model_version_id}",
                command=(
                    f"echo sleep_{self.global_model_version_id}_{self.datarich_model_version_id}; "
                    f"sleep {self.sleep_time}"
                ),
                queue="all.q",
                num_cores=1,
                max_runtime_seconds=self.sleep_time + 60,
                m_mem_free="256M"
            )

        self.datarich_task = CODEmTask(
            model_version_id=self.datarich_model_version_id,
            db_connection=self.db_connection,
            upstream_tasks=[self.sleep_task] if self.sleep_task else None
        )
        self.global_task = CODEmTask(
            model_version_id=self.global_model_version_id,
            db_connection=self.db_connection,
            upstream_tasks=[self.sleep_task] if self.sleep_task else None
        )

        self.hybrid_task = HybridTask(
            user=self.user,
            datarich_model_version_id=self.datarich_model_version_id,
            global_model_version_id=self.global_model_version_id,
            conn_def=self.conn_def,
            upstream_tasks=[self.datarich_task, self.global_task]
        )

        if self.sleep_task:
            return [self.datarich_task, self.global_task, self.hybrid_task, self.sleep_task]
        else:
            return [self.datarich_task, self.global_task, self.hybrid_task]
