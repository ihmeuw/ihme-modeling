import getpass
from typing import Any, Dict, List, Optional

from gbd import conn_defs
from hybridizer.joblaunch.HybridTask import HybridTask
from jobmon.client.task import Task

from codem.joblaunch.CODEmDiagnosticsTask import CODEmDiagnosticsTask
from codem.joblaunch.CODEmSleepTask import CODEmSleepTask
from codem.joblaunch.CODEmTask import CODEmTask
from codem.joblaunch.run_utils import new_models


class CODEmTriple:
    """Class for running a CODEm hybrid and feeder models via jobmon."""

    def __init__(
        self,
        global_model_version_id: int,
        datarich_model_version_id: int,
        conn_def: str,
        description: str,
        release_id: Optional[int] = None,
        sleep_time: int = 0,
        diagnostics_model_version_id: Optional[int] = None,
        add_covs: Optional[Dict[int, Any]] = None,
    ) -> None:
        """
        Class that creates a list of tasks for a
        global, data rich, and hybrid model.

        Args:
            global_model_version_id: (int) global model
            datarich_model_version_id: (int) data-rich model
            conn_def: database connection definition
            description: description of the model versions
            release_id: GBD release ID
            sleep_time : time to sleep before running tasks
            diagnostics_model_version_id: a hybrid model version ID to compare against using a
                CODEmDiagnosticsTask
            add_covs: Optional dictionary of covariates with settings to add
        """
        self.old_global_model_version_id = global_model_version_id
        self.old_datarich_model_version_id = datarich_model_version_id
        if conn_def not in [conn_defs.CODEM, conn_defs.CODEM_TEST]:
            raise RuntimeError("Must pass a valid connection definition!")
        self.conn_def = conn_def
        self.description = description
        self.release_id = release_id

        self.datarich_model_version_id = None
        self.global_model_version_id = None
        self.datarich_task = None
        self.global_task = None
        self.hybrid_task = None
        self.sleep_time = sleep_time
        self.sleep_task = None
        self.diagnostics_model_version_id = diagnostics_model_version_id
        self.add_covs = add_covs

    def task_list(self) -> List[Task]:
        """
        Creates a task list of a global, data-rich, and hybrid model tasks
        and uploads a new model version for each
        """
        self.datarich_model_version_id = new_models(
            self.old_datarich_model_version_id,
            conn_def=self.conn_def,
            release_id=self.release_id,
            desc=self.description,
            add_covs=self.add_covs,
        )[0]
        self.global_model_version_id = new_models(
            self.old_global_model_version_id,
            conn_def=self.conn_def,
            release_id=self.release_id,
            desc=self.description,
            add_covs=self.add_covs,
        )[0]

        if self.sleep_time:
            self.sleep_task = CODEmSleepTask(sleep_time=self.sleep_time).get_task()

        self.datarich_task = CODEmTask(
            model_version_id=self.datarich_model_version_id,
            conn_def=self.conn_def,
            upstream_tasks=[self.sleep_task] if self.sleep_task else None,
        ).get_task()
        self.global_task = CODEmTask(
            model_version_id=self.global_model_version_id,
            conn_def=self.conn_def,
            upstream_tasks=[self.sleep_task] if self.sleep_task else None,
        ).get_task()

        self.hybrid_task = HybridTask(
            user=getpass.getuser(),
            datarich_model_version_id=self.datarich_model_version_id,
            global_model_version_id=self.global_model_version_id,
            conn_def=self.conn_def,
            # pass the jobmon task associated with each CODEmTask
            upstream_tasks=[self.datarich_task, self.global_task],
        )
        # save hybrid model version ID in case diagnostics_model_version_id was passed
        hybrid_model_version_id = self.hybrid_task.model_version_id
        self.hybrid_task = self.hybrid_task.get_task()
        tasks = [self.datarich_task, self.global_task, self.hybrid_task]

        if self.sleep_task:
            tasks += [self.sleep_task]
        if self.diagnostics_model_version_id:
            diagnostics_task = CODEmDiagnosticsTask(
                hybrid_model_version_id,
                self.diagnostics_model_version_id,
                cluster_account="proj_codem",
                overwrite=1,
                upstream_tasks=[self.hybrid_task],
            ).get_task()
            tasks += [diagnostics_task]
        return tasks
