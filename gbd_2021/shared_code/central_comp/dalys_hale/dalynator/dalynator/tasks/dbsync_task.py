import logging

from jobmon.client.swarm.workflow.executable_task import ExecutableTask
from jobmon.models.job_status import JobStatus

from dalynator.tasks.pct_change_task import PercentageChangeTask

logger = logging.getLogger(__name__)


class DBSyncTask(ExecutableTask):

    def __init__(self, process_version_id):
        self.command = "sync_db_metadata -gv {pv} -remote_load".format(
            pv=int(process_version_id))
        super(DBSyncTask, self).__init__(self.command)

    def bind(self, job_list_manager):
        logger.debug("Create job, full command = {}"
                     .format(self.command))

        max_runtime_seconds_mins = 30
        self.job_id = job_list_manager.create_job(
            jobname=self.hash_name,
            job_hash=self.hash,
            command=self.command,
            num_cores=1,
            mem_free='2G',
            max_runtime_seconds=max_runtime_seconds_mins*60,
            max_attempts=3,
        )
        self.status = JobStatus.REGISTERED
        return self.job_id
