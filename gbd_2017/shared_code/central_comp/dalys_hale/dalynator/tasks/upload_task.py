import logging

from jobmon.workflow.executable_task import ExecutableTask
from jobmon.models import JobStatus

from dalynator.tasks.pct_change_task import PercentageChangeTask

logger = logging.getLogger(__name__)


class UploadTask(ExecutableTask):
    def __init__(self, out_dir, tool_name, gbd_process_version_id,
                 location_ids, start_years, end_years, measure_id, table_type,
                 storage_engine, upload_to_test, verbose, write_out_star_ids,
                 upstream_tasks):
        self.command = UploadTask.create_unique_base_command(
            gbd_process_version_id, measure_id, table_type, storage_engine)
        super(UploadTask, self).__init__(self.command, upstream_tasks=[])
        self.out_dir = out_dir

        if upstream_tasks:
            for location_id in location_ids:
                for start_year, end_year in zip(start_years, end_years):
                    self.add_upstream(upstream_tasks[
                        PercentageChangeTask.create_unique_base_command(
                            location_id, measure_id, start_year,
                            end_year, write_out_star_ids)])

        params = (' --out_dir {o} --location_ids {loc} --tool_name '
                  '{tn}'.format(
                      o=out_dir, tn=tool_name,
                      loc=' '.join(str(loc) for loc in location_ids)))
        if verbose:
            params += " --verbose"
        if upload_to_test:
            params += " --upload_to_test"
        self.extended_command = '{c} {p}'.format(c=self.command, p=params)

    def bind(self, job_list_manager):
        logger.debug("Create job, full command = {}"
                     .format(self.extended_command))

        process_timeout = 60 * 60 * 30
        self.job_id = job_list_manager.create_job(
            jobname=self.hash_name,
            job_hash=self.hash,
            command=self.extended_command,
            slots=20,
            mem_free=40,
            max_runtime=process_timeout + max(2, .1 * process_timeout),
            max_attempts=3
        )
        self.status = JobStatus.REGISTERED
        return self.job_id

    @staticmethod
    def create_unique_base_command(gbd_process_version_id, measure_id,
                                   table_type, storage_engine):
        """This command builds the minimum-sized unique command that identifies
        a job. This function is required bcause downstream tasks need to be
        able find their upstream's jobs, without passing all the extended
        arguments"""
        return ("run_upload "
                "--gbd_process_version_id {pv} --measure_id {m} --table_type "
                "{tt} --storage_engine {se}".format(
                    pv=int(gbd_process_version_id), m=int(measure_id),
                    tt=table_type, se=storage_engine))
