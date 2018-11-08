import logging

from jobmon.models import JobStatus
from jobmon.workflow.executable_task import ExecutableTask

logger = logging.getLogger(__name__)


class DalynatorMostDetailedTask(ExecutableTask):
    def __init__(self, input_data_root, out_dir, location_id,
                 year_id, cod_version, epi_version, n_draws, gbd_round_id,
                 version, verbose, turn_off_null_and_nan_check,
                 no_sex_aggr, no_age_aggr):
        self.command = DalynatorMostDetailedTask.create_unique_base_command(
            location_id, year_id)
        super(DalynatorMostDetailedTask, self).__init__(self.command,
                                                        upstream_tasks=[])
        self.out_dir = out_dir

        params = ' --input_data_root {i} --out_dir {o} --tool_name "dalynator"\
         --cod {cod} --epi {epi} --n_draws {n} --version {v} --gbd_round_id \
        {g}'.format(i=input_data_root, o=out_dir, cod=int(cod_version),
                    epi=int(epi_version), n=int(n_draws), v=int(version),
                    g=int(gbd_round_id))
        if verbose:
            params += " --verbose"
        if turn_off_null_and_nan_check:
            params += " --turn_off_null_and_nan_check"
        if no_sex_aggr:
            params += " --no_sex"
        if no_age_aggr:
            params += " --no_age"
        self.extended_command = '{c} {p}'.format(c=self.command, p=params)

    def bind(self, job_list_manager):
        logger.debug("Create job, full command = {}"
                     .format(self.extended_command))
        process_timeout = 60 * 90
        self.job_id = job_list_manager.create_job(
            jobname=self.hash_name,
            job_hash=self.hash,
            command=self.extended_command,
            slots=20,
            mem_free=40,
            max_attempts=11,
            max_runtime=process_timeout + max(2, .1 * process_timeout)
        )
        self.status = JobStatus.REGISTERED
        return self.job_id

    @staticmethod
    def create_unique_base_command(location_id, year_id):
        """This command builds the minimum-sized unique command that identifies
        a job. This function is required because downstream tasks need to be
        able find their upstream's jobs, without passing all the extended
        arguments"""
        return ("run_dalynator_most_detailed "
                "--location_id {location_id} --year_id {year_id}"
                .format(location_id=int(location_id), year_id=int(year_id)))
