import logging

from gbd import constants as gbd
from jobmon.models import JobStatus
from jobmon.workflow.executable_task import ExecutableTask

from dalynator.tasks.location_aggregation_task import LocationAggregationTask

logger = logging.getLogger(__name__)


class BurdenatorCleanupTask(ExecutableTask):
    def __init__(self, input_data_root, out_dir, measure_id,
                 location_id, year_id, n_draws, cod_version, epi_version,
                 version, write_out_star_ids, gbd_round_id, verbose,
                 turn_off_null_and_nan_check,
                 location_set_ids, male_reis, female_reis, upstream_tasks):
        self.command = BurdenatorCleanupTask.create_unique_base_command(
            measure_id, location_id, year_id, write_out_star_ids)
        super(BurdenatorCleanupTask, self).__init__(self.command,
                                                    upstream_tasks=[])
        self.out_dir = out_dir

        sex_to_sex_specific_reis = {gbd.sex.MALE: male_reis,
                                    gbd.sex.FEMALE: female_reis}
        if upstream_tasks:
            for loc_set in location_set_ids:
                for sex in [gbd.sex.MALE, gbd.sex.FEMALE]:
                    for rei in sex_to_sex_specific_reis[sex]:
                        self.add_upstream(upstream_tasks[
                            LocationAggregationTask.create_unique_base_command(
                                loc_set, year_id, rei, measure_id,
                                sex, write_out_star_ids)])

        params = (' --input_data_root {i} --out_dir {o} --n_draws {n} --cod '
                  '{cv} --epi {e} --version {v} '
                  '--gbd_round_id {g} '
                  '--tool_name '
                  'burdenator'.format(i=input_data_root, o=out_dir,
                                       n=int(n_draws), cv=int(cod_version),
                                       e=int(epi_version), v=int(version),
                                       g=int(gbd_round_id)))
        if verbose:
            params += " --verbose"
        if turn_off_null_and_nan_check:
            params += " --turn_off_null_and_nan_check"

        self.extended_command = '{c} {p}'.format(c=self.command, p=params)

    def bind(self, job_list_manager):
        logger.debug("Create job, full command = {}"
                     .format(self.extended_command))

        process_timeout = 60 * 60
        self.job_id = job_list_manager.create_job(
            jobname=self.hash_name,
            job_hash=self.hash,
            command=self.extended_command,
            slots=25,
            mem_free=50,
            max_runtime=process_timeout + max(2.0, .1 * process_timeout),
            max_attempts=11
        )
        self.status = JobStatus.REGISTERED
        return self.job_id

    @staticmethod
    def create_unique_base_command(measure_id, location_id, year_id,
                                   write_out_star_ids):
        """This command builds the minimum-sized unique command that identifies
        a job. This function is required because downstream tasks need to be
        able find their upstream's jobs, without passing all the extended
        arguments"""
        command = ('run_cleanup '
                   ' --measure_id {mid} --location_id {lid} --year_id {yid}'
                   .format(mid=int(measure_id),
                           lid=int(location_id),
                           yid=int(year_id)))

        if bool(write_out_star_ids):
            command += " --star_ids"
        return command
