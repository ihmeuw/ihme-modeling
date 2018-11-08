import logging

from jobmon.models import JobStatus
from jobmon.workflow.executable_task import ExecutableTask

from dalynator.tasks.burdenator_most_detailed_task import \
    BurdenatorMostDetailedTask

logger = logging.getLogger(__name__)


class LocationAggregationTask(ExecutableTask):
    def __init__(self, data_root, location_set_id, location_ids,
                 year_id, rei_id, sex_id, measure_id, n_draws, version,
                 write_out_star_ids, region_locs, verbose, gbd_round_id,
                 upstream_tasks):
        self.command = LocationAggregationTask.create_unique_base_command(
            location_set_id, year_id, rei_id, measure_id, sex_id,
            write_out_star_ids)
        super(LocationAggregationTask, self).__init__(self.command,
                                                      upstream_tasks=[])
        self.data_root = data_root

        if upstream_tasks:
            for loc in location_ids:
                self.add_upstream(upstream_tasks[
                    BurdenatorMostDetailedTask.create_unique_base_command(
                        loc, year_id, write_out_star_ids)])

        params = ('--data_root {dr} --gbd_round_id {g} '
                  '--version {v} --n_draws {n} '
                  '--region_locs {rl}'
                  .format(
                      dr=data_root, g=gbd_round_id, v=version,
                      n=n_draws, rl=' '.join(str(loc) for loc in region_locs)))
        if verbose:
            params += " --verbose"

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
            max_runtime=process_timeout + max(2, .1 * process_timeout),
            max_attempts=11
        )
        self.status = JobStatus.REGISTERED
        return self.job_id

    @staticmethod
    def create_unique_base_command(location_set_id, year_id,
                                   rei_id, measure_id, sex_id, write_out_star_ids):
        """This command builds the minimum-sized unique command that identifies
        a job. This function is required because downstream tasks need to be
        able find their upstream's jobs, without passing all the extended
        arguments"""
        command = ("run_loc_agg "
                   "--location_set_id {loc} --year_id {y} --rei_id {rei} "
                   "--sex_id {s} --measure_id {m}"
                   .format(loc=int(location_set_id), y=int(year_id),
                           rei=int(rei_id), s=int(sex_id), m=int(measure_id)))

        if bool(write_out_star_ids):
            command += " --star_ids"
        return command
