import logging

from jobmon.models import JobStatus
from jobmon.workflow.executable_task import ExecutableTask

from dalynator.tasks.burdenator_cleanup_task import BurdenatorCleanupTask
from dalynator.tasks.burdenator_most_detailed_task import \
    BurdenatorMostDetailedTask
from dalynator.tasks.dalynator_most_detailed_task import \
    DalynatorMostDetailedTask

logger = logging.getLogger(__name__)


class PercentageChangeTask(ExecutableTask):
    def __init__(self, out_dir, tool_name, location_id,
                 is_aggregate, start_year, end_year, measure_id,
                 gbd_round_id, version, write_out_star_ids,
                 n_draws, verbose, all_phases, upstream_tasks):
        self.command = PercentageChangeTask.create_unique_base_command(
            location_id, measure_id, start_year, end_year, write_out_star_ids)
        super(PercentageChangeTask, self).__init__(self.command,
                                                   upstream_tasks=[])
        self.out_dir = out_dir
        self.tool_name = tool_name

        if upstream_tasks:
            if self.tool_name == "dalynator":
                for year in [start_year, end_year]:
                    self.add_upstream(upstream_tasks[
                        DalynatorMostDetailedTask.create_unique_base_command(
                            location_id, year)])
            else:
                if is_aggregate:
                    for year in [start_year, end_year]:
                        self.add_upstream(upstream_tasks[
                            BurdenatorCleanupTask.create_unique_base_command(
                                measure_id, location_id, year,
                                write_out_star_ids)])
                else:  # no cleanup jobs for most-detailed locs
                    if 'most_detailed' in all_phases:  # if not, no upstreams
                        for year in [start_year, end_year]:
                            self.add_upstream(upstream_tasks[
                                BurdenatorMostDetailedTask.
                                              create_unique_base_command(
                                                  location_id, year,
                                                  write_out_star_ids)])

        params = ' --out_dir {o} --tool_name {t} --n_draws {n} \
                  --version {v}  --gbd_round_id {g} '.format(
            o=out_dir, t=tool_name, loc=location_id, s=start_year, e=end_year,
            n=int(n_draws), v=int(version), g=int(gbd_round_id))
        if verbose:
            params += " --verbose"
        if bool(write_out_star_ids):
            params += " --star_ids"
        self.extended_command = '{c} {p}'.format(c=self.command, p=params)

    def bind(self, job_list_manager):
        logger.debug("Create job, full command = {}"
                     .format(self.extended_command))

        process_timeout = 60 * 90
        self.job_id = job_list_manager.create_job(
            jobname=self.hash_name,
            job_hash=self.hash,
            command=self.extended_command,
            slots=45,
            mem_free=90,
            max_attempts=11,
            max_runtime=process_timeout + max(2, .1 * process_timeout),
        )
        self.status = JobStatus.REGISTERED
        return self.job_id

    @staticmethod
    def create_unique_base_command(location_id, measure_id,
                                   start_year, end_year, write_out_star_ids):
        """This command builds the minimum-sized unique command that identifies
        a job. This function is required because downstream tasks need to be
        able find their upstream's jobs, without passing all the extended
        arguments"""
        command = ("run_pct_change "
                   "--location_id {lid} --measure_id {m} --start_year {s} "
                   "--end_year {e}"
                   .format(lid=int(location_id), m=int(measure_id),
                           s=int(start_year), e=int(end_year)))

        if bool(write_out_star_ids):
            command += " --star_ids"

        return command
