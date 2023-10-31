import logging

from jobmon.client.swarm.workflow.bash_task import BashTask


logger = logging.getLogger(__name__)


class CSUpstreamFilter(object):

    def __init__(self, task_dag):
        logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
        logging.warning('is when cs sort task started.')
        self._task_dag = task_dag
        self._us_tasks_by_location_year = None
        self.task_loc_map = {}
        self.task_years_map = {}

    def get_upstreams(self, location_id, all_year_ids):
        return_tasks = []
        if not self._us_tasks_by_location_year:
            self._us_tasks_by_location_year = self._get_tasks_for_upstream()
        for year_id in all_year_ids:
            loc_year_str = "{loc} : {year_id}".format(loc=location_id, year_id=year_id)
            if loc_year_str in self._us_tasks_by_location_year:
                return_tasks.append(self._us_tasks_by_location_year[loc_year_str])
        return return_tasks

    def _get_phase_tasks(self):
        all_tasks = self._task_dag.tasks.values()
        tasks = []
        tasks.extend([t for t in all_tasks
                     if isinstance(t, BashTask) and "most_detailed" in t.command])
        tasks.extend([t for t in all_tasks
                     if isinstance(t, BashTask) and "run_cleanup" in t.command])
        tasks.extend([t for t in all_tasks
                     if isinstance(t, BashTask) and "run_pct_change" in t.command])
        if not tasks:
            logger.debug("No upstream Tasks identified. Something is amiss in the Swarm definition")
            raise RuntimeError("No upstream Tasks identified. Something is "
                               "amiss in the Swarm definition")
        return tasks

    def _get_tasks_for_upstream(self):
        tasks_for_upstream = {}
        us_tasks = self._get_phase_tasks()
        for task in us_tasks:
            loc = self.task_loc_map[task.hash]
            year_id = self.task_years_map[task.hash]
            loc_year_str = "{loc} : {year_id}".format(loc=loc, year_id=year_id)
            if loc_year_str not in tasks_for_upstream:
                tasks_for_upstream[loc_year_str] = []
            tasks_for_upstream[loc_year_str].append(task)
        return tasks_for_upstream

