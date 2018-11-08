import os
import pandas as pd

from jobmon.workflow.python_task import PythonTask
from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.task_dag_viz import TaskDagViz
from jobmon.workflow.workflow import Workflow

from db_queries import get_location_metadata


SPECIAL_LOCATIONS = [3, 5, 11, 20, 24, 26, 28, 31, 32, 46]


class CoDCorrectJobSwarm(object):

    CONN_DEF_MAP = {'cod': {'dev': 'cod-test', 'prod': 'cod'},
                    'gbd': {'dev': 'gbd-test', 'prod': 'gbd'}}
    STD_ERR = 'FILEPATH'
    STD_OUT = 'FILEPATH'

    def __init__(self, code_dir, version_id, year_ids, start_years,
                 location_set_ids, databases, db_env='dev'):
        """
        Arguments:
            code_dir (str): The directory containing CoDCorrect's code base.
            version_id (int): cod.output_version.output_version_id
            year_ids (int[]): set of years to run.
            start_years (int[]):
            location_set_ids (int[]):
            measure_ids (int[]):
            sex_ids (int[]):
            databases (str[]):
            db_env (str):
        """
        self.code_dir = code_dir
        self.version_id = version_id
        self.year_ids = year_ids
        self.start_years = start_years
        self.location_set_ids = location_set_ids
        self.databases = databases
        self.db_env = db_env

        self.most_detailed_locations = self.get_most_detailed_location_ids()
        self.all_locations = self.get_all_location_ids()

        self.measure_ids = [1, 4]
        self.sex_ids = [1, 2]
        self.pct_change = [True, False]

        self.task_dag = TaskDag(name=('CoDCorrect_v{}'
                                      .format(self.version_id)))
        self.shock_jobs_by_command = {}
        self.correct_jobs_by_command = {}
        self.agg_cause_jobs_by_command = {}
        self.ylls_jobs_by_command = {}
        self.agg_loc_jobs_by_command = {}
        self.append_shock_jobs_by_command = {}
        self.append_diag_jobs_by_command = {}
        self.summarize_jobs_by_command = {}
        self.upload_jobs_by_command = {}

    def calculate_slots_and_memory(self, years, base):
        """Determine the number of slots and memory to request.

        Arguments:
            years (int): list of years in run.
            base (int): base number of slots.

        Returns:
            Tuple representing the number of slots to request and the amount of
            memory.
        """
        slots = base * 6 if len(years) > 8 else base
        return slots, slots * 2

    def get_most_detailed_location_ids(self):
        locs = get_location_metadata(gbd_round_id=5, location_set_id=35)
        return locs.location_id.tolist()

    def get_all_location_ids(self):
        locs = []
        for loc_set in self.location_set_ids:
            locs.append(get_location_metadata(gbd_round_id=5,
                                              location_set_id=loc_set))
        locs = pd.concat(locs)
        all_locs = locs.location_id.unique().tolist()

        return all_locs

    def create_shock_and_correct_jobs(self):
        """First set of tasks, no upstream tasks."""
        slots, mem = self.calculate_slots_and_memory(self.year_ids, 3)
        for loc in self.most_detailed_locations:
            for sex in self.sex_ids:
                shock_task = PythonTask(
                    script=os.path.join(self.code_dir, 'shocks.py'),
                    args=['--output_version_id', self.version_id,
                          '--location_id', loc,
                          '--sex_id', sex],
                    name='shocks_{version}_{loc}_{sex}'.format(
                        version=self.version_id, loc=loc, sex=sex),
                    slots=slots,
                    mem_free=mem,
                    max_attempts=3,
                    tag='shock')
                self.task_dag.add_task(shock_task)
                self.shock_jobs_by_command[shock_task.name] = shock_task

                correct_task = PythonTask(
                    script=os.path.join(self.code_dir, 'correct.py'),
                    args=['--output_version_id', self.version_id,
                          '--location_id', loc,
                          '--sex_id', sex],
                    name='correct_{version}_{loc}_{sex}'.format(
                        version=self.version_id, loc=loc, sex=sex),
                    slots=slots,
                    mem_free=mem,
                    max_attempts=3,
                    tag='correct')
                self.task_dag.add_task(correct_task)
                self.correct_jobs_by_command[correct_task.name] = correct_task

    def create_agg_cause_jobs(self):
        slots, mem = self.calculate_slots_and_memory(self.year_ids, 7)
        for loc in self.most_detailed_locations:
            task = PythonTask(
                script=os.path.join(self.code_dir, 'aggregate_causes.py'),
                args=['--output_version_id', self.version_id,
                      '--location_id', loc],
                name='agg_cause_{version}_{loc}'.format(
                    version=self.version_id, loc=loc),
                slots=slots,
                mem_free=mem,
                max_attempts=3,
                tag='agg_cause')
            # add shock/correct upstream dependencies
            for sex in self.sex_ids:
                task.add_upstream(
                    self.shock_jobs_by_command[
                        'shocks_{version}_{loc}_{sex}'.format(
                            version=self.version_id, loc=loc, sex=sex)]
                )
                task.add_upstream(
                    self.correct_jobs_by_command[
                        'correct_{version}_{loc}_{sex}'.format(
                            version=self.version_id, loc=loc, sex=sex)]
                )
            self.task_dag.add_task(task)
            self.agg_cause_jobs_by_command[task.name] = task

    def create_yll_jobs(self):
        slots, mem = self.calculate_slots_and_memory(self.year_ids, 3)
        for loc in self.most_detailed_locations:
            task = PythonTask(
                script=os.path.join(self.code_dir, 'ylls.py'),
                args=['--output_version_id', self.version_id,
                      '--location_id', loc],
                name='ylls_{version}_{loc}'.format(
                    version=self.version_id, loc=loc),
                slots=slots,
                mem_free=mem,
                max_attempts=3,
                tag='ylls')
            # add cause_agg upstream dependencies
            task.add_upstream(
                self.agg_cause_jobs_by_command[
                    'agg_cause_{version}_{loc}'.format(
                        version=self.version_id, loc=loc)]
            )
            self.task_dag.add_task(task)
            self.ylls_jobs_by_command[task.name] = task

    def create_agg_location_jobs(self):
        slots, mem = 10, 100
        for loc_set in self.location_set_ids:
            for measure in self.measure_ids:
                for data_type in ['shocks', 'unscaled', 'rescaled']:
                    if data_type == 'unscaled' and measure == 4:
                        continue
                    for year_id in self.year_ids:
                        task = PythonTask(
                            script=os.path.join(
                                self.code_dir, 'aggregate_locations.py'),
                            args=['--output_version_id', self.version_id,
                                  '--df_type', data_type,
                                  '--measure_id', measure,
                                  '--location_set_id', loc_set,
                                  '--year_id', year_id],
                            name=('agg_locations_{}_{}_{}_{}_{}'
                                  .format(self.version_id, data_type, measure,
                                          loc_set, year_id)),
                            slots=slots,
                            mem_free=mem,
                            max_attempts=5,
                            tag='agg_location')
                        for loc in self.most_detailed_locations:
                            if measure == 4:
                                task.add_upstream(
                                    self.ylls_jobs_by_command[
                                        'ylls_{}_{}'.format(
                                            self.version_id, loc)])
                            else:
                                task.add_upstream(
                                    self.agg_cause_jobs_by_command[
                                        'agg_cause_{}_{}'.format(
                                            self.version_id, loc)])
                        # Some of our special locations for final round
                        # estimates treat otherwise aggregated locations as
                        # most-detailed locations. This will throw an
                        # AssertionError in the aggregator if it cannot find
                        # the aggregate location's file. This if block ensures
                        # that the primary estimation location set (35) is run
                        # first before these special location aggregation jobs
                        # are run. This will slow down CoDCorrect overall.
                        if loc_set in SPECIAL_LOCATIONS:
                            task.add_upstream(
                                self.agg_loc_jobs_by_command[
                                    'agg_locations_{}_{}_{}_{}_{}'.format(
                                        self.version_id, data_type, measure,
                                        35, year_id)])
                        self.task_dag.add_task(task)
                        self.agg_loc_jobs_by_command[task.name] = task

    def create_append_shock_jobs(self):
        slots, mem = self.calculate_slots_and_memory(self.year_ids, 7)
        for loc in self.all_locations:
            task = PythonTask(
                script=os.path.join(self.code_dir, 'append_shocks.py'),
                args=['--output_version_id', self.version_id,
                      '--location_id', loc],
                name='append_shocks_{version}_{loc}'.format(
                    version=self.version_id, loc=loc),
                slots=slots,
                mem_free=mem,
                max_attempts=3,
                tag='append_shock')
            # for job in self.agg_loc_jobs_by_command.values():
            #     task.add_upstream(job)
            self.task_dag.add_task(task)
            self.append_shock_jobs_by_command[task.name] = task

    def create_summary_jobs(self):
        for loc in self.all_locations:
            for db in ['gbd', 'cod']:
                slots, mem = (15, 30) if db == 'cod' else (26, 52)
                task = PythonTask(
                    script=os.path.join(self.code_dir, 'summary.py'),
                    args=['--output_version_id', self.version_id,
                          '--location_id', loc,
                          '--db', db],
                    name='summary_{version}_{loc}_{db}'.format(
                        version=self.version_id, loc=loc, db=db),
                    slots=slots,
                    mem_free=mem,
                    max_attempts=3,
                    tag='summary')
                task.add_upstream(
                    self.append_shock_jobs_by_command[
                        'append_shocks_{version}_{loc}'.format(
                            version=self.version_id, loc=loc)])
                self.task_dag.add_task(task)
                self.summarize_jobs_by_command[task.name] = task

    def create_append_diagnostic_jobs(self):
        slots, mem = (18, 36)
        task = PythonTask(
            script=os.path.join(self.code_dir, 'append_diagnostics.py'),
            args=['--output_version_id', self.version_id],
            name='append_diagnostics_{version}'.format(
                version=self.version_id),
            slots=slots,
            mem_free=mem,
            max_attempts=3,
            tag='append_diag')
        for job in self.append_shock_jobs_by_command.values():
            task.add_upstream(job)
        self.task_dag.add_task(task)
        self.append_diag_jobs_by_command[task.name] = task

    def create_upload_jobs(self):
        slots, mem = (10, 20)
        for measure in self.measure_ids:
            for db in self.databases:
                # cod and codcorrect databases only upload measure 1: deaths.
                if measure == 4 and db in ['cod', 'codcorrect']:
                    continue
                # cod and gbd db have separate test and production servers to
                # choose from. The codcorrect db doesn't have a test server
                if db in ['cod', 'gbd']:
                    conn_def = self.CONN_DEF_MAP[db][self.db_env]
                else:
                    conn_def = 'codcorrect'
                for change in self.pct_change:
                    # codcorrect & cod database does not upload for change.
                    if change and db in ['codcorrect', 'cod']:
                        continue
                    task = PythonTask(
                        script=os.path.join(self.code_dir, 'upload.py'),
                        args=['--output_version_id', self.version_id,
                              '--db', db,
                              '--measure_id', measure,
                              '--conn_def', conn_def,
                              '{}'.format('--change' if change else '')],
                        name='upload_{version}_{db}_{meas}_{change}'.format(
                            version=self.version_id, db=db, meas=measure,
                            change=change),
                        slots=slots,
                        mem_free=mem,
                        max_attempts=3,
                        tag='upload')
                    if db in ['cod', 'gbd']:
                        for loc in self.all_locations:
                            task.add_upstream(
                                self.summarize_jobs_by_command[
                                    'summary_{version}_{loc}_{db}'
                                    .format(version=self.version_id, loc=loc,
                                            db=db)])
                    else:
                        for job in self.append_diag_jobs_by_command.values():
                            task.add_upstream(job)
                    self.task_dag.add_task(task)

    def create_post_scriptum_upload(self):
        slots, mem = (1, 2)
        for db in self.databases:
            if db in ['cod', 'gbd']:
                task = PythonTask(
                    script=os.path.join(self.code_dir,
                                        'post_scriptum_upload.py'),
                    args=['--output_version_id', self.version_id,
                          '--db', db,
                          '{}'.format(
                              '--test' if self.db_env == 'dev' else '')],
                    name=('post_scriptum_upload_{version}_{db}'
                          .format(version=self.version_id, db=db)),
                    slots=slots,
                    mem_free=mem,
                    max_attempts=1,
                    tag='post_scriptum_upload')
                upload_jobs = list(self.upload_jobs_by_command.values())
                for job in upload_jobs:
                    task.add_upstream(job)
                self.task_dag.add_task(task)

    def run(self):
        wf = Workflow(self.task_dag, 'codcorrect_v{}'.format(self.version_id),
                      stderr=self.STD_ERR, stdout=self.STD_OUT,
                      project='proj_codcorrect')
        success = wf.run()
        return success

    def visualize(self):
        TaskDagViz(self.task_dag, graph_outdir='FILEPATH',
                   output_format='svg').render()
