import math
import os

import numpy as np

from db_tools import ezfuncs
from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.bash_task import BashTask
from orm_stgpr.lib.util import query

from stgpr.model.config import *
from stgpr.common.constants import paths


def intersection(a, b):
    return list(set(a) & set(b))

RESOURCE_SCALES = {
    'm_mem_free': 1,
    'max_runtime_seconds': 1
}

class STGPRJobSwarm:
    def __init__(self, run_id, run_type, holdouts, draws, nparallel,
                 n_parameter_sets, cluster_project, error_log_path,
                 output_log_path, location_set_id, gbd_round_id,
                 custom_stage1, rake_logit, code_version, decomp_step,
                 modelable_entity_id, output_path):

        self.run_id = run_id
        self.run_type = run_type
        self.holdouts = holdouts
        self.draws = draws
        self.nparallel = nparallel
        self.n_parameter_sets = n_parameter_sets
        self.cluster_project = cluster_project
        self.location_set_id = location_set_id
        self.gbd_round_id = gbd_round_id
        self.custom_stage1 = custom_stage1
        self.rake_logit = rake_logit
        self.code_version = code_version
        self.decomp_step = decomp_step
        self.output_path = output_path
        self.is_diet_model = modelable_entity_id in [
            2430, 2442, 2431, 2434, 2433, 2437, 2435, 2428, 2429, 2436, 2427,
            2432, 2440, 9804, 2441, 2544, 23766, 2544, 2438, 23604, 23683

        ]

        # set some stuff
        self.max_attempts = 3

        # create workflow
        self.workflow = Workflow(
            workflow_args=f'stgpr_{self.run_id}',
            project=self.cluster_project,
            stderr=error_log_path,
            stdout=output_log_path,
            resume=True
        )

        # set up job lists
        self.stage1_jobs = {}
        self.st_jobs = {}
        self.descanso_jobs = {}
        self.gpr_jobs = {}
        self.post_jobs = {}
        self.rake_jobs = {}
        self.cleanup_jobs = {}
        self.eval_jobs = {}

        # set up conditionals
        if self.run_type == 'in_sample_selection':
            self.param_groups = np.array_split(list(range(0, self.n_parameter_sets)),
                                               MAX_SUBMISSIONS)
        elif self.run_type == 'oos_selection':
            split = math.floor(float(MAX_SUBMISSIONS) / float(self.holdouts))
            self.param_groups = np.array_split(list(range(0, self.n_parameter_sets)),
                                               split)
        else:
            self.param_groups = np.array_split(
                list(range(0, self.n_parameter_sets)), 1)

    def prep_parallelization_groups(self):
        """Parallize by splitting locations
        to be modelled into *nparallel* groups.
        This function grabs the location hierarchy,
        identifies needed locations, and assigns
        each one to a parallelization group."""
        session = ezfuncs.get_session(conn_def='epi')
        locs = query.get_locations(
            self.location_set_id, self.gbd_round_id, self.decomp_step, session
        )[0]
        self.locs = locs.sort_values(by=['level_{}'.format(NATIONAL_LEVEL)])
        self.parallel_groups = np.array_split(
            self.locs.loc[self.locs.level >= NATIONAL_LEVEL][SPACEVAR].values,
            self.nparallel)

        # prep raking upstreams and submission locations
        lvl = 'level_{}'.format(NATIONAL_LEVEL)
        self.subnat_locations = (locs.loc[locs.level > NATIONAL_LEVEL, lvl]
                                 .unique().astype(int))

    def assign_rake_runtimes(self):
        """
        Rake jobs have immensely different times based
        almost exclusively on the number of subnationals nested
        within the national location. Assign memory based on the
        number of subnationals for each national location with
        subnationals, using (slightly modified)
        intercept and beta values from a  super simple
        linear regression,

        'memory ~ n_subnats'

        for one very data-dense model
        (ie a good upper bound for all st-gpr models)

        Memory Intercept: .75
        Memory Beta_N_subnats: .5

        (More conservative for runtime because not as
        wasteful to go high)
        Runtime Intercept: 5 (min), so 300 sec
        Runtime Beta_N_subnats: .35 (min), so 21 sec
        """

        natcol = f'level_{NATIONAL_LEVEL}'
        self.locs['subnat'] = \
            (self.locs['level'] > NATIONAL_LEVEL).astype(int)
        n_subnats = self.locs.groupby(natcol)['subnat'].sum()
        n_subnats = n_subnats.loc[n_subnats > 0].reset_index(name='N')
        n_subnats[natcol] = n_subnats[natcol].astype(int)

        # assign memory
        n_subnats['mem'] = .75 + .5 * n_subnats['N']
        n_subnats['runtime'] = 300 + 21 * n_subnats['N']
        n_subnats = n_subnats.rename(columns={natcol: 'location'})
        self.rake_memory_df = n_subnats.copy()

    def create_stage1_jobs(self):
        """First set of tasks, thus no upstream tasks.
        Only run stage1 if no custom stage1 (custom_stage1)
        estimates. """
        for ko in list(range(0, self.holdouts + 1)):

            # ie shell, script, and args pasted together
            model_root = os.path.join(paths.CODE_ROOT, 'model')
            cmd = (
                f'{RSHELL} -s {STAGE1_SCRIPT} '
                f'{self.output_path} {model_root} {ko}'
            )

            task = BashTask(
                command=cmd,
                name=f'stage1_{self.run_id}_{ko}',
                num_cores=1,
                m_mem_free='3G',
                max_attempts=2,
                max_runtime_seconds=300,
                tag='stgpr_stage1',
                queue='all.q',
                resource_scales=RESOURCE_SCALES,
                hard_limits=True
            )

            self.workflow.add_task(task)
            self.stage1_jobs[task.name] = task

    def create_st_jobs(self):
        for ko in range(0, self.holdouts + 1):
            upstream_job = self.stage1_jobs['stage1_{}_{}'.format(
                self.run_id, ko)]
            for param_group in range(0, len(self.param_groups)):
                for loc_group in range(0, self.nparallel):

                    submit_params = ','.join(
                        [str(x) for x in self.param_groups[param_group]])
                    jname = 'st_{}_{}_{}_{}'.format(
                        self.run_id, ko, param_group, loc_group)

                    memory = 50
                    runtime = 1500
                    if self.is_diet_model:
                        memory = 120
                        runtime = 28800 # 8 hours

                    task = PythonTask(
                        script=ST_SCRIPT,
                        args=[
                            self.run_id,
                            self.output_path,
                            ko,
                            self.run_type,
                            submit_params,
                            self.nparallel,
                            loc_group
                        ],
                        name=jname,
                        num_cores=6,
                        m_mem_free=f'{memory}G',
                        max_attempts=3,
                        max_runtime_seconds=runtime,
                        tag='stgpr_spacetime',
                        queue='all.q',
                        resource_scales=RESOURCE_SCALES,
                        hard_limits=True
                    )

                    task.add_upstream(upstream_job)

                    self.workflow.add_task(task)
                    self.st_jobs[task.name] = task

    def create_descanso_jobs(self):
        """Depends on aggregate locations coming out of loc agg jobs"""
        for ko in list(range(0, self.holdouts + 1)):
            for param_group in list(range(0, len(self.param_groups))):

                submit_params = ','.join(
                    [str(x) for x in self.param_groups[param_group]])

                runtime = 3600 if self.is_diet_model else 300
                task = PythonTask(
                    script=IM_SCRIPT,
                    args=[
                        self.run_id,
                        self.output_path,
                        ko,
                        self.draws,
                        self.nparallel,
                        submit_params
                    ],
                    name=f'descanso_{self.run_id}_{ko}_{param_group}',
                    num_cores=1,
                    m_mem_free='20G',  # upped from 5 to 20 for variance simulation
                    max_runtime_seconds=runtime,
                    max_attempts=2,
                    tag='stgpr_amp_nsv',
                    queue='all.q',
                    resource_scales=RESOURCE_SCALES,
                    hard_limits=True
                )

                # add ST upstreams
                for loc_group in list(range(0, self.nparallel)):
                    st_label = 'st_{}_{}_{}_{}'.format(
                        self.run_id, ko, param_group, loc_group)
                    upstream_job = self.st_jobs[st_label]
                    task.add_upstream(upstream_job)

                self.workflow.add_task(task)
                self.descanso_jobs[task.name] = task

    def create_gpr_jobs(self):
        # set runtime and memory based on draws
        gpr_runtime = 1200
        gpr_memory = 4
        if self.draws == 100:
            gpr_runtime = 1500
            gpr_memory = 7
        elif self.draws == 1000:
            gpr_runtime = 1800
            gpr_memory = 10

        if self.is_diet_model:
            gpr_runtime *= 3
            gpr_memory *= 3

        for ko in list(range(0, self.holdouts + 1)):
            for param_group in list(range(0, len(self.param_groups))):
                upstream_job = self.descanso_jobs['descanso_{}_{}_{}'.format(self.run_id,
                                                                             ko, param_group)]
                for loc_group in list(range(0, self.nparallel)):

                    submit_params = ','.join(
                        [str(x) for x in self.param_groups[param_group]])

                    jname = 'gpr_{}_{}_{}_{}'.format(
                        self.run_id, ko, param_group, loc_group)

                    

                    task = PythonTask(
                        script=GPR_SCRIPT,
                        args=[
                            self.run_id,
                            self.output_path,
                            ko,
                            self.draws,
                            submit_params,
                            self.nparallel,
                            loc_group
                        ],
                        name=jname,
                        num_cores=1,
                        m_mem_free=f'{gpr_memory}G',
                        max_runtime_seconds=gpr_runtime,
                        max_attempts=2,
                        tag='stgpr_gpr',
                        queue='all.q',
                        resource_scales=RESOURCE_SCALES,
                        hard_limits=True
                    )

                    task.add_upstream(upstream_job)

                    self.workflow.add_task(task)
                    self.gpr_jobs[task.name] = task

    def create_rake_jobs(self):
        """Depends on GPR jobs including all the subnationals
        and national locations for each
        rake job, parallelized out by parent_id.
        Raking only done on the first KO (KO 0),
        which does not hold out any data from the dataset."""
        for loc in self.subnat_locations:
            mem = int(np.ceil(self.rake_memory_df.query(
                f'location == {loc}')['mem'].iat[0]))

            rt = int(np.ceil(self.rake_memory_df.query(
                f'location == {loc}')['runtime'].iat[0]))

            if self.draws == 1000:
                mem *= 2
                rt *= 3
                rt = max(rt, 7200)
            
            if self.is_diet_model:
                mem *= 2
                rt *= 3
                rt = max(rt, 14400)

            task = PythonTask(
                script=RAKE_SCRIPT,
                args=[
                    self.run_id,
                    self.output_path,
                    0,
                    self.draws,
                    self.run_type,
                    self.rake_logit,
                    loc
                ],
                name=f'rake_{self.run_id}_{loc}',
                num_cores=1,
                m_mem_free=f'{mem}G',
                max_runtime_seconds=rt,
                max_attempts=2,
                tag='stgpr_rake',
                queue='all.q',
                resource_scales=RESOURCE_SCALES,
                hard_limits=True
            )

            # grab all subnationals and country location_ids associated with a country
            lvl = 'level_{}'.format(NATIONAL_LEVEL)
            all_needed_locs = self.locs.loc[self.locs[lvl] == loc,
                                            'location_id'].unique()

            # add each gpr job containing a needed national/subnational
			# for raking to upstreams
            if self.holdouts == 0:
                for param_group in list(range(0, len(self.param_groups))):
                    for loc_group in list(range(0, self.nparallel)):
                        loc_group_vals = self.parallel_groups[loc_group]

                        common_elements = len(intersection(all_needed_locs.tolist(),
                                                           loc_group_vals.tolist()))
                        if common_elements > 0:
                            task.add_upstream(self.gpr_jobs['gpr_{}_0_{}_{}'.format(self.run_id,
                                                                                    param_group, loc_group)])
            else:
                task.add_upstream(
                    self.eval_jobs['eval_{}'.format(self.run_id)])

            self.workflow.add_task(task)
            self.rake_jobs[task.name] = task

    def create_post_jobs(self):
        """Depends on rake jobs. Calculates fit stats
        and cleans up file folders, no mas."""

        for ko in list(range(0, self.holdouts + 1)):
            for param_group in list(range(0, len(self.param_groups))):

                submit_params = ','.join(
                    [str(x) for x in self.param_groups[param_group]])

                task = PythonTask(
                    script=POST_SCRIPT,
                    args=[
                        self.run_id,
                        self.output_path,
                        ko,
                        self.run_type,
                        self.holdouts,
                        submit_params
                    ],
                    name=f'post_{self.run_id}_{ko}_{param_group}',
                    num_cores=1,
                    m_mem_free='2G',
                    max_runtime_seconds=300,
                    max_attempts=2,
                    tag='stgpr_post',
                    queue='all.q',
                    resource_scales=RESOURCE_SCALES,
                    hard_limits=True
                )

                # add ST upstreams
                for loc_group in list(range(0, self.nparallel)):
                    gp_label = 'gpr_{}_{}_{}_{}'.format(self.run_id, ko,
                                                        param_group, loc_group)
                    upstream_job = self.gpr_jobs[gp_label]
                    task.add_upstream(upstream_job)

                self.workflow.add_task(task)
                self.post_jobs[task.name] = task

    def create_cleanup_jobs(self):
        """Saves rake summaries and removes
        tempfiles no longer needed"""
        runtime = 600 if self.draws == 0 else 7200

        for ko in list(range(0, self.holdouts + 1)):
            task = PythonTask(
                script=CLEANUP_SCRIPT,
                args=[self.run_id, self.output_path, self.run_type, ko, self.draws],
                name=f'clean_{self.run_id}_{ko}',
                num_cores=1,
                m_mem_free='1G',
                max_runtime_seconds=runtime,
                max_attempts=1,
                tag='stgpr_clean',
                queue='all.q'
            )

            if ko == 0:
                for loc in self.subnat_locations:
                    task.add_upstream(
                        self.rake_jobs['rake_{}_{}'.format(self.run_id, loc)])
            else:
                task.add_upstream(
                    self.eval_jobs['eval_{}'.format(self.run_id)])

            self.workflow.add_task(task)
            self.cleanup_jobs[task.name] = task

    def create_eval_jobs(self):
        """
        For hyperparameter selection runs only, determine best hyperparameter
        set based on in-sample or out-of-sample RMSE.
        - run_type = "in_sample_selection"
        - run_type = "oos_selection"

        For runs with only one set of parameters, set the best_param_set
        to the *only* param_set (param_set 0) for consistency in rake inputs.

        Lastly, just collect the disparate fit_stats files and combine into
        a single file, saved as fit_stats.csv for all run types
        """

        task = PythonTask(
            script=EVAL_SCRIPT,
            args=[self.run_id, self.output_path, self.run_type,
                  self.holdouts, self.n_parameter_sets],
            name=f'eval_{self.run_id}',
            num_cores=1,
            m_mem_free='500M',
            max_runtime_seconds=180,
            max_attempts=2,
            tag='stgpr_eval',
            queue='all.q',
            resource_scales=RESOURCE_SCALES,
            hard_limits=True
        )

        for ko in list(range(0, self.holdouts + 1)):
            for param_group in list(range(0, len(self.param_groups))):
                post_label = 'post_{}_{}_{}'.format(
                    self.run_id, ko, param_group)
                task.add_upstream(self.post_jobs[post_label])

        self.workflow.add_task(task)
        self.eval_jobs[task.name] = task

    def run(self):

        # run main model estimation pipeline
        self.prep_parallelization_groups()
        self.create_stage1_jobs()
        self.create_st_jobs()
        self.create_descanso_jobs()
        self.create_gpr_jobs()
        self.create_post_jobs()

        # choose best parameter set to run rake for
        self.create_eval_jobs()

        # run rake/aggregation step and clean outputs
        self.assign_rake_runtimes()
        self.create_rake_jobs()
        self.create_cleanup_jobs()

        status = self.workflow.run()
        print(f'Workflow finished with status {status}')
        return status
