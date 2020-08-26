from hierarchies.tree import Node
from jobmon.client.swarm.workflow.python_task import PythonTask

from cascade_ode.demographics import Demographics
from cascade_ode import sge
from cascade_ode.sge import queue_from_runtime
from cascade_ode.settings import load as load_settings
from cascade_ode import drill

settings = load_settings()


class DagNode:
    """
    A node in the cascade job graph. Can be either global, upload, or
    location/year/sex specific node.

    Attributes:
        job_name (str): The SGE job name
        args (List[Union[int, str]]): List of input arguments to job
        upstream_jobs (List[str]): List of job_names of other jobs that must
            finish first before this job can run
            (can be empty list, for global jobs)
        task (List[jobmon.client.swarm.workflow.python_task]): Initially
            empty list. When driver class creates PythonTasks for each
            DagNode in the job dag, this task list is populated with the
            task (this is used to build graph since PythonTask instantiation
            involves passing other PythonTask objects to specify upstream
            dependencies)
        details (dict[str, Any]): A dict that contains any information specific
            to a particular type of node. Currently, it only contains
            information for location specific nodes
        max_attempts (int): How many retries Jobmon should do.
        queue (str): all.q or long.q, which machines should run the job.
        host_type (str|None): Whether to restrict to Intel computers.
        j_resource (bool): Whether this needs a particular mounted drive.
    """
    def __init__(self, job_name, args, upstream_jobs, details):
        self.job_name = job_name
        self.args = args
        self.upstream_jobs = upstream_jobs
        self.task = list()
        self.details = details
        # This is a place to set defaults for all nodes.
        self.max_attempts = 3
        self.host_type = 'intel'

    def __eq__(self, other):
        return self.__dict__ == other.__dict__

    def __hash__(self):
        return hash(self.__dict__)


def add_if_set(host_type):
    """The logic around this flag in qsub is strange. If it is true, then
    you get that resource. If they are false, you are excluded from
    nodes that have that resource. We want to not include the flag
    at all if they are false."""
    if host_type is not None:
        return {'sge_add_args': f'-l {host_type}'}
    else:
        return None


class GlobalDagNode(DagNode):
    '''
    A global cascade job that calls run_global.py
    '''

    name_template = 'dm_{mvid}_G{cv_iter}'
    script = f"{drill.this_path}/run_global.py"

    def add_job(self, wf, jobdag, mvm):
        """
        Create a PythonTask, add it to the workflow, and update the DagNode to
        contain a reference to the task.

        Args:
            wf (jobmon.client.swarm.workflow.workflow): jobmon workflow
            job_dag (dict[str, DagNode]): a mapping of job name to DagNode
            mvm (cascade_ode.importer.Importer.model_version_meta): a dataframe
                of model settings
        """
        # we ignore jobdag since we have no upstream dependencies
        slots, memory, runtime = sge.cluster_limits(
            'global', mvm, details=self.details)
        environment_variables_to_add = settings['env_variables']
        environment_variables_to_add['OMP_NUM_THREADS'] = slots

        memory, runtime = sge.update_ME_specific_allocations(
            modelable_entity_id=mvm.modelable_entity_id.unique()[0],
            memory=memory,
            runtime=runtime)

        task = PythonTask(
            script=self.script,
            args=self.args,
            name=self.job_name,
            upstream_tasks=list(),
            env_variables=environment_variables_to_add,
            num_cores=slots,
            max_runtime_seconds=runtime,
            queue=queue_from_runtime(runtime),
            m_mem_free=f"{memory}G",
            max_attempts=self.max_attempts,
            tag='global',
            j_resource=False)
        task.context_args = add_if_set(self.host_type)

        self.task.append(task)
        wf.add_task(task)


class ChildDagNode(DagNode):
    '''
    A sex/location/year specific cascade job that calls run_children.py
    '''

    name_template = 'dm_{mvid}_{loc_id}_{sex}_{year}_{cv_iter}'
    script = f"{drill.this_path}/run_children.py"

    def add_job(self, wf, job_dag, mvm):
        """
        Create a PythonTask, add it to the workflow, and update the DagNode to
        contain a reference to the task.

        Args:
            wf (jobmon.client.swarm.workflow.workflow): jobmon workflow
            job_dag (dict[str, DagNode]): a mapping of job name to DagNode
            mvm (cascade_ode.importer.Importer.model_version_meta): a dataframe
                of model settings
        """
        num_children = self.details['num_children']
        slots, memory, runtime = sge.cluster_limits(
            'node',
            mvm,
            num_children,
            details=self.details)
        environment_variables_to_add = settings['env_variables']
        environment_variables_to_add['OMP_NUM_THREADS'] = slots

        memory, runtime = sge.update_ME_specific_allocations(
            modelable_entity_id=mvm.modelable_entity_id.unique()[0],
            memory=memory,
            runtime=runtime)

        upstream_tasks = []
        for upstream_jobname in self.upstream_jobs:
            upstream_tasks.extend(job_dag[upstream_jobname].task)

        task = PythonTask(
            script=self.script,
            args=self.args,
            name=self.job_name,
            upstream_tasks=upstream_tasks,
            env_variables=environment_variables_to_add,
            num_cores=slots,
            max_runtime_seconds=runtime,
            queue=queue_from_runtime(runtime),
            m_mem_free=f"{memory}G",
            max_attempts=self.max_attempts,
            tag='node',
            j_resource=False)
        task.context_args = add_if_set(self.host_type)

        self.task.append(task)
        wf.add_task(task)


class UploadDagNode(DagNode):
    '''
    A single cascade job for uploading that calls varnish.py
    '''

    name_template = 'dm_{mvid}_varnish'
    script = f"{drill.this_path}/varnish.py"

    def add_job(self, wf, job_dag, mvm):
        """
        Create a PythonTask, add it to the workflow, and update the DagNode to
        contain a reference to the task.

        Args:
            wf (jobmon.client.swarm.workflow.workflow): jobmon workflow
            job_dag (dict[str, DagNode]): a mapping of job name to DagNode
            mvm (cascade_ode.importer.Importer.model_version_meta): a dataframe
                of model settings

        The varnish job does the following:
           1. Uploads fits
           2. Uploads adjusted data
           3. Computes fit statistics
           4. Uploads fit statistics
           5. Attempts to generate diagnostic plots
           5. Computes and uploads finals (save-results)
           6. Updates the status of the model to finished
        """
        slots, memory, runtime = sge.cluster_limits(
            'varnish', mvm, details=self.details)
        environment_variables_to_add = settings['env_variables']
        environment_variables_to_add['OMP_NUM_THREADS'] = slots

        # Varnish memory allocation shouldn't change with job size, and
        # large memory allocations will prohibit scheduling.
        _, runtime = sge.update_ME_specific_allocations(
            modelable_entity_id=mvm.modelable_entity_id.unique()[0],
            memory=memory,
            runtime=runtime)

        upstream_tasks = []
        for upstream_jobname in self.upstream_jobs:
            upstream_tasks.extend(job_dag[upstream_jobname].task)

        task = PythonTask(
            script=self.script,
            args=self.args,
            name=self.job_name,
            upstream_tasks=upstream_tasks,
            env_variables=environment_variables_to_add,
            num_cores=slots,
            max_runtime_seconds=runtime,
            queue=queue_from_runtime(runtime),
            m_mem_free=f"{memory}G",
            max_attempts=self.max_attempts,
            tag='upload',
            j_resource=True)
        task.context_args = add_if_set(self.host_type)

        self.task.append(task)
        wf.add_task(task)


def make_dag(mvid, loctree, cv_iter, add_arguments=None):
    """
    Build a dict that represents the cascade job graph. The dict is
    a mapping of job-name -> DagNode

    Args:
        mvid (int): model version id (used in job-name creation)
        loctree (hierarchies.tree.Tree): location hierarchy to build dag
            from
        cv_iter (int): cross validation iteration.
        add_arguments (List[str]): Arguments to add to every executable of
            the Cascade.

    Returns:
        dict[str, DagNode]
    """
    add_arguments = add_arguments if add_arguments else list()

    dag = {}
    demo = Demographics(mvid)

    global_jobs = make_global_jobs(mvid, cv_iter, add_arguments=add_arguments)
    global_job_names = list(global_jobs.keys())

    node_jobs = make_child_jobs(mvid, demo, cv_iter, global_job_names, loctree,
                                add_arguments=add_arguments
                                )
    node_job_names = list(node_jobs.keys())

    all_job_names = global_job_names + node_job_names
    varnish_job = make_upload_job(
        mvid, all_job_names, add_arguments=add_arguments)

    # this ordering is important -- when we build the jobmon workflow we
    # iterate through this dict and build PythonTasks. For every node in the
    # graph we assume all upstream tasks have already been visited and
    # their DagNodes have PythonTasks associated in DagNode.task
    all_dicts = [global_jobs, node_jobs, varnish_job]
    for d in all_dicts:
        dag.update(d)

    return dag


def make_global_jobs(mvid, cv_iter, add_arguments=None):
    """
    Returns a dict of job-name -> GlobalDagNode that represents all global
    jobs.

    If cv_iter is None, then this is just a dict of length 1. If cv_iter is
    an integer, the dict will be of length cv_iter.

    Args:
        mvid (int): model version id
        cv_iter (Opt[int]): Optional integer representing number of cross
            validation jobs
        add_arguments (List[str]): Arguments to add to every executable of
            the Cascade.

    Returns:
        dict[str, GlobalDagNode]
    """
    add_arguments = add_arguments if add_arguments else list()
    jobs = {}
    if cv_iter is None:
        cv_iter = [0]
    for i in cv_iter:
        job_name = GlobalDagNode.name_template.format(mvid=mvid, cv_iter=i)
        args = [mvid, '--cv_iter', i] + add_arguments
        upstream_jobs = []
        details = {}
        jobs[job_name] = GlobalDagNode(
            job_name, args, upstream_jobs, details)
    return jobs


def make_child_jobs(mvid, demo, cv_iter, global_job_names, lt,
                    add_arguments=None):
    """
    Returns a dict of job-name -> ChildDagNode that represents all
    run_children.py jobs.

    Args:
        mvid (int): model version id
        demo (cascade_ode.demographics.Demographics): demographic info
        cv_iter (Opt[int]): Optional integer representing number of cross
            validation jobs
        global_job_names (List[str]): We need list of global job names
            to associate upstream tasks of initial node jobs
        lt (hierarchies.tree.Tree): location hierarchy
        add_arguments (List[str]): Arguments to add to all jobs.

    Returns:
        dict[str, ChildDagNode]
    """
    add_arguments = add_arguments if add_arguments else list()
    jobs = {}
    if cv_iter is None:
        cv_iter = [0]
    sex_dict = {1: 'male', 2: 'female'}

    root_node = lt.root
    leaf_locs = Node.s_leaves(root_node)
    nodes_to_run = [l for l in lt.nodes if l not in leaf_locs]

    for i in cv_iter:
        global_job = [j for j in global_job_names if j.endswith("G" + str(i))]
        assert len(global_job) == 1
        for sex_id in demo.sex_ids:
            sex = sex_dict[sex_id]
            sex_short = sex[0]
            for year_id in demo.year_ids:
                year = str(year_id)[2:]
                for node in nodes_to_run:
                    parent_id = None if node == root_node else node.parent.id
                    if parent_id is None:
                        parent_name = global_job[0]
                    else:
                        parent_name = ChildDagNode.name_template.format(
                            mvid=mvid, loc_id=parent_id, sex=sex_short,
                            year=year, cv_iter=i)

                    this_job = ChildDagNode.name_template.format(
                        mvid=mvid, loc_id=node.id, sex=sex_short, year=year,
                        cv_iter=i)
                    args = [mvid, node.id, sex, year_id, i] + add_arguments
                    parent_jobs = [parent_name]
                    # we need to include num-children and demographic info
                    # here because we need
                    # that info to infer SGE memory allocation when building
                    # jobmon PythonTask
                    details = {
                        'num_children': len(node.children),
                        'location_id': node.id,
                        'sex': sex_short,
                        'year': str(year)
                    }
                    jobs[this_job] = ChildDagNode(
                        this_job, args, parent_jobs, details)

    return jobs


def make_upload_job(mvid, all_job_names, add_arguments=None):
    """
    Returns a dict of job-name -> UploadDagNode that represents the final
    upload job (varnish.py)

    Args:
        mvid (int): model version id
        all_job_names (List[str]): We need list of all job names
            to associate upstream tasks for varnish
        add_arguments (List[str]): Arguments to add to all jobs.

    Returns:
        dict[str, UploadDagNode]
    """
    add_arguments = add_arguments if add_arguments else list()
    job_name = UploadDagNode.name_template.format(mvid=mvid)
    args = [mvid] + add_arguments
    details = {}
    return {
        job_name: UploadDagNode(job_name, args, all_job_names, details)}


def check_error_msg_for_sigkill(error_msg):
    """
    Take an error string, return boolean if strings contain
    '<Signals.SIGKILL: 9>'

    '<Signals.SIGKILL: 9>' is a generic error code for when the scheduler kills
    a given process. We assume that when we run into this error that the dismod
    kernel has been killed by the SGE due to memory overages. This sigkill,
    however, is not propagated to the Python process, and thus jobmon does not
    allocate more resources upon this type of failure. We use this function to
    look for this so we can kill the parent python process that causes jobmon
    to bump the given task's memory.
    """
    return '<Signals.SIGKILL: 9>' in error_msg
