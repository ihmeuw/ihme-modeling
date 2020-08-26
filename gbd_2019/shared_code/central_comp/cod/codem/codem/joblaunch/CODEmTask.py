import os
import subprocess
from datetime import datetime
import logging
import json

from jobmon.client.swarm.workflow.bash_task import BashTask
from db_tools.ezfuncs import query
from codem.reference import db_connect
from codem.joblaunch.profiling import get_parameters

logger = logging.getLogger(__name__)


class CODEmBaseTask(BashTask):
    def __init__(self, model_version_id, parameter_dict,
                 gb, minutes, max_attempts,
                 upstream_tasks, db_connection=None, conn_def=None,
                 hybridizer=False, cores=30,
                 start_date=datetime(2019, 1, 11), end_date=datetime.now()):
        """
        CODEm Base Task inheriting from Bash Task -- should be shared for both CODEm jobs and Hybrid Jobs.

        :param model_version_id:
        :param parameter_dict:
        :param gb:
        :param minutes:
        :param cores: (int) number of cores to do.
        :param max_attempts:
        :param upstream_tasks:
        """
        self.model_version_id = model_version_id

        self.parameter_dict = parameter_dict
        self.gb = gb
        self.minutes = minutes
        self.max_attempts = max_attempts
        self.upstream_tasks = upstream_tasks
        self.cores = cores

        self.db_connection = db_connection
        self.conn_def = conn_def

        self.name = None

        self.metadata_df = None
        self.decomp_step_id = None
        self.model_version_type_id = None
        self.model_version_type = None
        self.sex_id = None
        self.cause_id = None
        self.acause = None
        self.age_start = None
        self.age_end = None

        self.model_dir = None

        self.parameters = None

        self.model_version_metadata()
        self.run_parameters(hybridizer=hybridizer, start_date=start_date, end_date=end_date)

    def model_version_metadata(self):
        """
        Get metadata for the model version object that has already been created.
        """
        call = '''
                SELECT cmt.model_version_type, sc.acause, cmv.gbd_round_id, cmv.decomp_step_id,
                       cmv.run_covariate_selection, cmv.previous_model_version_id,
                       cmv.cause_id, cmv.age_start, cmv.age_end, cmv.model_version_type_id
                FROM cod.model_version cmv
                INNER JOIN shared.cause sc
                ON sc.cause_id = cmv.cause_id
                INNER JOIN cod.model_version_type cmt
                ON cmt.model_version_type_id = cmv.model_version_type_id
                WHERE model_version_id = {}'''.format(self.model_version_id)

        if self.db_connection is not None:
            self.metadata_df = db_connect.query(call, self.db_connection)
        elif self.conn_def is not None:
            self.metadata_df = query(call, conn_def=self.conn_def)
        else:
            raise RuntimeError("Must pass either a connection definition or db connection.")

        self.acause = self.metadata_df['acause'].iloc[0]
        self.cause_id = self.metadata_df['cause_id'].iloc[0]
        self.age_start = self.metadata_df['age_start'].iloc[0]
        self.age_end = self.metadata_df['age_end'].iloc[0]
        self.model_version_type_id = self.metadata_df['model_version_type_id'].iloc[0]
        self.decomp_step_id = self.metadata_df['decomp_step_id'].iloc[0]
        self.model_version_type = ''.join(self.metadata_df['model_version_type'].iloc[0].split()).lower()

        self.model_dir = 'FILEPATH'.format(self.acause, self.model_version_id)

        self.name = 'cod_{}_{}_{}'.format(self.model_version_id, self.model_version_type, self.acause)

        if not os.path.exists(self.model_dir):
            os.makedirs(self.model_dir)
            subprocess.call('chmod 777 -R {}'.format('/'.join(self.model_dir.split('/'))), shell=True)
        return self

    def run_parameters(self, hybridizer, start_date, end_date, max_cores=56,
                       max_seconds=604800):
        if self.parameter_dict is None:
            self.parameters = get_parameters(cause_id=self.cause_id, age_start=self.age_start,
                                             age_end=self.age_end, model_version_type_id=self.model_version_type_id,
                                             hybridizer=hybridizer,
                                             start_date=start_date, end_date=end_date)
        else:
            self.parameters = \
                self.parameter_dict[self.model_version_type_id][self.age_start][self.age_end]

        # Add some padding for hybrid jobs -- 1 hr and 10 gigs
        if hybridizer:
            self.parameters['ram_gb'] = self.parameters['ram_gb'] + 10
            self.parameters['runtime_min'] = self.parameters['runtime_min'] + 60

        self.parameters['m_mem_free'] = round(self.parameters['ram_gb'])
        self.parameters['num_cores'] = min(self.parameters['cores_requested'], max_cores)
        self.parameters['max_runtime_seconds'] = min(self.parameters['runtime_min']*60,
                                                     max_seconds)
        logger.info(f"Max runtime seconds is {self.parameters['max_runtime_seconds']}")
        # If the run-time is longer than 2.5 days, put it in the long queue
        if hybridizer:
            self.parameters['queue'] = 'all.q'
            self.parameters['max_runtime_seconds'] = min(self.parameters['max_runtime_seconds'], 60*60*24*3)
        else:
            if self.parameters['max_runtime_seconds'] / (60*60*24) > 3:
                self.parameters['queue'] = 'long.q'
            else:
                self.parameters['queue'] = 'all.q'
            return self

    def setup_task(self, command, **kwargs):
        """
        Command to run -- took this out of the init because sometimes we need model metadata to determine
        which parameters to pass to the command.
        :param command: (str) the model launch command
        """
        memory = str(self.parameters['m_mem_free']) + 'GB'
        max_runtime_seconds = self.parameters['max_runtime_seconds']
        queue = self.parameters['queue']
        logger.info(f"Task for model version {self.model_version_id} in queue {queue}, {max_runtime_seconds} seconds, and {memory}")
        super().__init__(command=command,
                         name=self.name,
                         m_mem_free=memory,
                         num_cores=self.cores,
                         max_runtime_seconds=max_runtime_seconds,
                         queue=queue,
                         env_variables={'OMP_NUM_THREADS': 1, 'MKL_NUM_THREADS': 1},
                         max_attempts=self.max_attempts,
                         upstream_tasks=self.upstream_tasks,
                         **kwargs)
        return self


class CODEmTask(CODEmBaseTask):
    def __init__(self, model_version_id, db_connection,
                 debug_mode=0, parameter_dict=None,
                 gb=10, minutes=60*24*10,
                 max_attempts=5, upstream_tasks=None,
                 cores=3,
                 step_cores=None, step_gb=None, step_min=None,
                 step_resources=None):
        """
        Creates a CODEmTask. At this point, all model metadata has been uploaded
        to the database so CODEmTask is going to pull it all to seamlessly
        submit a jobmon job in a standalone workflow OR in a central run. All
        this needs to know is model_version_id and db_connection, and maybe
        slots for when we start making the slot allotment smarter.

        :param model_version_id: (int) existing model version ID
        :param db_connection: (str) database connection
        :param debug_mode: (bool) whether to run in debug mode (saves extra info)
        :param parameter_dict: (dict) dictionary of existing parameters for running on cluster
        :param gb: (float) number of GB
        :param minutes: (int) amount of minutes padding to give to the min_minutes request
        :param max_attempts: (int) maximum number of jobmon attempts
        :param step_resources: (dict) dictionary of resources to provide to the steps
        """
        super().__init__(model_version_id=model_version_id,
                         parameter_dict=parameter_dict,
                         gb=gb, minutes=minutes,
                         max_attempts=max_attempts, db_connection=db_connection, upstream_tasks=upstream_tasks,
                         hybridizer=False,
                         cores=cores)

        self.debug_mode = debug_mode
        self.old_covariates_mvid = None

        if self.metadata_df['run_covariate_selection'].iloc[0] == 0:
            self.old_covariates_mvid = self.metadata_df['previous_model_version_id'].iloc[0]
        else:
            self.old_covariates_mvid = 0
        if step_resources is None:
            self.step_resources = '{}'
        else:
            self.step_resources = step_resources

        command = "run {} {} {} -dbg {} -sr '{}'".format(self.model_version_id, self.db_connection,
                                                       self.old_covariates_mvid, self.debug_mode,
                                                       self.step_resources)
        if step_cores is not None:
            command += f' -cores {step_cores}'
        if step_gb is not None:
            command += f' -gb {step_gb}'
        if step_min is not None:
            command += f' -min {step_min}'

        self.setup_task(command=command)






