import logging
import json

from jobmon.client.swarm.workflow.bash_task import BashTask
from codem.metadata.step_metadata import STEP_DICTIONARY, STEP_NAMES
from codem.data.parameters import get_model_parameters
from codem.joblaunch.resource_predictions import get_step_prediction
from codem.joblaunch.resource_predictions import import_resource_parameters

logger = logging.getLogger(__name__)


class StepTaskGenerator:
    def __init__(self, model_version_id, db_connection,
                 old_covariates_mvid, debug_mode, additional_resources=None):
        """
        Generates a CODEm Step that subclasses BashTask
        for a given model version ID so we don't have to constantly
        pass those arguments.

        :param model_version_id:
        :param db_connection:
        :param old_covariates_mvid:
        :param debug_mode:
        :param additional_resources: dictionary of more resources to use, by step
        """
        self.model_version_id = model_version_id
        self.db_connection = db_connection
        self.old_covariates_mvid = old_covariates_mvid
        self.debug_mode = debug_mode
        self.resource_prediction_data = import_resource_parameters()
        if additional_resources is None:
            additional_resources = {}
        self.additional_resources = additional_resources

    def generate(self, step_id, inputs_info, additional_args=None, **kwargs):
        parameters = get_step_prediction(
            resource_parameters=self.resource_prediction_data,
            step_id=step_id,
            input_info=inputs_info
        )
        if str(step_id) in self.additional_resources:
            if 'max_runtime_seconds' in self.additional_resources[str(step_id)]:
                logger.info("Overriding max runtime seconds.")
                parameters['max_runtime_seconds'] = int(
                    self.additional_resources[str(step_id)]['max_runtime_seconds']
                )
            if 'm_mem_free' in self.additional_resources[str(step_id)]:
                logger.info("Overriding memory.")
                parameters['m_mem_free'] = str(
                    self.additional_resources[str(step_id)]['m_mem_free']
                )
            if 'num_cores' in self.additional_resources[str(step_id)]:
                logger.info("Overriding cores.")
                parameters['num_cores'] = int(
                    self.additional_resources[str(step_id)]['num_cores']
                )
        return CODEmStep(
            model_version_id=self.model_version_id,
            db_connection=self.db_connection,
            old_covariates_mvid=self.old_covariates_mvid,
            debug_mode=self.debug_mode,
            step_id=step_id,
            inputs_info=inputs_info,
            additional_args=additional_args,
            num_cores=parameters['num_cores'],
            max_runtime_seconds=parameters['max_runtime_seconds'],
            m_mem_free=parameters['m_mem_free'],
            queue=parameters['queue'],
            **kwargs
        )


class CODEmStep(BashTask):
    def __init__(self, model_version_id, db_connection,
                 old_covariates_mvid, debug_mode, step_id,
                 inputs_info, retries=10,
                 additional_args=None,
                 upstream_tasks=None,
                 max_runtime_seconds=None,
                 m_mem_free=None, num_cores=None,
                 queue='all.q',
                 **kwargs):
        """
        CODEm Step Task used to set up the tasks in the
        workflow.

        :param model_version_id:
        :param db_connection:
        :param old_covariates_mvid:
        :param debug_mode:
        :param step_id:
        :param retries:
        :param inputs_info: information about the inputs of the model
        :param additional_args: dict of more keyword arguments that we can
               pass to the model tasks
        :param upstream_tasks: upstream jobmon tasks
        """
        logger.info(f"Creating a step {step_id} BashTask for {model_version_id}.")
        self.model_version_id = model_version_id
        self.db_connection = db_connection
        self.old_covariates_mvid = old_covariates_mvid
        self.debug_mode = debug_mode
        self.step_id = step_id
        self.additional_args = additional_args
        self.retries = retries
        self.upstream_tasks = upstream_tasks
        self.inputs_info = inputs_info
        self.m_mem_free = m_mem_free
        self.max_runtime_seconds = max_runtime_seconds
        self.num_cores = num_cores
        self.queue = queue

        self.step_name = STEP_NAMES[self.step_id]
        self.step_metadata = STEP_DICTIONARY[self.step_name]
        self.model_parameters = get_model_parameters(
            model_version_id=self.model_version_id,
            db_connection=self.db_connection,
            update=False
        )

        self.job_name = self.get_job_name()
        self.command = self.generate_command()
        self.json = json.dumps(self.inputs_info)
        logger.info(f"The attributes json is {self.json}")
        super().__init__(
            command=self.command,
            name=self.job_name,
            m_mem_free=self.m_mem_free,
            num_cores=self.num_cores,
            max_runtime_seconds=self.max_runtime_seconds,
            queue=self.queue,
            env_variables={'OMP_NUM_THREADS': 1, 'MKL_NUM_THREADS': 1},
            max_attempts=self.retries,
            upstream_tasks=self.upstream_tasks,
            job_attributes={8: json.dumps(self.inputs_info)},
            **kwargs
        )

    def get_job_name(self):
        """
        The job name that will be used.
        """
        return (
            f'cod_{self.model_version_id}'
            f'_{self.model_parameters["acause"]}'
            f'_{self.model_parameters["model_version_type"].lower().replace(" ", "_")}'
            f'_{self.step_id:02d}'
        )

    def generate_command(self):
        """
        Generate the command that will be run by jobmon. Calls an executable
        that lives in codem.work_exec. ... because they are installed as
        entry_points in setup.py.
        """
        command = (
            f'{self.step_name} '
            f'-model-version-id {self.model_version_id} '
            f'-db-connection {self.db_connection} '
            f'-old-covariates-mvid {self.old_covariates_mvid} '
            f'-debug-mode {self.debug_mode} '
            f'-cores {self.num_cores} '
        )
        if self.additional_args is not None:
            for k, v in self.additional_args.items():
                command += f'--{k} {v}'
        return command
