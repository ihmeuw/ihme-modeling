import os
import logging

from jobmon.workflow.python_task import PythonTask
from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.workflow import Workflow

import gbd.constants as GBD


logger = logging.getLogger(__name__)


class ImportedCasesJobSwarm(object):
    ADDITIONAL_RESTRICTIONS = {562: 'mental_drug_opioids'}
    """This class creates and submits the imported cases task dag."""
    def __init__(self, code_dir, out_dir, version_id, cause_ids,
                 gbd_round_id=GBD.GBD_ROUND_ID):
        self.code_dir = code_dir
        self.out_dir = out_dir
        self.version_id = version_id
        self.cause_ids = cause_ids
        self.gbd_round_id = gbd_round_id

        self.task_dag = TaskDag(
            name='imported_cases_v{}'.format(self.version_id))
        self.stderr = 'FILEPATH'
        self.stdout = 'FILEPATH'

    def create_imported_cases_jobs(self):
        """Generates the tasks and adds them to the task_dag."""
        slots = 38
        memory = slots * 2
        for cause in self.cause_ids:
            task = PythonTask(
                script=os.path.join(self.code_dir, 'imported_cases.py'),
                args=[self.version_id,
                      '--cause_id', cause,
                      '--gbd_round_id', self.gbd_round_id,
                      '--output_dir', self.out_dir],
                name='imported_cases_{}_{}'.format(self.version_id, cause),
                slots=slots,
                mem_free=memory,
                max_attempts=3,
                tag='imported_cases')
            self.task_dag.add_task(task)

    def run(self):
        wf = Workflow(self.task_dag,
                      'imported_cases_v{}'.format(self.version_id),
                      stderr=self.stderr, stdout=self.stdout,
                      project='proj_codcorrect')
        success = wf.run()
        return success
