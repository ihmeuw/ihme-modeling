import os
import datetime
import logging

from jobmon.client.swarm.workflow.python_task import PythonTask
from jobmon.client.swarm.workflow.workflow import Workflow

import gbd.constants as GBD
import getpass

logger = logging.getLogger(__name__)


class ImportedCasesJobSwarm(object):
    ADDITIONAL_RESTRICTIONS = {562: 'mental_drug_opioids'}
    """This class creates and submits the imported cases task dag."""
    def __init__(
            self,
            code_dir,
            out_dir,
            version_id,
            cause_ids,
            decomp_step,
            gbd_round_id=GBD.GBD_ROUND_ID
    ):
        self.code_dir = code_dir
        self.out_dir = out_dir
        self.version_id = version_id
        self.cause_ids = cause_ids
        self.decomp_step = decomp_step
        self.gbd_round_id = gbd_round_id

        username = getpass.getuser()
        self.workflow = Workflow(
            workflow_args='imported_cases_v{version}_{timestamp}'.format(
                version=self.version_id,
                timestamp=datetime.datetime.now().isoformat()
            ),
            name="Imported Cases Generator",
            project='proj_codcorrect',
            stdout=f'FILEPATH',
            stderr=f'FILEPATH'
        )

    def create_imported_cases_jobs(self):
        """Generates the tasks and adds them to the task_dag."""
        # TODO: profile and revise core/mem allocation.
        for cause in self.cause_ids:
            task = PythonTask(
                script=os.path.join(self.code_dir, 'imported_cases.py'),
                args=[self.version_id,
                      '--cause_id', cause,
                      '--decomp_step', self.decomp_step,
                      '--gbd_round_id', self.gbd_round_id,
                      '--output_dir', self.out_dir],
                name='imported_cases_{}_{}'.format(self.version_id, cause),
                num_cores=42,
                m_mem_free="100.0G",
                max_attempts=3,
                tag='imported_cases',
                queue='all.q')
            self.workflow.add_task(task)

    def run(self):
        success = self.workflow.run()
        return success
