from datetime import datetime
import logging
import os

from jobmon.client.swarm.workflow.workflow import Workflow
from jobmon.client.swarm.workflow.python_task import PythonTask


logger = logging.getLogger(__name__)


class SplitCoDSwarm(object):

    _CODEDIR = os.path.dirname(os.path.abspath(__file__))

    def __init__(self, source_id, proportion_ids, proportion_measure_id,
                 sex_ids, gbd_round_id, decomp_step, intermediate_dir, outdir,
                 project):
        self.source_id = source_id
        self.proportion_ids = proportion_ids
        self.proportion_measure_id = proportion_measure_id
        self.sex_ids = sex_ids
        self.gbd_round_id = gbd_round_id
        self.decomp_step = decomp_step
        self.intermediate_dir = intermediate_dir
        self.outdir = outdir

        time = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')

        self.workflow = Workflow(
            workflow_args=('split_cod_model_interpolate_{}_{}'
                           .format(source_id, time)),
            name='Split CoD Model cause_id: {}'.format(source_id),
            project=project,
            stderr=outdir,
            stdout=outdir
        )

    def add_interpolate_tasks(self):
        for meid in self.proportion_ids:
            for sex in self.sex_ids:
                arglist = [
                    '--gbd_id', meid,
                    '--proportion_measure_id', self.proportion_measure_id,
                    '--sex_id', sex,
                    '--gbd_round_id', self.gbd_round_id,
                    '--intermediate_dir', self.intermediate_dir]
                if self.decomp_step:
                    arglist.extend(['--decomp_step', self.decomp_step])

                task = PythonTask(
                    script=os.path.join(self._CODEDIR, 'split_interp.py'),
                    args=arglist,
                    name='split_model_interpolate_{}_{}'.format(meid, sex),
                    num_cores=30,
                    m_mem_free='60G',
                    max_runtime_seconds=14400,
                    max_attempts=10
                )
                self.workflow.add_task(task)

    def run(self):
        return self.workflow.run()

