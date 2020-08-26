import sys
import os
import argparse

from gbd.constants import GBD_ROUND_ID

from epic.lib.super_squeeze import run_squeeze
from epic.util.common import name_task
from epic.util.constants import Params, DAG
from jobmon.client.swarm.workflow.python_task import PythonTask

this_file = os.path.realpath(__file__)

class SuperSqueezeFactory(object):

    def __init__(self, task_registry):
        self.task_registry = task_registry

    @staticmethod
    def get_task_name(node, location_id, year_id, sex_id):
        return name_task(node, {Params.LOCATION_ID: location_id,
                                Params.SEX_ID: sex_id,
                                Params.YEAR_ID: year_id})

    def get_task(self, node, output_dir, location_id, year_id, sex_id,
        decomp_step, n_draws, dependency_list):

        # make task
        name = self.get_task_name(node, location_id, year_id, sex_id)
        task = PythonTask(
            script=this_file,
            args=[
                "--output_dir", output_dir,
                "--location_id", str(location_id),
                "--year_id", str(year_id),
                "--sex_id", str(sex_id),
                "--decomp_step", decomp_step,
                "--n_draws", str(n_draws)
            ],
            upstream_tasks=dependency_list,
            name=name,
            num_cores=10,
            m_mem_free="100.0G",
            max_attempts=DAG.Tasks.MAX_ATTEMPTS,
            tag=DAG.Tasks.SUPER_SQUEEZE,
            queue=DAG.Tasks.QUEUE)

        return task

if __name__ == '__main__':
    ###################################
    # Parse input arguments
    ###################################
    parser = argparse.ArgumentParser()
    parser.add_argument("--output_dir", required=True, type=str)
    parser.add_argument("--location_id", required=True, type=int)
    parser.add_argument("--year_id", required=True, type=int)
    parser.add_argument("--sex_id", required=True, type=int)
    parser.add_argument("--decomp_step", required=True, type=str)
    parser.add_argument("--gbd_round_id", type=int, default=GBD_ROUND_ID)
    parser.add_argument("--n_draws", type=int, default=1000)
    args = parser.parse_args()

    run_squeeze(
        output_dir=args.output_dir,
        location_id=args.location_id,
        year_id=args.year_id,
        sex_id=args.sex_id,
        decomp_step=args.decomp_step,
        gbd_round_id=args.gbd_round_id,
        n_draws=args.n_draws)
