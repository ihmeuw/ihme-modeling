import argparse
import os

from gbd.constants import GBD_ROUND_ID
from jobmon.client.swarm.workflow.python_task import PythonTask

from epic.lib.super_squeeze import run_squeeze
from epic.util.common import name_task
from epic.util.constants import Params, DAG

this_file = os.path.realpath(__file__)


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("--output_dir", required=True, type=str,
                        help="output directory to save results to")
    parser.add_argument("--location_id", required=True, type=int,
                        help="location ID of results")
    parser.add_argument("--year_id", required=True, type=int,
                        help="year ID of results")
    parser.add_argument("--sex_id", required=True, type=int,
                        help="sex ID of results")
    parser.add_argument("--gbd_round_id", type=int, default=GBD_ROUND_ID,
                        help="GBD round ID of results")
    parser.add_argument("--decomp_step", type=str, default="iterative",
                        help="Decomp step of results")
    parser.add_argument("--n_draws", type=int, default=1000,
                        help="number of draws in results")
    args = parser.parse_args()

    return args


class SuperSqueezeFactory(object):

    def __init__(self, task_registry):
        self.task_registry = task_registry

    @staticmethod
    def get_task_name(node, location_id, year_id, sex_id):
        return name_task(node, {Params.LOCATION_ID: location_id,
                                Params.SEX_ID: sex_id,
                                Params.YEAR_ID: year_id})

    def get_task(self, node, output_dir, location_id, year_id, sex_id,
                 gbd_round_id, decomp_step, n_draws, dependency_list):

        # make task
        name = self.get_task_name(node, location_id, year_id, sex_id)
        task = PythonTask(
            script=this_file,
            args=[
                "--output_dir", output_dir,
                "--location_id", str(location_id),
                "--year_id", str(year_id),
                "--sex_id", str(sex_id),
                "--gbd_round_id", gbd_round_id,
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
    """Entry point to call run_squeeze from super_squeeze.py"""
    args = parse_arguments()
    run_squeeze(
        output_dir=args.output_dir,
        location_id=args.location_id,
        year_id=args.year_id,
        sex_id=args.sex_id,
        gbd_round_id=args.gbd_round_id,
        n_draws=args.n_draws
    )
