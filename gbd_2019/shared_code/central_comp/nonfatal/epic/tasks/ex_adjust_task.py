import sys
import os
import argparse

from gbd.constants import GBD_ROUND_ID

from epic.lib import ex_adjust
from epic.util.common import name_task, get_dependencies
from epic.util.constants import DAG
from jobmon.client.swarm.workflow.python_task import PythonTask

this_file = os.path.realpath(__file__)

class ExAdjustFactory(object):

    def __init__(self, task_registry):
        self.task_registry = task_registry

    @staticmethod
    def get_task_name(node):
        # The exclusivity adjustment tasks are not parallelized by
        # demographics so pass an empty dictionary
        return name_task(node, {})

    def get_task(self, node, process_graph, output_dir, decomp_step,
            year_id, n_draws):

        dep_list = get_dependencies(node, process_graph, self.task_registry)
        # make task
        name = self.get_task_name(node)
        task =  PythonTask(
            script=this_file,
            args=[
                "--process_name", node,
                "--output_dir", output_dir,
                "--decomp_step", decomp_step,
                "--year_id", " ".join([str(x) for x in year_id]),
                "--n_draws", str(n_draws)
            ],
            upstream_tasks=dep_list,
            name=name,
            num_cores=25,
            m_mem_free="300.0G",
            max_attempts=DAG.Tasks.MAX_ATTEMPTS,
            tag=DAG.Tasks.EX_ADJUST,
            queue=DAG.Tasks.QUEUE)

        return task



if __name__ == "__main__":

    # parse command line args
    parser = argparse.ArgumentParser()
    parser.add_argument("--process_name", help="process to pull out of json"
        " style string map of ops", required=True)
    parser.add_argument("--output_dir", help="root directory to save stuff",
                        required=True)
    parser.add_argument("--decomp_step", help="the decomposition step for this"
        " adjustment", required=True)
    parser.add_argument("--location_id", type=int, nargs="*",
        help="location(s) to adjust", default=None)
    parser.add_argument("--year_id", type=int, nargs="*",
        help="year(s) to adjust", default=None)
    parser.add_argument("--age_group_id", type=int, nargs="*",
        help="age group(s) to adjust", default=None)
    parser.add_argument("--sex_id", type=int, nargs="*",
        help="sex(es) to adjust", default=None)
    parser.add_argument("--measure_id", type=int, nargs="*",
        help="measure_id(s) to adjust", default=None)
    parser.add_argument("--location_set_id", type=int,
        help="the location_set_id to use if location_id is None", default=35)
    parser.add_argument("--gbd_round_id", type=int,
        help="id for the gbd round to adjust", default=GBD_ROUND_ID)
    parser.add_argument("--n_draws", type=int,
        help="number of draws on which to run the adjustment", default=1000)
    args = parser.parse_args()


    adjuster = ex_adjust.ExAdjust(
        process_name=args.process_name,
        output_dir=args.output_dir,
        decomp_step=args.decomp_step,
        location_id=args.location_id,
        year_id=args.year_id,
        age_group_id=args.age_group_id,
        sex_id=args.sex_id,
        measure_id=args.measure_id,
        location_set_id=args.location_set_id,
        gbd_round_id=args.gbd_round_id,
        n_draws=args.n_draws)
    adjuster.run_all_adjustments_mp()
