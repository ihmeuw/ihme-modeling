import sys
import os
import argparse
from typing import List, Union

from gbd.constants import GBD_ROUND_ID

from epic.lib import sev_split, modeled_sev_split
from epic.util.common import name_task, get_dependencies
from epic.util.constants import DAG
from jobmon.client.swarm.workflow.python_task import PythonTask

_THIS_FILE = os.path.realpath(__file__)


def parse_arguments():
    parser = argparse.ArgumentParser()

    parser.add_argument("--parent_id", type=int, required=True)
    parser.add_argument("--child_id", type=int, required=True)
    parser.add_argument("--proportion_id", type=int, required=True)
    parser.add_argument("--output_dir", type=str, required=True,
                        help="root directory to save stuff")
    parser.add_argument("--decomp_step", type=str, required=True,
                        help="the decomposition step for this split")
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
    parser.add_argument("--proportion_measure_id", type=int, nargs="*",
                        help="proportion measure_id(s) to adjust", default=None)
    parser.add_argument("--location_set_id", type=int,
                        help="the location_set_id to use if location_id is None",
                        default=35)
    parser.add_argument("--gbd_round_id", type=int,
                        help="id for the gbd round to adjust",
                        default=GBD_ROUND_ID)
    parser.add_argument("--n_draws", type=int,
                        help="number of draws on which to run the adjustment",
                        default=1000)

    return parser.parse_args()


class ModeledSevSplitTaskFactory(object):

    def __init__(self, task_registry):
        self.task_registry = task_registry

    @staticmethod
    def get_task_name(node: str) -> str:
        return name_task(node, {})

    def get_task(
        self,
        node:str,
        process_graph,
        parent_id: int,
        child_id: int,
        proportion_id: int,
        output_dir: str,
        gbd_round_id: int,
        decomp_step: str,
        year_id: Union[int, List[int]],
        n_draws: int
    ) -> PythonTask:

        dep_list = get_dependencies(node, process_graph, self.task_registry)
        # make task
        name = self.get_task_name(node)
        task = PythonTask(
            script=_THIS_FILE,
            args=[
                "--parent_id", parent_id,
                "--child_id", child_id,
                "--proportion_id", proportion_id,
                "--output_dir", output_dir,
                "--gbd_round_id", gbd_round_id,
                "--decomp_step", decomp_step,
                "--year_id", " ".join([str(x) for x in year_id]),
                "--n_draws", n_draws
            ],
            upstream_tasks=dep_list,
            name=name,
            num_cores=25,
            m_mem_free="200.0G",
            max_attempts=DAG.Tasks.MAX_ATTEMPTS,
            tag=DAG.Tasks.MODELED_PROPORTION,
            queue=DAG.Tasks.QUEUE
        )
        return task


if __name__ == "__main__":
    """Entry point to call modeled_sev_split.ModeledSevSplitter from modeled_sev_split.py"""
    args = parse_arguments()
    splitter = modeled_sev_split.ModeledSevSplitter(
        parent_id=args.parent_id,
        child_id=args.child_id,
        proportion_id=args.proportion_id,
        output_dir=args.output_dir,
        decomp_step=args.decomp_step,
        location_id=args.location_id,
        year_id=args.year_id,
        age_group_id=args.age_group_id,
        sex_id=args.sex_id,
        measure_id=args.measure_id,
        proportion_measure_id=args.proportion_measure_id,
        location_set_id=args.location_set_id,
        gbd_round_id=args.gbd_round_id,
        n_draws=args.n_draws
    )
    splitter.run_all_splits_mp()