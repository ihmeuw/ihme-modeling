import argparse
import getpass
import logging
import shutil
from typing import List, Tuple

from gbd import conn_defs
from jobmon.core.constants import WorkflowStatus

from hybridizer.joblaunch.HybridTask import HybridTask
from hybridizer.joblaunch.HybridWorkflow import HybridWorkflow
from hybridizer.log_utilities import get_log_dir
from hybridizer.reference import paths

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


def parse_arguments() -> Tuple[List[int], List[int], str, str]:
    """
    Parses arguments from the command line, adds three parser arguments:
    global_ids, datarich_ids, and conn_def.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--global_ids", type=int, nargs="+", help="list of global model version IDs"
    )
    parser.add_argument(
        "--datarich_ids", type=int, nargs="+", help="list of data-rich model version IDs"
    )
    parser.add_argument("--conn_def", type=str, help="database connection")
    parser.add_argument("--workflow_name", type=str, help="name of workflow")

    args = parser.parse_args()
    return args.global_ids, args.datarich_ids, args.conn_def, args.workflow_name


def main(
    global_ids: List[int], datarich_ids: List[int], conn_def: str, workflow_name: str
) -> None:
    """
    Main function for file, creates a list of hybrid models and then runs them
    in a CODEm workflow.

    Args:
    global_ids (List[int]): list of global model version IDs to
        hybridize
    datarich_ids (List[int]): list of data-rich model version IDs to
        hybridize
    conn_def (str): Database connection to use for model data reading and
        writing
    workflow_name (str): name of workflow
    """
    if len(global_ids) != len(datarich_ids):
        raise ValueError(
            f"global and data-rich model version ID lists must be the same "
            f"length, given global list length={len(global_ids)} "
            f"and data-rich list length={len(datarich_ids)}"
        )
    global_ids = [int(glb) for glb in global_ids]
    datarich_ids = [int(dev) for dev in datarich_ids]
    valid_connections = [conn_defs.CODEM, conn_defs.CODEM_TEST]
    if conn_def not in valid_connections:
        raise RuntimeError(
            f"Need to pass a valid database connection! Valid database "
            f"connections are {', '.join([c for c in valid_connections])}"
        )

    user = getpass.getuser()
    hybrid_wf = HybridWorkflow(name=workflow_name, description=workflow_name, user=user)
    wf = hybrid_wf.get_workflow()
    model_pairs = list(zip(global_ids, datarich_ids))
    tasks = []
    log_dirs = []
    logger.info("Making Hybrid tasks")
    for model_pair in model_pairs:
        hybrid_task = HybridTask(
            user=user,
            global_model_version_id=model_pair[0],
            datarich_model_version_id=model_pair[1],
            conn_def=conn_def,
        ).get_task()
        tasks.append(hybrid_task)

        base_dir = paths.get_base_dir(
            model_version_id=hybrid_task.model_version_id, conn_def=conn_def
        )
        log_dir = get_log_dir(base_dir)
        log_dirs.append(log_dir)
    logger.info(
        f"All hybrid tasks have been made, hybrid model version IDs "
        f"are {', '.join([str(task.model_version_id) for task in tasks])}"
    )

    wf.add_tasks(tasks)
    logger.info(f"Running hybrid workflow, name of workflow is: {workflow_name}")
    exit_status = wf.run()
    if exit_status != WorkflowStatus.DONE:
        logger.info(
            f"The workflow failed, returning exit status {exit_status}, "
            f"see workflow logs {hybrid_wf.stderr}."
        )
    else:
        logger.info("The workflow successfully completed!")
        shutil.rmtree(hybrid_wf.stderr)


if __name__ == "__main__":
    """
    Unpacks arguments from the command line that are passed to script.
    *parse_arguments() is unpacked as a triple at runtime
    """
    main(*parse_arguments())
