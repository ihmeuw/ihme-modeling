"""
Worker script sent out by jobmon goes here
"""

import argparse
from typing import Any, Dict, Tuple

import pandas as pd
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from inpatient.Clinical_Runs.utils.config.manager import replace_nulls
from pydantic import validate_arguments

from cms.src.pipeline.api.internal import CreateCmsEstimates


def get_swarm_objects(swarm_id: int, swarm_row_id: int) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Gets the single task and task resource from the saved swarm.

    Args:
        swarm_id (int): The integer assigned to the swarm task and resource tables.
        swarm_row_id (int): Identifier to pull a specific task from
            the swarm task and tables.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: Task parameters, task resources
    """
    # Get base path to swarm builds.
    swarm_path = filepath_parser(ini="pipeline.cms", section="run_paths", section_key="swarms")

    # Get task args and worker resources for specified swarm.
    swarm_all_tasks = pd.read_csv("FILEPATH")
    resources_all_tasks = pd.read_csv("FILEPATH")
    task = swarm_all_tasks[swarm_all_tasks.index == swarm_row_id].copy()
    resources = resources_all_tasks[resources_all_tasks.index == swarm_row_id].copy()

    return task, resources


def make_args(task: pd.DataFrame) -> Dict[str, Any]:
    """Make task mappable task param dictionary to pass to CreateCmsEstimates.

    Args:
        task (pd.DataFrame): Swarm build filtered to a single row.
            Must have the column 'swarm_id'.

    Returns:
        Dict[str, Any]: param:val dictionary for the task.
    """

    # Remove key from task which doesn't map into CreateCmsEstimates.
    task = task.T.drop("swarm_id", axis=0)
    # Convert into dictionary
    task.columns = ["vals"]
    param_pack: Dict[str, Any] = task.set_index(task.index)["vals"].to_dict()
    # Replace any nulls in dict with None.
    param_pack = replace_nulls(obj=param_pack)

    return param_pack


@validate_arguments(config=dict(arbitrary_types_allowed=True))
def validate_worker_objs(task: pd.DataFrame, resources: pd.DataFrame) -> None:
    """Validates task resource and param objects are as expected and agree.

    Args:
        task (pd.DataFrame): Swarm build filtered to a single row.
        resources (pd.DataFrame): Resources for swarm filtered to a single row.

    Raises:
        RuntimeError: 'task' has more than 1 row.
        RuntimeError: 'resources' has more than 1 row.
        ValueError: Overlapping columns between 'task' and 'resources' disagree.
    """

    if len(task) != 1:
        raise RuntimeError("More than 1 task associated to swarm_row_id")
    if len(resources) != 1:
        raise RuntimeError("More than 1 task associated to swarm_row_id")

    # Each column of overlap must have the same values.
    check_cols = ["cms_system", "bundle_id", "estimate_id", "deliverable_name"]
    for col in check_cols:
        if task[col].item() != resources[col].item():
            raise ValueError(f"Task and Resources have mismatched {col} values.")


if __name__ == "__main__":
    # Gather args
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument(
        "--swarm_id", type=int, required=True, help="Numeric value representing swarm build."
    )
    arg_parser.add_argument(
        "--row", type=int, required=True, help="Row index for the worker task."
    )
    args = arg_parser.parse_args()

    # Collect and validate task params and resources.
    task, resources = get_swarm_objects(swarm_id=args.swarm_id, swarm_row_id=args.row)
    validate_worker_objs(task=task, resources=resources)

    # Make task params mappable.
    param_pack = make_args(task=task)

    # Make CMS estimate
    cms_est = CreateCmsEstimates(**param_pack)
    cms_est.threads = resources["threads"].item()
    cms_est.main()
