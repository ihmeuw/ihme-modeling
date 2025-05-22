import sys
from pathlib import Path

import pandas as pd
from jobmon.client.task import Task
from jobmon.client.tool import Tool
from jobmon.client.workflow import Workflow

from mortest.actions import ACTION_ARGNAMES


def create_action_tasks(
    stage: str,
    action: str,
    settings: dict,
    resources: dict,
    tool: Tool,
    upstream_tasks: list[Task],
    action_dir: str = "actions",
    task_args: dict | None = None,
    node_args: dict | None = None,
) -> list[Task]:
    task_arg_names = ACTION_ARGNAMES[action]["task_args"]
    node_arg_names = ACTION_ARGNAMES[action]["node_args"]
    command_template = (
        "{python} {script}"
        + "".join(f" --{arg} {{{arg}}}" for arg in task_arg_names)
        + "".join(f" --{arg} {{{arg}}}" for arg in node_arg_names)
    )
    task_template = tool.get_task_template(
        template_name=f"{stage}_{action}_template",
        default_compute_resources=get_resources(resources, action),
        default_cluster_name="slurm",
        command_template=command_template,
        op_args=["python", "script"],
        task_args=task_arg_names,
        node_args=node_arg_names,
    )

    python = sys.executable
    action_dir = Path(__file__).parent.parent.parent / "scripts" / action_dir

    # parse task_args and node_args, if not provided look up in settings
    # and we convert all arguments to lists
    arg_map = {**(task_args or {}), **(node_args or {})}
    for arg_name in task_arg_names + node_arg_names:
        if arg_name not in arg_map:
            arg_map[arg_name] = settings[arg_name]

    task_builder = task_template.create_tasks
    if len(node_arg_names) == 0:
        task_builder = task_template.create_task

    tasks = task_builder(
        name=f"{stage}_{action}_task",
        max_attempts=1,
        upstream_tasks=upstream_tasks,
        python=python,
        script=str(action_dir / f"{action}.py"),
        **arg_map,
    )

    if len(node_arg_names) == 0:
        tasks = [tasks]
    return tasks


def get_resources(resources: dict, action: str) -> dict:
    """Get stage resources."""
    stage_resources = resources["default"].copy()
    if action in resources:
        stage_resources.update(resources[action])
    return stage_resources


def add_source_location_id(data: pd.DataFrame) -> pd.DataFrame:
    data["source_location_id"] = data["location_id"]
    index = data["location_id"] >= 1_000_000
    data.loc[index, "source_location_id"] = (
        data.loc[index, "location_id"].astype(str).str[:3].astype("int32")
    )
    return data


def add_ids(settings: dict) -> dict:
    """Add sexes, super regions, and locations to settings."""
    id_types = ["sex_id", "national_id", "location_id"]
    if any([id_type not in settings for id_type in id_types]):
        data = pd.read_parquet(
            settings["loc_meta"], columns=["location_id", "national_id"]
        ).merge(pd.DataFrame(dict(sex_id=[1, 2])), how="cross")

    for id_type in id_types:
        if id_type not in settings:
            if id_type != "location_id":
                settings[id_type] = data[id_type].unique().tolist()
            else:
                settings[id_type] = (
                    data.query(f"national_id in {settings['national_id']}")[
                        "location_id"
                    ]
                    .astype(int)
                    .unique()
                    .tolist()
                )
    return settings


def run_workflow(workflow: Workflow) -> None:
    # Run workflow
    workflow.bind()
    print(f"Running workflow with ID {workflow.workflow_id}.")
    print("For full information see the Jobmon GUI:")
    print(
        f"https://jobmon-gui.ihme.washington.edu/#/workflow/{workflow.workflow_id}/tasks"
    )
    status = workflow.run(fail_fast=True, configure_logging=True)
    print(f"Workflow {workflow.workflow_id} completed with status {status}.")
