"""Run OneMod pipeline."""

import fire
from jobmon.client.task import Task
from jobmon.client.tool import Tool
from pplkit.data.interface import DataInterface

from mortest.job_manager import add_ids, create_action_tasks, run_workflow

STAGES = [
    "splice_patterns",
    "fit_kreg",
    "prepare_handoff",
    "create_cmp_figures",
]


def main(directory: str, stages: list = STAGES) -> None:
    """Run OneMod pipeline."""
    # Setup
    dataif = DataInterface(directory=directory)
    dataif.add_dir("config", dataif.directory / "config")

    settings = dataif.load_config("settings.yaml")
    settings["directory"] = directory
    settings["scaled"] = [True, False]
    settings["x"] = ["year_id", "age_mid"]
    settings = add_ids(settings)

    resources = dataif.load_config("resources.yaml")

    # Create workflow
    tasks, upstream_tasks = [], []
    tool = Tool(name="onemod_tool")
    workflow = tool.create_workflow(name="onemod_workflow")
    for stage in stages:
        print(stage)
        tasks, upstream_tasks = create_tasks(
            stage, settings, resources, tool, tasks, upstream_tasks
        )
    workflow.add_tasks(tasks)

    # Run workflow
    run_workflow(workflow)


def create_tasks(
    stage: str,
    settings: dict,
    resources: dict,
    tool: Tool,
    tasks: list[Task] | None = None,
    upstream_tasks: list[Task] | None = None,
) -> tuple[list[Task], list[Task]]:
    tasks = tasks or []
    upstream_tasks = upstream_tasks or []

    default_args = dict(
        stage=stage,
        action=stage,
        settings=settings,
        resources=resources,
        tool=tool,
        upstream_tasks=upstream_tasks,
    )

    if stage in ["splice_patterns", "fit_kreg"]:
        action_tasks = create_action_tasks(**default_args)
        tasks.extend(action_tasks)
        upstream_tasks = action_tasks
        return tasks, upstream_tasks

    if stage in ["prepare_handoff"]:
        args = default_args.copy()
        args["action_dir"] = "action_set"
        action_tasks = create_action_tasks(**args)
        tasks.extend(action_tasks)
        upstream_tasks = action_tasks

        args = default_args.copy()
        args["action"] = "cleanup_dir"
        args["upstream_tasks"] = upstream_tasks
        args["node_args"] = dict(
            dirpath=[f"{name}/predictions" for name in ["location_model"]]
        )
        action_tasks = create_action_tasks(**args)
        tasks.extend(action_tasks)
        return tasks, upstream_tasks

    if stage == "create_cmp_figures":
        action_tasks = create_action_tasks(**default_args)
        tasks.extend(action_tasks)
        upstream_tasks_0 = action_tasks

        args = default_args.copy()
        args["action"] = "create_life_expectancy_figures"
        action_tasks = create_action_tasks(**args)
        tasks.extend(action_tasks)
        upstream_tasks_1 = action_tasks

        args = default_args.copy()
        args["action"] = "combine_figures"
        args["upstream_tasks"] = upstream_tasks_0 + upstream_tasks_1
        action_tasks = create_action_tasks(**args)
        tasks.extend(action_tasks)
        upstream_tasks = action_tasks

        args = default_args.copy()
        args["action"] = "cleanup_dir"
        args["upstream_tasks"] = upstream_tasks
        args["node_args"] = dict(dirpath=["figures"])
        action_tasks = create_action_tasks(**args)
        tasks.extend(action_tasks)
        return tasks, upstream_tasks

    if stage == "create_draws":
        action_tasks = create_action_tasks(**default_args)
        tasks.extend(action_tasks)
        upstream_tasks = action_tasks

        args = default_args.copy()
        args["action"] = "combine_data"
        args["upstream_tasks"] = upstream_tasks
        args["node_args"] = dict(dirpath=["location_model/draws"])
        action_tasks = create_action_tasks(**args)
        tasks.extend(action_tasks)
        upstream_tasks = action_tasks

        args = default_args.copy()
        args["action"] = "cleanup_dir"
        args["upstream_tasks"] = upstream_tasks
        args["node_args"] = dict(dirpath=["location_model/draws"])
        action_tasks = create_action_tasks(**args)
        tasks.extend(action_tasks)
        return tasks, upstream_tasks

    raise ValueError(f"Stage {stage} not recognized.")


if __name__ == "__main__":
    fire.Fire(main)
