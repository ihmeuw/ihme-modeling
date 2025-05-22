import subprocess
import sys
from pathlib import Path

import fire
from pplkit.data.interface import DataInterface

from mortest.actions import ACTION_ARGNAMES

STAGES = [
    "splice_patterns",
    "fit_kreg",
    "calibrate_uncertainty",
    "prepare_handoff",
    "create_cmp_figures",
]

CREATE_FIGURES_X = "[year_id,age_mid]"


def get_command(
    action: str,
    settings: dict,
    task_args: dict | None = None,
    node_args: dict | None = None,
) -> str:
    task_arg_names = ACTION_ARGNAMES[action]["task_args"]
    node_arg_names = ACTION_ARGNAMES[action]["node_args"]
    command_template = (
        "{python} {script}"
        + "".join(f" --{arg} {{{arg}}}" for arg in task_arg_names)
        + "".join(f" --{arg} {{{arg}}}" for arg in node_arg_names)
    )

    python = sys.executable
    script = str(Path(__file__).parent / "actions" / f"{action}.py")

    arg_map = {**(task_args or {}), **(node_args or {})}
    for arg_name in task_arg_names + node_arg_names:
        if arg_name not in arg_map:
            arg_map[arg_name] = settings[arg_name]

    command = command_template.format(python=python, script=script, **arg_map)
    return command


def build_stage_commands(
    stage: str, settings: dict, commands: list[str] | None
) -> list[str]:
    commands = commands or []

    if stage in [
        "splice_patterns",
        "calibrate_uncertainty",
        "prepare_handoff",
        "create_cmp_figures",
    ]:
        commands.append(get_command(stage, settings))
        return commands

    if stage in ["fit_kreg"]:
        commands.append(get_command(stage, settings))
        commands.append(
            get_command(
                "combine_data",
                settings,
                node_args=dict(dirpath="location_model/predictions"),
            )
        )
        return commands

    raise ValueError(f"Stage {stage} not recognized.")


def build_command_list(stages: list[str], settings: dict) -> list[str]:
    commands = []
    for stage in stages:
        commands = build_stage_commands(stage, settings, commands)
    return commands


def main(
    directory: str,
    sex_id: int,
    location_id: int,
    start_from: str = "splice_patterns",
    end_on: str = "create_cmp_figures",
) -> None:
    dataif = DataInterface(directory=directory)
    dataif.add_dir("config", dataif.directory / "config")

    settings = dataif.load_config("settings.yaml")

    loc_meta = (
        dataif.load(settings["loc_meta"])
        .set_index("location_id")
        .loc[location_id]
    )

    settings["directory"] = directory
    settings["sex_id"] = sex_id
    settings["location_id"] = location_id
    settings["national_id"] = loc_meta["national_id"]
    settings["x"] = CREATE_FIGURES_X
    settings["scaled"] = "[True,False]"

    index_0 = STAGES.index(start_from)
    index_1 = STAGES.index(end_on) + 1
    commands = build_command_list(STAGES[index_0:index_1], settings)

    for command in commands:
        print(command)
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError as error:
            raise RuntimeError(error.output)


if __name__ == "__main__":
    fire.Fire(main)
