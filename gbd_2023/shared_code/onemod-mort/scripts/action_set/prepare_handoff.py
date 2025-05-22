"""This is a combination action with
- combine_data
- calibrate_uncertainty
- prepare_handoff

"""

import subprocess
import sys
from pathlib import Path

import fire
from pplkit.data.interface import DataInterface

from mortest.actions import ACTION_ARGNAMES


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
    script = str(Path(__file__).parent.parent / "actions" / f"{action}.py")

    arg_map = {**(task_args or {}), **(node_args or {})}
    for arg_name in task_arg_names + node_arg_names:
        if arg_name not in arg_map:
            arg_map[arg_name] = settings[arg_name]

    command = command_template.format(python=python, script=script, **arg_map)
    return command


def main(directory: str) -> None:
    dataif = DataInterface(directory=directory)
    dataif.add_dir("config", dataif.directory / "config")

    settings = dataif.load_config("settings.yaml")
    settings["directory"] = directory

    commands = [
        get_command(
            "combine_data",
            settings,
            node_args=dict(dirpath="location_model/predictions"),
        ),
        get_command("calibrate_uncertainty", settings),
        get_command("prepare_handoff", settings),
        get_command("compute_life_expectancy", settings),
        get_command("create_intercept_figures", settings),
    ]

    for command in commands:
        print(command)
        try:
            subprocess.check_call(command, shell=True)
        except subprocess.CalledProcessError as error:
            raise RuntimeError(error.output)


if __name__ == "__main__":
    fire.Fire(main)
