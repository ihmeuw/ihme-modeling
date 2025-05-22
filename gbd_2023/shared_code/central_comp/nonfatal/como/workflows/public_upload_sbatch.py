import os
import subprocess
from pathlib import Path

import click

LAUNCH_SCRIPT_TEMPLATE = """#!/bin/bash
#SBATCH -J como_public_upload_launcher
#SBATCH -A proj_como
#SBATCH -p long.q
#SBATCH -c 1
#SBATCH --mem=5GB
#SBATCH -t 168:00:00
#SBATCH -o {como_dir}/public_upload_launcher.out
#SBATCH -e {como_dir}/public_upload_launcher.err

source {conda_install}/bin/activate
conda run -p {conda_env} public_upload --como_dir {como_dir}"""


@click.command
@click.option(
    "como_dir",
    "--como_dir",
    type=str,
    required=True,
    help="Root directory of a COMO run where summaries already exist.",
)
@click.option("--resume", is_flag=True, help="whether to resume the workflow.")
@click.option(
    "--create_tables",
    is_flag=True,
    help=(
        "whether to create the tables in the public upload host."
        "Ignored if --resume is set."
    ),
)
@click.option("--no_slack", is_flag=True, help="whether to post to slack.")
def sbatch_public_upload(
    como_dir: str, resume: bool, create_tables: bool, no_slack: bool
) -> None:
    """Launch the public upload workflow via sbatch."""
    sub_call = subprocess.run(
        ["conda", "info", "--base"], capture_output=True
    )  # nosec: B603, B607
    conda_install = sub_call.stdout.decode().strip()
    conda_env = os.environ["CONDA_PREFIX"]

    launch_script = Path(como_dir) / "public_upload_launch.sh"
    launch_script_content = LAUNCH_SCRIPT_TEMPLATE.format(
        como_dir=como_dir, conda_install=conda_install, conda_env=conda_env
    )
    if resume:
        launch_script_content += " --resume"
    if create_tables:
        launch_script_content += " --create_tables"
    if no_slack:
        launch_script_content += " --no_slack"

    with open(launch_script, "w") as fp:
        fp.write(launch_script_content + "\n")

    # start a small sbatch that launches the workflow
    subprocess.run(["sbatch", str(launch_script)])  # nosec: B603, B607


if __name__ == "__main__":
    sbatch_public_upload()
