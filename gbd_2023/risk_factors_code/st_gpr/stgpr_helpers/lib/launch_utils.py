import logging
import os
import os.path
import subprocess  # noqa: S404
from typing import Optional

import stgpr_schema

from stgpr_helpers.lib import file_utils
from stgpr_helpers.lib.constants import paths


def launch_model(
    stgpr_version_id: int,
    project: str,
    path_to_python_shell: str,
    path_to_main_script: str,
    log_path: Optional[str],
    nparallel: int,
) -> None:
    """Submits an ST-GPR model to the cluster.

    If log_path is given, creates the directory if it doesn't already exist.

    Note that we pass through only environment variables that start with STGPR_. This means we
    lose the flexibility that comes from propagating all environment variables, but it solves
    an issue where models can't launch from GPR Viz due to the differing environments when
    jobs are submitted via sbatch on the cluster vs. through the Slurm REST API.
    (SLURM_ environment variables are always propagated:
    https://slurm.schedmd.com/sbatch.html#OPT_export)
    """
    if log_path and (log_path.startswith("FILEPATH") or log_path.startswith("FILEPATH")):
        raise ValueError(f"Cannot store logs at {log_path}: storing logs on J is not allowed")

    settings = stgpr_schema.get_settings()
    stgpr_paths = paths.StgprPaths(stgpr_version_id)
    is_standard_workflow_log_path = log_path is None
    if not is_standard_workflow_log_path and not os.path.exists(log_path):
        try:
            logging.info(f"Creating directory for logs at {log_path}")
            file_utility = file_utils.StgprFileUtility(stgpr_version_id)
            file_utility.make_logs_directory(log_path)
        except PermissionError as error:
            raise RuntimeError(
                f"Do not have permissions to create log directory at {log_path}"
            ) from error

    model_error_log_directory = log_path or stgpr_paths.ERROR_LOG_PATH
    model_output_log_directory = log_path or stgpr_paths.OUTPUT_LOG_PATH
    launch_log_directory = log_path or settings.mnt_cc_output_root_format.format(
        stgpr_version_id=stgpr_version_id
    )
    launch_error_log_path = os.path.join(launch_log_directory, "model_launch.error.log")
    launch_output_log_path = os.path.join(launch_log_directory, "model_launch.output.log")
    logging.info(f"Saving model error logs to {model_error_log_directory}")
    logging.info(f"Saving model output logs to {model_output_log_directory}")
    logging.info(f"Saving launch job error logs to {launch_error_log_path}")
    logging.info(f"Saving launch job output logs to {launch_output_log_path}")

    args = " ".join(
        [
            path_to_main_script,
            str(stgpr_version_id),
            model_error_log_directory,
            model_output_log_directory,
            project,
            str(nparallel),
            str(is_standard_workflow_log_path),
        ]
    )
    stgpr_env_vars = ",".join(
        env_var for env_var in os.environ.keys() if env_var.startswith("STGPR_")
    )
    command = (
        f"sbatch --export={stgpr_env_vars} -J MAIN_{stgpr_version_id} --mem=2G -c 3 "
        f"-t 48:00:00 -A {project} -p all.q -o {launch_output_log_path} "
        f"-e {launch_error_log_path} {path_to_python_shell} {settings.conda_env_path} {args} "
    )
    logging.info(f"Submitting main ST-GPR job for run ID {stgpr_version_id}")
    try:
        subprocess.run(command, shell=True, check=True, capture_output=True)  # noqa: S602
    except subprocess.CalledProcessError as error:
        raise RuntimeError(
            f"Submission failed with exit status {error.returncode} and output "
            f'{error.stderr.decode("utf-8")}'
        ) from error
    logging.info("Job submitted successfully")
