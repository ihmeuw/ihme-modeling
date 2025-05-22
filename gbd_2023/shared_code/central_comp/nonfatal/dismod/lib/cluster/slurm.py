import logging
import os
import pathlib
import subprocess  # nosec: B404
from functools import lru_cache
from typing import Any, Dict, List, Literal, Optional

import numpy as np


def submit(
    runfile: str,
    jobname: str,
    account: str,
    partition: str,
    threads: int,
    memory_gb: float,
    runtime: str,
    parameters: Optional[List[Any]],
    conda_env: str,
    environment_variables: Optional[Dict[str, str]],
    prepend_to_path: str,
    stderr: str,
) -> None:
    """Submits a job on slurm cluster.

    Arguments:
        runfile: Path to python script to run.
        jobname: Name of job to submit.
        account: Name of account.
        partition: Name of partition.
        threads: Number of threads to allocate.
        memory_gb: GB of memory to allocate.
        runtime: Runtime limit (e.g. '384:00:00').
        parameters: List of arguments to pass to runfile.
        conda_env: Conda environment name.
        environment_variables: Environment variables to assign to job env.
        prepend_to_path: Path to preppend to current path.
        stderr: Path to directory that will contain stderr log file, or path to log file.
    """
    if os.path.isdir(stderr):
        stderr = 

    sbatch(
        runfile=runfile,
        jobname=jobname,
        account=account,
        partition=partition,
        threads=threads,
        memory_gb=memory_gb,
        runtime=runtime,
        parameters=parameters,
        conda_env=conda_env,
        environment_variables=environment_variables,
        prepend_to_path=prepend_to_path,
        stderr=stderr,
    )


def create_compute_resources(
    cores: int, memory_gb: float, runtime: float, needs_j: bool
) -> Dict[str, Any]:
    """Create the compute_resources argument that jobmon expects.

    Arguments:
        cores: Number of cores for job.
        memory_gb: Max memory in GB.
        runtime: Max runtime in seconds.
        needs_J: Whether the job requires archive node.

    Returns:
        A dictionary of compute resources.
    """
    queue = (
        queue_from_runtime(runtime_in_secs=int(runtime)) if memory_gb <= 1000.0 else "long.q"
    )
    compute_resources = {
        "cores": cores,
        "memory": memory_gb,
        "queue": queue,
        "runtime": runtime,
        "fallback_queues": [q for q in ["long.q", "all.q"] if q != queue],
    }
    if needs_j:
        compute_resources["constraints"] = "archive"

    return compute_resources


def queue_from_runtime(runtime_in_secs: int) -> Literal["long.q", "all.q"]:
    """Return the appropriate queue based on runtime.

    Arguments:
        runtime_in_secs: Runtime in seconds.

    Returns:
        The appropriate queue name.
    """
    all_q_max = get_sec(max_run_time_on_partition("all.q"))
    if runtime_in_secs > all_q_max:
        return "long.q"
    else:
        return "all.q"


def get_sec(time_str: str) -> int:
    """Get seconds from a time string.

    Arguments:
        time_str: A time string in the format HH:MM:SS.

    Returns:
        The number of seconds.
    """
    h, m, s = time_str.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def create_sbatch_command(
    runfile: str,
    jobname: str,
    partition: str,
    parameters: Optional[List[Any]],
    account: str,
    threads: int,
    memory_gb: float,
    runtime: str,
    stdout: Optional[str] = None,
    stderr: Optional[str] = None,
    prepend_to_path: Optional[str] = None,
    conda_env: Optional[str] = None,
    environment_variables: Optional[Dict[str, str]] = None,
) -> str:
    """Create an sbatch command.

    Arguments:
        runfile: Path to python script to run.
        jobname: Name of job to submit.
        partition: Name of partition.
        parameters: List of arguments to pass to runfile.
        account: Name of account.
        threads: Number of threads to allocate.
        memory_gb: GB of memory to allocate.
        runtime: Runtime limit (e.g. '384:00:00').
        stdout: Path to directory that will contain stdout log file, or path to log file.
        stderr: Path to directory that will contain stderr log file, or path to log file.
        prepend_to_path: Path to preppend to current path.
        conda_env: Conda environment name.
        environment_variables: Environment variables to assign to job env.

    Returns:
        The sbatch command.
    """
    runfile = os.path.expanduser(runfile)
    parameters = np.atleast_1d(parameters if parameters else []).tolist()
    sbatch_args = []
    if conda_env:
        python_env_bin = 
        sbatch_args.extend([python_env_bin, runfile])
    else:
        sbatch_args.extend(["python", runfile])
    sbatch_args.extend([str(param) for param in parameters])
    sbatch_args_str = f"--wrap='{' '.join(sbatch_args)}'"

    environment_variables = environment_variables if environment_variables is not None else {}
    if prepend_to_path:
        path = f"{prepend_to_path}:{os.environ['PATH']}"
        environment_variables["PATH"] = path
    environment_variables_str = ",".join(
        f"{k}={v}" for (k, v) in environment_variables.items()
    )
    options = [
        f"-J {jobname}",
        f"-A {account}",
        f"-p {partition}",
        f"--mem={memory_gb:.0f}G",
        f"-c {threads:.0f}",
        f"-t {runtime}",
        f"-e ",
        f"-o ",
        f"--export {environment_variables_str if environment_variables_str else 'NONE'}",
    ]
    command = " ".join(["sbatch"] + options + [sbatch_args_str])
    return command


def sbatch(
    runfile: str,
    jobname: str,
    partition: str,
    parameters: Optional[List[Any]],
    account: str,
    threads: int,
    memory_gb: float,
    runtime: str,
    stdout: Optional[str] = None,
    stderr: Optional[str] = None,
    prepend_to_path: Optional[str] = None,
    conda_env: Optional[str] = None,
    environment_variables: Optional[Dict[str, str]] = None,
) -> None:
    """Launch an sbatch.

    Arguments:
        runfile: Path to python script to run.
        jobname: Name of job to submit.
        partition: Name of partition.
        parameters: List of arguments to pass to runfile.
        account: Name of account.
        threads: Number of threads to allocate.
        memory_gb: GB of memory to allocate.
        runtime: Runtime limit (e.g. '384:00:00').
        stdout: Path to directory that will contain stdout log file, or path to log file.
        stderr: Path to directory that will contain stderr log file, or path to log file.
        prepend_to_path: Path to preppend to current path.
        conda_env: Conda environment name.
        environment_variables: Environment variables to assign to job env.
    """
    cmd = create_sbatch_command(
        runfile=runfile,
        jobname=jobname,
        partition=partition,
        parameters=parameters,
        account=account,
        threads=threads,
        memory_gb=memory_gb,
        runtime=runtime,
        stdout=stdout,
        stderr=stderr,
        prepend_to_path=prepend_to_path,
        conda_env=conda_env,
        environment_variables=environment_variables,
    )
    logger = logging.getLogger(__name__)
    logger.info(cmd)
    out = subprocess.check_output(cmd, shell=True)  # nosec: B602
    logger.info(out)


@lru_cache(maxsize=None)
def max_run_time_on_partition(partition_name: str) -> str:
    """Return a partition's max runtime, like '384:00:00'.

    Arguments:
        partition_name: Name of partition to check. (e.g., 'all.q')

    Returns:
        A string representing the max runtime of the partition. (e.g., '384:00:00')
    """
    sinfo_command = subprocess.run(  # nosec: B607
        "which sinfo",
        shell=True,  # nosec: B602
        stdout=subprocess.PIPE,
        universal_newlines=True,
    ).stdout.strip()
    command = [sinfo_command, "-p", partition_name, "-o", "%l"]
    key_value = subprocess.run(
        command, shell=False, stdout=subprocess.PIPE, universal_newlines=True  # nosec: B603
    ).stdout
    time_str = key_value.splitlines()[-1]
    time_in_days, h_m_s = time_str.split("-")
    days_in_hours = int(time_in_days) * 24
    hours, mins, seconds = h_m_s.split(":")
    hours = days_in_hours + int(hours)
    correct_h_m_s = ":".join([str(hours), mins, seconds])
    return correct_h_m_s
