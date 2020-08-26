import getpass
import logging
import os
import subprocess
from typing import Optional

import stgpr
from stgpr.common import logging_utils
from stgpr.common.constants import paths
from stgpr.model.config import MASTER_SCRIPT, PYSHELL


def stgpr_sendoff(
        run_id: int,
        project: str,
        log_path: Optional[str] = None,
        nparallel: int = 50
) -> None:
    """
    Submits an ST-GPR model to the cluster.

    Args:
        run_id: an ST-GPR version ID
        project: the cluster project to which jobs will be submitted
        log_path: path to a directory for saving run logs.
            Defaults to FILEPATH and
            FILEPATH if not specified.
            Don't put logs on h/ or j/
        nparallel: Number of parallelizations to split your data over
            (by location_id). Set to 50 for small datasets, 100 for large
            datasets.

    Raises:
        ValueError: if path to logs is on J
        RuntimeError: if the qsub command fails
    """
    logging_utils.configure_logging()
    if log_path and log_path.startswith('FILEPATH'):
        raise ValueError(
            f'Cannot store logs at {log_path}: storing logs on J is not'
            'allowed'
        )

    username = getpass.getuser()
    error_log_path = log_path or \
        paths.ERROR_LOG_PATH_FORMAT.format(username=username)
    output_log_path = log_path or \
        paths.OUTPUT_LOG_PATH_FORMAT.format(username=username)
    os.makedirs(error_log_path, exist_ok=True)
    os.makedirs(output_log_path, exist_ok=True)
    logging.info(f'Saving error logs to {error_log_path}')
    logging.info(f'Saving output logs to {output_log_path}')
    output_path = paths.OUTPUT_ROOT_FORMAT.format(run_id=run_id)

    args = ' '.join([
        str(run_id),
        error_log_path,
        output_log_path,
        project,
        str(nparallel),
        stgpr.__version__,
        output_path
    ])
    command = (
        f'qsub -N MASTER_{run_id} -l m_mem_free=0.25G -l fthread=3 '
        f'-l h_rt=48:00:00 -P {project} -q all.q -o {output_log_path} '
        f'-e {error_log_path} {PYSHELL} {MASTER_SCRIPT} {args}'
    )
    logging.info(f'Submitting master ST-GPR job for run ID {run_id}')
    try:
        subprocess.run(command, shell=True, check=True, capture_output=True)
    except subprocess.CalledProcessError as error:
        raise RuntimeError(
            f'qsub failed with exit status {error.returncode} and output '
            f'{error.stderr.decode("utf-8")}'
        )
    logging.info(f'Job submitted successfully')
