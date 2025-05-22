import logging
import os

import db_tools_core

import codem.data.queryStrings as QS
from codem.reference import paths


def get_log_dir(model_version_id: int, conn_def: str) -> str:
    """Returns log directory of given model_version_id."""
    base_dir = paths.get_base_dir(model_version_id, conn_def=conn_def)
    return paths.setup_dir(f"{base_dir}/logs")


def setup_logging(model_version_id: int, step_id: int, conn_def: str) -> str:
    """
    Setup logging settings for each step.

    :param model_version_id: int
    :param step_id: int
    :param conn_def: str
    """
    log_dir = get_log_dir(model_version_id, conn_def=conn_def)
    logger = logging.getLogger()

    # Assign filename for log, and format how each line is labeled
    if step_id is not None:
        log_filename = os.path.join(log_dir, f"step_{step_id}.log")
    else:
        log_filename = os.path.join(log_dir, "run.log")
    fh = logging.FileHandler(log_filename, mode="w")
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.setLevel(logging.INFO)
    # Change httpx level to WARNING to avoid logging all rules GET requests.
    logging.getLogger("httpx").setLevel(logging.WARNING)

    logger.info(f"Setup logger at {log_filename}.")
    return log_dir


class ModelerAlert:
    """Class for inserting log entries into cod.model_version_log."""

    def __init__(self, model_version_id: int, conn_def: str) -> None:
        """
        Write to the database when a particular part of the procedure has
        started so that it can be read by the CodViz tool. This enables users
        to see what stage a model is currently at (i.e. covariate selection,
        linear model building, space time smoothing, gaussian process, etc.).
        """
        self.model_version_id = model_version_id
        self.conn_def = conn_def

    def alert(self, message: str) -> None:
        """Updates model version status in cod.model_version."""
        with db_tools_core.session_scope(conn_def=self.conn_def) as session:
            session.execute(
                QS.status_write,
                params={
                    "model_version_id": self.model_version_id,
                    "model_version_log_entry": message,
                },
            )
