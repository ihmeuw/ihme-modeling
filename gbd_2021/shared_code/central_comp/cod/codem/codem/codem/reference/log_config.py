import logging
import os

import codem.data.queryStrings as QS
from codem.reference import db_connect, paths


def get_log_dir(model_version_id, db_connection):
    base_dir = paths.get_base_dir(model_version_id, db_connection=db_connection)
    return paths.setup_dir(f"FILEPATH")


def setup_logging(model_version_id, step_id, db_connection):
    """
    Setup logging settings for each step.

    :param model_version_id: int
    :param step_id: int
    """

    log_dir = get_log_dir(model_version_id, db_connection)
    logger = logging.getLogger()

    # Assign filename for log, and format how each line is labeled
    if step_id is not None:
        log_filename = os.path.join(log_dir, f'step_{step_id}.log')
    else:
        log_filename = os.path.join(log_dir, 'run.log')
    fh = logging.FileHandler(log_filename, mode='w')
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.setLevel(logging.INFO)

    logger.info(f"Setup logger at {log_filename}.")
    return log_dir


class ModelerAlert:
    def __init__(self, model_version_id, db_connection):
        """
        Write to the database when a particular part of the procedure has
        started so that it can be read by the CodViz tool. This enables users
        to see what stage a model is currently at (i.e. covariate selection,
        linear model building, space time smoothing, gaussian process, etc.).
        """
        self.model_version_id = model_version_id
        self.db_connection = db_connection

    def alert(self, message):
        call = QS.status_write.format(self.model_version_id, message)
        db_connect.query(call, self.db_connection)
