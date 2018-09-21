import logging
import os
from copy import deepcopy

from dalynator.constants import UMASK_PERMISSIONS
from transmogrifier.draw_ops import save_draws

os.umask(UMASK_PERMISSIONS)
logger = logging.getLogger(__name__)


class DataSink:
    """writes a dataframe, a simple coloring interface"""

    def write(self, data_frame):
        return "Undefined virtual method"

    def check_paths(self):
        if not os.path.exists(self.dir_name):
            logger.info(" making dir {}".format(self.dir_name))
            try:
                logger.info(" making dir {}".format(self.dir_name))
                os.makedirs(self.dir_name)
            except Exception as e:
                # This can be caused by a race between two processes
                logger.info(" makedirs threw an exception but continuing, " +
                            "exception was {}".format(self.dir_name))
                pass
        if os.path.exists(self.file_path):
            logger.info(" removing existing file (expected behavior) " +
                        "{}".format(self.file_path))
            os.remove(self.file_path)


class CSVDataSink(DataSink):
    def __init__(self, file_path):
        self.file_path = file_path
        self.dir_name = os.path.dirname(self.file_path)

    def write(self, data_frame):
        self.check_paths()
        should_write_index = not data_frame.index.is_integer()
        data_frame.to_csv(self.file_path, index=should_write_index)


class HDFDataSink(DataSink):
    def __init__(self, file_path, **kwargs):
        self.file_path = file_path
        self.dir_name = os.path.dirname(self.file_path)
        self.kwargs = kwargs

    def write(self, data_frame, **kwargs):
        logger.info("Writing HDF file '{}'".format(self.file_path))
        self.check_paths()
        to_pass = deepcopy(self.kwargs)
        to_pass.update(kwargs)
        id_cols = to_pass.pop("data_columns", None)

        save_draws(data_frame, self.file_path, append=False, id_cols=id_cols,
                   **to_pass)
        logger.info("  finished write to HDF file {}".format(self.file_path))
