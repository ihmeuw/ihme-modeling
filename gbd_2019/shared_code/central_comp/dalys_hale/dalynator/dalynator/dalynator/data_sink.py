import logging
import os
from copy import deepcopy

from gbd.constants import risk

from dalynator.constants import UMASK_PERMISSIONS
from draw_sources.draw_sources import DrawSink

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
            except Exception:
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
        self.dir_name, self.file_name = os.path.split(file_path)
        self.kwargs = kwargs

    def write(self, data_frame, **kwargs):

        logger.info("Writing HDF file '{}'".format(self.file_path))
        self.check_paths()
        to_pass = deepcopy(self.kwargs)
        to_pass.update(kwargs)

        sink = DrawSink(params={'file_pattern': self.file_name,
                                'draw_dir': self.dir_name})

        if 'rei_id' in data_frame.columns:
            sink.push(data_frame[data_frame.rei_id != risk.TOTAL_ATTRIBUTABLE],
                      append=False,
                      **to_pass)
        else:
            sink.push(data_frame, append=False, **to_pass)
        logger.info("  finished write to HDF file {}".format(self.file_path))
