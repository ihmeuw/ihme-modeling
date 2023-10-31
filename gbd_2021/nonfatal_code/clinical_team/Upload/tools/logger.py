import datetime
import logging
import importlib
from pathlib import Path
import shutil
import sys

import pandas as pd

logging.VERBOSE = 5
logging.addLevelName(logging.VERBOSE, "VERBOSE")


class LogError(Exception):
    pass


class LogInfo:
    """ This is a base class that holds info on loggers"""

    def __init__(self, level, formt):
        self._level = level
        self._format = logging.Formatter(formt)
        self._handler = None
        self._setup()

    def _setup(self):
        raise LogError("Need to override _setup")

    @property
    def path(self):
        return self._path

    @property
    def format(self):
        return self._format

    @property
    def handler(self):
        if not self._handler:
            raise LogError("Need to override _setup")
        return self._handler

    def __repr__(self):
        return f"self.__class__.__name__: " f"format: {self._format}"


class FileLogInfo(LogInfo):
    def __init__(self, path, level, formt):
        self._path = path
        super().__init__(self, formt)

    def _setup(self):
        self._handler = logging.FileHandler(self.path)
        self._handler.setFormatter(self._format)


class StdOutLogInfo(LogInfo):
    def _setup(self):
        self._handler = logging.StreamHandler(sys.stdout)
        self._handler.setFormatter(self._format)


class BaseLogger:
    """ ClinicalLogger is an tool for logging information during pipeline runs"""

    def __init__(self, path, name="log"):
        path = self._parse_path(path)
        self._log_dir = path

        self._name = name
        self._grouper = None

        # Set up logging https://stackoverflow.com/questions/12034393/import-side-effects-on-logging-how-to-reset-the-logging-module
        importlib.reload(logging)
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.VERBOSE)
        formt = "%(asctime)s - %(levelname)s: %(message)s"

        # Logging to files
        timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M")
        self._file_base = f"{name}_{timestamp}"
        self._file_log = FileLogInfo(
            path=self._log_dir / f"{self._file_base}.log",
            level=logging.VERBOSE,
            formt=formt,
        )
        self._logger.addHandler(self._file_log.handler)

        # Logging to stdout (the screen)
        self._stdout_log = StdOutLogInfo(level=logging.DEBUG, formt=formt)
        self._logger.addHandler(self._stdout_log.handler)

    def log(self, msg, level=logging.DEBUG):
        """ used to log human readable information e.g. print to the screen"""
        self._logger.log(level, msg)

    def save(self, path):
        path = self._parse_path(path)
        shutil.copy(self._file_log.path, path)

    def _parse_path(self, path):
        # Check if we can make a path
        try:
            path = Path(path)
            path = path.expanduser()
        except Exception as e:
            raise LogError(
                f"Provided path: {path} must be convertable to " "a pathlib.Path"
            )
        # Check Path exists
        if not path.exists():
            raise LogError(f"Provided path: {path} doesn't exist")
        return path


class GrouperError(Exception):
    pass


class Grouper:
    """ Grouper is an object that holds information about groups of data in a Pandas.DataFrame"""

    def __init__(self, groups):
        self._groups = groups

    def setup(self, df):
        self.validate(df)

        self._group_lengths = {}
        groupby = df.groupby(by=self._groups)
        for names, group_df in groupby:
            self._group_lengths[names] = len(group_df)

    @property
    def groups(self):
        return self._groups.copy()

    @property
    def stats(self):
        return self._group_lengths.copy()

    def get_stat(self, key):
        return self._group_lengths[key]

    def _to_df(self, dictionary):
        """ Creates a Pandas.DataFrame from a dictionary whose keys are tuples and values are tuples. Each key value pair represents a row. Used with summary(df)"""
        rows = [key + value for key, value in dictionary.items()]
        return pd.DataFrame(rows, columns=self._groups)

    def summary(self, df):
        """ Calculates the percentage of group size of the passed in DF"""
        self.validate(df)

        results = {}
        groupby = df.groupby(by=self._groups)
        for names, group_df in groupby:
            # Grab the size of the group and the percentage of the group
            # compared to the df that was used during setup.
            results[names] = (len(group_df), len(group_df) / self._group_lengths[names])

        return results

    def validate(self, df):
        missing_groups = [group for group in self._groups if group not in df.columns]
        if missing_groups:
            raise GrouperError(
                f"Dataframe is missing the following groups: " f"{missing_groups}"
            )
        return True


class FilterLogger(BaseLogger):
    def __init__(self, path, name="log"):
        super().__init__(path, name)

        # file to save dropped rows
        self._filter_path = self._log_dir / f"{self._file_base}.csv"

    def set_groups(self, df, groups):
        self.log(f"Reporting data by {groups}")
        self._grouper = Grouper(groups)
        self._grouper.setup(df)

    @property
    def groups(self):
        return self._groupers.groups

    def report_filter(self, filter_name, df):
        self.log(f"FILTER: {filter_name} - dropped {len(df)} rows.")
        if len(df) == 0:
            return

        # Print a more detailed summary of the filtered out rows
        summary_stats = self._grouper.summary(df)
        for names, stats in summary_stats.items():
            # We'll format this like a csv (seperate values with a comma)
            # this will make it easy to generate csv's from the logs
            drop_stats = ",".join([str(num) for num in names + stats])
            self.log(f"  - dropping {drop_stats}")

        # Save Dropped rows to a file
        df["filter"] = filter_name
        if not self._filter_path.exists():
            df.to_csv(self._filter_path, mode="w", index=False)
        else:
            # Grab the columns from the current log file for 2 reasons:
            # 1) We need to know what columns we need to write
            # 2) We need to make sure we write our data using the same order
            #    as the data in the file since we are appending rows.
            columns = pd.read_csv(self._filter_path, nrows=0).columns
            # Fill in missing columns
            for col in columns:
                if col not in df.columns:
                    df[col] = None
            # Write dropped columns
            df[columns].to_csv(self._filter_path, mode="a", header=False, index=False)

    def save(self, path):
        super().save(path)
        if self._filter_path.exists():
            shutil.copy(self._filter_path, path)


class TestLogger(BaseLogger):
    def report_test(self, test_name, result):
        """ logs test results """
        # Passed
        if result:
            status = "PASSED"
        else:
            # The weird stuff below formats the text as red
            status = "\t\x1b[6;70;41m" + "FAILED" + "\x1b[0m"
        status = "passed" if result else "failed"
        self.log(f"TEST ({status}): {test_name}")


class ClinicalLogger(FilterLogger, TestLogger):
    pass
