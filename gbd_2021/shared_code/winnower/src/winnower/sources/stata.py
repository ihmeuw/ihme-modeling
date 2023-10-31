import datetime as dt
import functools
from pathlib import Path
import struct
import sys
import warnings
from ihmeutils import cluster

import numpy
from pandas.io.stata import (
    StataReader,
    _stata_elapsed_date_to_datetime_vec,
)
import pyreadstat

from winnower import errors


class WinnowerStataReader(StataReader):
    def _get_time_stamp(self):
        try:
            return super()._get_time_stamp()
        except ValueError:
            if self.format_version == 104:
                msg = ("Applying work-around to load Stata 4 file. This has "
                       "no known downsides but you should double-check the "
                       "output for correctness")
                warnings.warn(msg)
                return "time_stamp not present on file format version 104"

            msg = ("_get_time_stamp not supported for Stata format "
                   f"{self.format_version}")
            tb = sys.exc_info()[2]
            raise ValueError(msg).with_traceback(tb) from None


def reader(path: Path):
    # only after you've read *some* data you can call
    # stataReader.value_labels() which returns a dict
    # of {col_name: {col_val1: label_val_1, col_val2: ...}, ... }
    #
    # this can be used to *gently* reapply converted categoricals
    # except for those SPECIFIC COLUMNS which have 2 keys that
    # map to the same value
    try:
        return WinnowerStataReader(path.open('rb'),
                                   convert_categoricals=False)
    except OSError:
        # Encountered on Windows - unable to reproduce internally
        # Best guess is not enough contiguous memory to read() the entire file
        msg = ("Failed reading data from file. It may be that your file is "
               "too large to read into memory right now. Try closing programs "
               "before extracting again. If this still fails contact the "
               "winnower team for help.")
        raise errors.LoadError(msg, str(path))
    except ValueError as e:
        if len(e.args) == 1:
            if e.args[0].startswith('cannot convert stata dtypes '):
                # I/O issue - pandas does not support this.
                msg = (f"pandas cannot open {path.name} as there are data "
                       "types it does not recognize")
                tb = sys.exc_info()[2]
                raise errors.LoadError(msg,
                                   str(path),
                                   e.args[0]).with_traceback(tb) from None
        raise
    except KeyError:
        # Error raised by buggy pandas code:
        # self.dtyplist = [self.DTYPE_MAP[typ] for typ in typlist]
        tb = sys.exc_info()[2]
        raise errors.LoadError(f"Failed to load {path}").with_traceback(tb)


def instruct_if_bad_format(func):
    """
    Decorator to wrap functions and provide a better error message.
    """
    # Prefix of message pandas.io.stata.StataReader gives on bad format_version
    pandas_prefix = "Version of given Stata file is not"
    pyreadstat_prefix = "Invalid file, or file has unsupported features"
    howto_fix = ("winnower cannot open this DTA file because it is an "
                 "unsupported version. Please contact your data indexer and "
                 "have them re-save the file as Stata 13 format")

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValueError as e:
            # TODO: expand this to check if _get_time_stamp was called and
            # raise this error as well
            args = e.args
            prefix = pandas_prefix
            exc = e
        except pyreadstat._readstat_parser.ReadstatError as e:
            args = e.args
            prefix = pyreadstat_prefix
            exc = e

        if args and isinstance(args[0], str) and args[0].startswith(prefix):
            raise errors.Error(howto_fix) from None

        raise exc

    return wrapped


def pandas_get_stata_columns(path: Path):
    return tuple(reader(path).varlist)


def pandas_get_stata_column_labels(path: Path):
    """
    Produce a dictionary of column names and their corresponding labels.
    """
    stata_reader_object = reader(path)
    keys = stata_reader_object.varlist
    values = stata_reader_object._variable_labels
    return dict(zip(keys, values))


def pandas_get_stata_value_labels(path: Path):
    _reader = reader(path)
    value_label_dict = _reader.value_labels()
    return {col: value_label_dict[label]
            for col, label in zip(_reader.varlist, _reader.lbllist)
            if label in value_label_dict}


def pandas_load_stata(path: Path, columns):
    return reader(path).read(columns=columns)


def _pyreadstat_read_dta(path: Path, **kwargs):
    """
    Wrapper function for reading DTA files via pyreadstat.

    pyreadstat includes an important function that pandas does not: UTF-8
    support.

    Unfortunately the plumbing for setting encodings is not present in winnower
    because few of the file formats support it and the one that commonly needs
    it (CSV) doesn't have a good way of explicitly providing it in codebooks.

    This implements a silly hack to guess at UTF-8 if whatever is provided
    (likely nothing) doesn't work.
    """
    if(cluster.max_cpus() >= 8):
        try:
            return pyreadstat.read_file_multiprocessing(pyreadstat.read_dta, str(path), num_processes=8, **kwargs)
        except (UnicodeDecodeError, pyreadstat._readstat_parser.ReadstatError):
            kwargs['encoding'] = 'utf-8'
            return pyreadstat.read_file_multiprocessing(pyreadstat.read_dta, str(path), num_processes=8, **kwargs)
    else:
        try:
            return pyreadstat.read_dta(str(path), **kwargs)
        except (UnicodeDecodeError, pyreadstat._readstat_parser.ReadstatError):
            kwargs['encoding'] = 'utf-8'
            return pyreadstat.read_dta(str(path), **kwargs)


def pyreadstat_get_stata_columns(path: Path):
    _, meta = _pyreadstat_read_dta(path, metadataonly=True)
    return tuple(meta.column_names)


def pyreadstat_get_stata_column_labels(path: Path):
    _, meta = _pyreadstat_read_dta(path, metadataonly=True)
    return meta.column_names_to_labels


def pyreadstat_get_stata_value_labels(path: Path):
    _, meta = _pyreadstat_read_dta(path, metadataonly=True)
    # variable_to_label: column_name: label_name
    # value_labels: label_name: dict_of_labels
    value_label_dict = meta.value_labels
    try:
        return {column: value_label_dict[label]
                for column, label in meta.variable_to_label.items()}
    except KeyError as e:
        if not value_label_dict:
            # pyreadstat doesn't always support reading the value labels, but
            # we require them. Raise this as a ReadstatError and catch in
            # instruct_if_bad_format
            msg = "Invalid file, or file has unsupported features"
            raise pyreadstat._readstat_parser.ReadstatError(msg)
        else:
            column = e.args[0]
            msg = f"Failed to load labels for {column}."
            raise errors.Error(msg)


def pyreadstat_load_stata(path: Path, columns):
    df, meta = _pyreadstat_read_dta(path,
                                    dates_as_pandas_datetime=True,
                                    usecols=columns)

    # special "missing" values encountered in old data files
    ODD_INVALID_VALUES = {
        numpy.float64: (
            # JAM_LSMS_1997_CHILD_HEALTH.DTA
            struct.unpack("<d", b'\x00\x00\x00\x00\x00\x00\xc0T')[0],
        ),
    }

    # loop through columns in df. meta contains all columns while
    # df might be partially loaded.
    for col_name in df.columns:
        col = df[col_name]

        # test for incomplete handling of "missing" numeric values
        bad_vals = ODD_INVALID_VALUES.get(col.dtype.type)
        if bad_vals is not None:
            bad_idx = col.isin(bad_vals)
            df.loc[bad_idx, col_name] = numpy.nan

        # test for incomplete date/datetime conversion
        format = meta.original_variable_types[col_name]
        if format.startswith(("%t", "%d")):
            if not isinstance(col[0], (dt.date, dt.datetime)):
                # manually convert dates if not automatically converted
                # WORKING: %tdD_m_y is, as is %tc
                # NOT WORKING: %dD_m_y and %tcD_m_Y
                # See page 3 for more info on format codes
                # https://www.stata.com/manuals13/u24.pdf
                df[col_name] = _stata_elapsed_date_to_datetime_vec(col, format)
    return df


@instruct_if_bad_format
def get_stata_columns(path: Path):
    try:
        return pandas_get_stata_columns(path)
    except errors.LoadError:
        return pyreadstat_get_stata_columns(path)


@instruct_if_bad_format
def get_stata_column_labels(path: Path):
    try:
        return pandas_get_stata_column_labels(path)
    except errors.LoadError:
        return pyreadstat_get_stata_column_labels(path)


@instruct_if_bad_format
def get_stata_value_labels(path: Path):
    try:
        return pandas_get_stata_value_labels(path)
    except errors.LoadError:
        return pyreadstat_get_stata_value_labels(path)


@instruct_if_bad_format
def load_stata(path: Path, columns):
    try:
        return pandas_load_stata(path, columns)
    except errors.LoadError:
        return pyreadstat_load_stata(path, columns)
