"""
Separated values support.

Most likely CSV. Except when it's TSV, or |SV...
"""
import csv
from pathlib import Path

from pandas.io.parsers import read_csv


# Different encoding guesses to try when lodaing data
# order is arbitary and should be revisited once more inputs are found that
# aren't UTF-8 compatible
# https://docs.python.org/3/library/codecs.html#standard-encodings
CSV_ENCODING_GUESSES = (
    "utf-8",
    # OS X default
    "mac_roman",
    # western europe
    "windows-1252",
)

LOAD_ERR_TEMPLATE = (
    # NOTE: path will be interpolated at runtime
    f"Failed to open {{}} with encodings: {CSV_ENCODING_GUESSES}. Please"
    "contact the winnower team about supporting your file.")


def iter_readers(path, *, delimiter):
    "Yields the same open file in different encodings."
    for encoding in CSV_ENCODING_GUESSES:
        with path.open(encoding=encoding) as open_file:
            yield open_file


def get_separated_values_columns(path: Path, *, delimiter):
    for open_file in iter_readers(path, delimiter=delimiter):
        reader = csv.reader(open_file, delimiter=delimiter)
        try:
            return tuple(next(reader))
        except UnicodeDecodeError:
            pass
    else:
        raise RuntimeError(LOAD_ERR_TEMPLATE.format(path))


def load_separated_values(path: Path, *, delimiter, usecols=None):
    for open_file in iter_readers(path, delimiter=delimiter):
        try:
            return read_csv(open_file, delimiter=delimiter,
                            low_memory=False, usecols=usecols)
        except UnicodeDecodeError:
            pass
    else:
        raise RuntimeError(LOAD_ERR_TEMPLATE.format(path))
