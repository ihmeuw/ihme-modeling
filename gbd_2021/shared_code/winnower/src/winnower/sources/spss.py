import pyreadstat
from pathlib import Path
from ihmeutils import cluster


_ENCODING = 'LATIN1'


def _get_metadata(path: Path):
    _, meta = pyreadstat.read_sav(str(path), metadataonly=True, encoding=_ENCODING)
    return meta


def get_spss_columns(path: Path):
    """
    Make a tuple of column names
    """
    meta = _get_metadata(path)
    return tuple(meta.column_names)


def get_spss_value_labels(path: Path):
    """
    Produce a dictionary of column values and their corresponding labels.
    """
    meta = _get_metadata(path)
    result = meta.variable_value_labels
    return result


def get_spss_column_labels(path: Path):
    """
    Produce a dictionary of column names and their corresponding labels.
    """
    meta = _get_metadata(path)
    return meta.column_names_to_labels


def load_spss(path: Path, usecols):
    """
    Load spss file as a data frame
    """
    if(cluster.max_cpus() >= 8):
        try:
            df, meta = pyreadstat.read_file_multiprocessing(pyreadstat.read_sav, str(path), encoding=_ENCODING, usecols=usecols)  # noqa
        except pyreadstat._readstat_parser.ReadstatError:
            # fallback to serial read as in some cases this fixes things
            df, meta = pyreadstat.read_sav(str(path), encoding=_ENCODING, usecols=usecols)  # noqa
    else:
        df, meta = pyreadstat.read_sav(str(path), encoding=_ENCODING, usecols=usecols)  # noqa
    return df
