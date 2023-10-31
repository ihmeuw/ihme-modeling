from pathlib import Path
import pyreadstat


def get_xport_columns(path: Path):
    _, meta = pyreadstat.read_xport(str(path), metadataonly=True)
    return tuple(meta.column_names)


def load_xport(path: Path, usecols):
    df, meta = pyreadstat.read_xport(str(path), usecols=usecols)
    return df
