from io import StringIO
from pathlib import Path
from unittest import mock

# Wrap and suppress "print" statements run in simpledbf
sio = StringIO()
with mock.patch('builtins.print', new=lambda msg: sio.write(msg + '\n')):
    import simpledbf
sio.seek(0)
lines = [X for X in sio.readlines() if X not in {
    'PyTables is not installed. No support for HDF output.\n',
    'SQLalchemy is not installed. No support for SQL output.\n',
}]
for line in lines:
    print(line, end='')

del sio
del lines


def get_dbf_columns(path: Path):
    return tuple(simpledbf.Dbf5(path).columns)


def load_dbf(path: Path):
    return simpledbf.Dbf5(path).to_dataframe()
