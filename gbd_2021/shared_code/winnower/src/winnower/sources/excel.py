from pathlib import Path

from pandas.io.excel import read_excel


def get_excel_columns(path: Path):
    return tuple(load_excel(path).columns)


def load_excel(path: Path):
    suffix = path.suffix.lower()
    if suffix == '.xls':
        df = read_excel(path)
    else:
        df = read_excel(path, engine='openpyxl')
    # Stata will remove all spaces in Excel column names by default
    df = df.rename(columns={column: column.replace(' ', '')
                            for column in df
                            if ' ' in column})
    return df
