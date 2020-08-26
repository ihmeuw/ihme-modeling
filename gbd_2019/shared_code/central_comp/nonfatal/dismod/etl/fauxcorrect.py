from pathlib import Path
import sys

from db_queries.get_age_metadata import get_age_spans
import pandas as pd

HEADER_COLS = [
    "measure_id",
    "location_id",
    "sex_id",
    "age_group_id",
    "year_id",
    "cause_id",
    "metric_id",
    "val",
    "upper",
    "lower",
]

ROOT_DIR = Path("PATH")
NAME_TEMPLATE = "{cause_id}_{fauxcorrect_version}.csv"
RAW_DIR = "{fauxcorrect_version}/raw/"
OUT_DIR = "{fauxcorrect_version}/formatted/"


def format_df(df):
    df = df[df.sex_id != 3]

    new_column_map = {
        "sex_id": "x_sex",
        "year_id": "time_lower",
        "val": "meas_value",
        "upper": "meas_upper",
        "lower": "meas_lower",
    }
    df.columns = [new_column_map.get(col, col) for col in df.columns]
    age_group_set_id = 12
    age_df = get_age_spans(age_group_set_id).rename(
        {"age_group_years_start": "age_lower", "age_group_years_end": "age_upper"},
        axis=1,
    )
    df = df.merge(age_df, how="inner")

    new_column_names = [
        "location_id",
        "time_lower",
        "age_group_id",
        "x_sex",
        "age_lower",
        "age_upper",
        "meas_value",
        "meas_lower",
        "meas_upper",
    ]

    return df[new_column_names]


def read_file(filename):
    return pd.read_csv(filename, header=None, names=HEADER_COLS)


def get_input_file_name(fauxcorrect_version, cause_id):
    return (
        Path(ROOT_DIR)
        / Path(RAW_DIR.format(fauxcorrect_version=fauxcorrect_version))
        / Path(
            NAME_TEMPLATE.format(
                cause_id=cause_id, fauxcorrect_version=fauxcorrect_version
            )
        )
    )


def get_output_file_name(fauxcorrect_version, cause_id):
    return (
        Path(ROOT_DIR)
        / Path(OUT_DIR.format(fauxcorrect_version=fauxcorrect_version))
        / Path(
            NAME_TEMPLATE.format(
                cause_id=cause_id, fauxcorrect_version=fauxcorrect_version
            ).replace('csv', 'h5')
        )
    )


def write_file(df, fauxcorrect_version, cause_id):
    fname = get_output_file_name(fauxcorrect_version, cause_id)
    df.to_hdf(fname, key="data", format="fixed")


def format_cause(fauxcorrect_version, cause_id):
    fname = get_input_file_name(fauxcorrect_version, cause_id)
    df = read_file(fname)
    formatted = format_df(df)
    write_file(formatted, fauxcorrect_version, cause_id)


def main(fauxcorrect_version, cause_id):
    format_cause(fauxcorrect_version, cause_id)


if __name__ == '__main__':
    fauxcorrect_version = int(sys.argv[1])
    cause_id = int(sys.argv[2])
    main(fauxcorrect_version, cause_id)
