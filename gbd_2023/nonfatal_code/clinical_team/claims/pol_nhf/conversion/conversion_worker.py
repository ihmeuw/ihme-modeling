import argparse
import os
from typing import Dict, Union

import numpy as np
import pandas as pd
import pyspark.pandas as ps
from crosscutting_functions.pipeline_constants import poland as constants
from crosscutting_functions.formatting-functions.general import date_formatter
from crosscutting_functions.clinical_metadata_utils.parser import filepath_parser
from crosscutting_functions.clinical_metadata_utils.initialize import ci_spark


def get_unique_vals(df: Union[pd.DataFrame, ps.DataFrame], col: str) -> np.ndarray:
    """Brings the unique values for a given column of DataFrame into memory.
    Wrapper around unique() method to make the call the same for either pandas
    or pyspark.pandas.

    Args:
        df (Union[pd.DataFrame, ps.DataFrame]): A pandas like DataFrame.
        col (str): The column to parse for unique values.

    Returns:
        np.ndarray: Unique values found in the specified column.
    """
    current_vals = df[col].unique()
    if isinstance(df, ps.DataFrame):
        current_vals = current_vals.to_numpy()

    return current_vals


def correct_names(
    df: Union[pd.DataFrame, ps.DataFrame],
    col: str,
    mapdict: Dict[str, str],
    oldsuffix: str = "_og",
    keep: bool = True,
) -> Union[pd.DataFrame, ps.DataFrame]:
    """Maps correct strings to improper strings values. This corrects those with a mapping dictionary keyed by the values
    currently in the dataframe, with values being the corrected version. If keep is True,
    the original and corrected columns will both be kept. The original column will be
    given the suffix specified by 'oldsuffix'.

    Args:
        df (Union[pd.DataFrame, ps.DataFrame]): Raw Poland dataframe read in with
            latin-2 encoding.
        col (str): Column name to correct.  Ex: voivodeship, poviats.
        mapdict (Dict[str, str]): key: value in current data, value: value to be corrected to.
            Maps these into the dataframe.
        oldsuffix (str, optional): Suffix to give the current data column. Defaults to "_og".
        keep (bool, optional): Indicate if the column being corrected should kept.
            the 'incorrect' version. Defaults to True.

    Returns:
        Union[pd.DataFrame, ps.DataFrame]: Input dataframe with string values corrected
            based on 'mapdict'.
    """

    current_vals = get_unique_vals(df=df, col=col)

    if len(set(mapdict.values()) - set(current_vals)) > 0:
        df[f"{col}_tmp"] = df[col].map(mapdict)
    else:
        df[f"{col}_tmp"] = df[col].copy()

    df = df.rename(columns={col: f"{col}{oldsuffix}", f"{col}_tmp": col})
    if not keep:
        df = df.drop(f"{col}{oldsuffix}", axis=1)  # type:ignore

    return df


if __name__ == "__main__":
    clinical_env_path = "FILEPATH"
    os.environ["PATH"] = f"{clinical_env_path}:{os.environ['PATH']}"
    par = argparse.ArgumentParser()
    par.add_argument("--year_id", type=int, help="Year of data to process")
    par.add_argument("--worker_mem", type=int, help="worker memory for spark")
    par.add_argument("--file_source_id", type=int, help="Data version for Poland")
    args = par.parse_args()

    year_id = str(int(args.year_id))

    spark = ci_spark(
        memory_gb=int(args.worker_mem),
        tmp_dirkey="spark_default",
        name="pol_nhf_conversion_worker",
    )

    path = str(filepath_parser(ini="pipeline.pol_nhf", section="raw", section_key=year_id))

    READ_KWARGS = constants.READ_KWARGS
    if args.file_source_id <= 3:
        READ_KWARGS["encoding"] = "ISO-8859-2"
    else:
        READ_KWARGS["encoding"] = constants.RAW_YEAR_ENCODING[args.file_source_id][
            args.year_id
        ]

    df = ps.read_csv(path, **READ_KWARGS)

    for col in constants.MAKE_LATIN_2_COLS:
        df = correct_names(
            df=df, col=col, mapdict=eval(f"constants.READ_TO_LATIN_{col.upper()}")
        )

    # Save as GBD location name
    df = correct_names(
        df=df, col="voivodeship", mapdict=constants.LATIN_2_GBD_MAP, oldsuffix="_corrected"
    )

    fixed_locs = get_unique_vals(df=df, col="voivodeship")

    if set(constants.LATIN_2_GBD_MAP.values()) - set(fixed_locs):
        raise ValueError(
            "Location behavior performed unexpectedly. Check if encoding changed."
        )

    for col, dtype in constants.READ_DTYPES.items():
        if dtype == str:
            df[col] = df[col].replace({"None": None})

    df = date_formatter(df=df, columns=constants.DATE_COLS)

    path_ext = "FILEPATH"
    path_dir = filepath_parser(
        ini="pipeline.pol_nhf", section="conversion", section_key="parquet_dir"
    )
    df.to_parquet("FILEPATH")
