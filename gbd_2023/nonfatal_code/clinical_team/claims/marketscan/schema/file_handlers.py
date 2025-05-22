from pathlib import Path
from typing import Dict, List

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import marketscan as constants

from marketscan.schema import config, input_data_types


def get_file_path(source: str, data_type: input_data_types.dataclass, year: int) -> str:
    """Given a source (mdcr, ccae), data set type, and year return the full filepath
    for the parquet conversion of that raw data file.

    Args:
        source: Marketscan data source, eg ccae or mdcr.
        data_type: Data type class object containing metadata.
        year: Reporting year of data to process.

    Returns:
        Full filepath of parquet converted MS data
    """
    year_suf = constants.YEAR_NAMING_SCHEME[year]

    settings = config.get_settings()
    file_path = "FILEPATH"
    if not Path(file_path).exists():
        raise RuntimeError(f"The file path {file_path} does not exist")

    return file_path


def col_cast(df: pd.DataFrame, col: str, dtype: str) -> pd.DataFrame:
    """Different pandas methods are required to cast to each column data type."""
    if dtype in ["integer", "float"]:
        df[col] = pd.to_numeric(df[col], downcast=dtype)
    elif dtype == "datetime":
        df[col] = pd.to_datetime(df[col])
    elif dtype == "category":
        df[col] = pd.Categorical(df[col])
    else:
        raise ValueError(f"Column data type {dtype} is not recognized")

    return df


def dx_to_str(cat_df: pd.DataFrame) -> pd.DataFrame:
    """This is a helper to cast back from cat(egorical) to str."""

    dx_cols = ["cause_code"] + get_dx_cols()
    for col in dx_cols:
        if col in cat_df.columns:
            null_index = cat_df[col].isnull()
            cat_df[col] = cat_df[col].astype(str)
            cat_df.loc[null_index, col] = None
    return cat_df


def downcast_col_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Cast all matching columns in df to their expected dtype defined in get_col_dtypes."""
    cols = df.columns

    for col, dtype in get_col_dtypes().items():
        if col in cols:
            df = col_cast(df, col, dtype)

    return df


def read_ms_parquet(file_path: str, cols: List[str]) -> pd.DataFrame:
    """Read and standardize column names for a parquet file in location file_path."""

    df = pd.read_parquet(file_path, columns=[col.upper() for col in cols])

    df.columns = df.columns.str.lower()
    df.rename(columns=get_rename_dict(), inplace=True)

    df = downcast_col_dtypes(df)
    return df


def check_path(p: str, year: int) -> None:
    """Pandas.df.to_parquet will append data when partition columns are set. This would result
    in duplicated claims data so throw an error."""

    job_partition = f"{p}/year={year}"
    if Path(job_partition).exists():
        raise IOError(
            (
                "The partition for this year already exists "
                "and data would be appended on, potentially creating duplicates. "
                f"Manually delete {job_partition}"
            )
        )


def check_icd_mart_path(p: str, filter_dict: Dict[str, int]) -> None:
    """Pandas.df.to_parquet will append data when partition columns are set. This would result
    in duplicated claims data so throw an error."""

    job_partition = (
        "FILEPATH"
    )

    if Path(job_partition).exists():
        raise IOError(
            (
                "The partition for this year already exists "
                "and data would be appended on, potentially creating duplicates. "
                f"Manually delete {job_partition}"
            )
        )


def subset_df_writer(df: pd.DataFrame, file_path: str, part_cols: List[str]) -> None:
    """Write the ICD Mart data by subsets of ICD group IDs.

    Args:
        df: Full df of data to process.
        file_path: Parent parquet path to write to.
    """
    df["icd_group_id"] = df["icd_group_id"].astype(int)
    for icd_group in df.icd_group_id.unique().tolist():  # a few thousand groups per year
        if not isinstance(icd_group, int):
            raise ValueError("icd_group_id must be integer")
        write_filter = f"icd_group_id == {icd_group}"
        tmp = df.query(write_filter).copy()
        if len(tmp) == 0:
            continue
        tmp = dx_to_str(tmp)
        tmp.to_parquet(file_path, partition_cols=part_cols, compression=constants.COMP)


def write_schema(
    df: pd.DataFrame, file_path: str, part_cols: List[str], write_by_icd_group=False
):
    """Wrap some shared functionality when writing out the new schema to disk.

    Args:
        df: MS data with updated schema
        file_path: Parent parquet directory to write to
        part_cols: List of columns to use as partitions
        write_by_icd_group: Whether to use icd_group_id to subset the data
        when writing. Only applicable to ICD mart data. Defaults to False.
    """

    if write_by_icd_group:
        subset_df_writer(df=df, file_path=file_path, part_cols=part_cols)
    else:
        df.to_parquet(file_path, partition_cols=part_cols, compression=constants.COMP)


def get_dx_cols() -> List[str]:
    """MS data has up to 15 diagnosis columns."""
    return [f"dx_{i}" for i in range(1, 16)]


def get_col_dtypes() -> Dict[str, str]:
    """Returns a dictionary of {column_name: data_type} to use in downcasting data"""

    col_dtypes: Dict[str, str] = {
        "service_start": "datetime",
        "facility_id": "float",
        "diagnosis_id": "integer",
        "cause_code": "category",
        "dxver": "integer",
        "code_system_id": "integer",
        "age": "integer",
        "sex": "integer",
        "sex_id": "integer",
        "mhsacovg": "float",
        "medadv": "float",
        "dstatus": "integer",
        "location_id": "integer",
        "egeoloc": "integer",
        "year": "integer",
        "year_id": "integer",
        "is_duplicate": "integer",
        "in_std_sample": "integer",
        "is_otp": "integer",
        "icd_group_id": "integer",
        "bundle_id": "integer",
    }
    dx_cols = get_dx_cols()

    col_dtypes.update(dict(zip(dx_cols, ["category"] * len(dx_cols))))
    return col_dtypes


def get_rename_dict() -> Dict[str, str]:
    """Raw MS data column names to standard clinical claims processing column names."""
    rename_dict = {
        "admdate": "service_start",
        "svcdate": "service_start",
        "dtstart": "service_start",
        "enrolid": "bene_id",
        "sex": "sex_id",
        "stdplac": "facility_id",
    }
    rename_dict.update(dict(zip([f"dx{i}" for i in range(1, 16)], get_dx_cols())))
    return rename_dict


def fill_null_locations(df: pd.DataFrame) -> pd.DataFrame:
    """Fill nulls locations in a DataFrame with the appropriate values.
    Will fill for egeoloc and location_id and is not case sensitive."""
    location_cols = [
        col
        for col in constants.NULL_LOCATION_FILLS.keys()
        if col in df.columns or col.upper() in df.columns
    ]
    for col in location_cols:
        fill_val = constants.NULL_LOCATION_FILLS[col]
        if col not in df.columns:
            col = col.upper()
        df[col] = df[col].fillna(fill_val)

    return df
