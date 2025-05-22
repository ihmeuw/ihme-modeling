from pathlib import Path
from typing import Any, List, Optional, Tuple

import pandas as pd
from crosscutting_functions.deduplication.estimates import IdentEstimate
from crosscutting_functions.general_purpose import read_wrapper
from crosscutting_functions.clinical_map_tools.icd_groups.use_icd_group_ids import UseIcdGroups
from loguru import logger

from marketscan.pipeline.lib import config
from marketscan.schema import file_handlers as fh
from marketscan.schema import manage_year_tracker


def cast_col_dtypes(df: pd.DataFrame, dtypes: dict) -> pd.DataFrame:
    """Parquet uses the file system to store values in the partition columns, and when they're
    read in with Pandas they're converted to 'category' dtype.
    This casts these columns to numeric if the target data type defined in dtypes is either
    'float' or 'integer'.
    """
    if len(df) > 0:
        for col, dtype in dtypes.items():
            if col in df.columns:
                if dtype in ["float", "integer"]:
                    df[col] = pd.to_numeric(df[col], downcast=dtype)
        # bene_id should be int64 regardless of actual size
        if "bene_id" in df.columns:
            df["bene_id"] = df["bene_id"].astype(int)
    return df


def validate_filters(filters: List[Tuple[str, str, Any]]) -> None:
    """Order and operators for filters must be in a certain order or the filepath will
    not be built correctly. 

    Args:
        filters: List of tuples of filter arguments.
    """

    exp_order = ["is_duplicate", "in_std_sample", "is_otp", "icd_group_id"]
    if exp_order != [e[0] for e in filters]:
        raise RuntimeError(f"Filter partition cols must be in this order: {exp_order}")

    if [e[1] for e in filters] != ["="] * len(filters):
        raise ValueError("Filter operator must be '='")

    if [isinstance(e[2], int) for e in filters] != [True] * len(filters):
        raise ValueError("Filter values must be integer type")
    return


def read_and_concat(filepath: str, a_filter: List[Tuple[str, str, Any]]) -> pd.DataFrame:
    """Read all parquet files from a directory and concat them together into a single df.

    Args:
        filepath: Directory of parquet files to read.
        a_filter: A filter object to use with predicate pushdown while reading.

    Returns:
        Concatenated and filtered data if any parquet files are present. If not an empty df.
    """
    read_dir = filepath
    validate_filters(a_filter)
    for f in a_filter:
        read_dir += "/" + "".join(str(s) for s in f)
    # regex to get all parquet files from the read dir
    files = list(Path(read_dir).glob("*.parquet"))
    if not files:
        return pd.DataFrame({})
    else:
        df = pd.concat([pd.read_parquet(f) for f in files])
        return df


def replace_in_with_equal(
    filters: List[Tuple[str, str, Any]],
) -> List[List[Tuple[str, str, Any]]]:
    """Generate a list of filters if the icd_group_id filter is using the 'in' operator.

    Args:
        filters: list of tuples matching the pyarrow/pandas "filters" arg for reading
        parquet files.

    Returns:
        Nested list of tuples.
    """
    filters_list = []

    for f in filters:
        if f[1] == "in":
            if f[0] != "icd_group_id":
                raise ValueError("Cannot convert non icd group ids from in to equals")
            for val in f[2]:
                new_filter = filters.copy()
                new_filter.append((f[0], "=", val))
                new_filter.remove(f)
                filters_list.append(new_filter)
    if not filters_list:
        return [filters]
    else:
        return filters_list


def read_marketscan_parquet(
    filepath: str, filters: List[Tuple[str, str, Any]]
) -> pd.DataFrame:
    """This wraps pd.read_parquet, works with the same arguments (only in the
    context of processing this Marketscan data), and builds a set of filepaths for the the data
    that will need to be pulled given a List[tuple] of filter values.

    Args:
        filepath: filepath of marketscan 'ICD-mart' data restructed into new schema.
        filters: Matches the filters used in pd.read_parquet.

    Returns:
        Marketscan data matching the filter values.
    """

    df_list = []
    for year in manage_year_tracker.get_all_years():
        year_file_path = "FILEPATH"
        for a_filter in replace_in_with_equal(filters):
            if filters[2][0] != "is_otp":
                for otp_val in [0, 1]:
                    otp_filter = a_filter.copy()
                    otp_filter.insert(2, ("is_otp", "=", otp_val))
                    df = read_and_concat(filepath=year_file_path, a_filter=otp_filter)
                    df["year_id"] = year
                    for filter_vals in otp_filter:

                        if filter_vals[1] != "=":
                            raise ValueError("Only equals filter operator is supported")
                        df[filter_vals[0]] = filter_vals[2]
                    # append df with all cols to return list
                    df_list.append(df)
            # otherwise pull in only the is_otp value identified with the filter
            else:
                df = read_and_concat(filepath=year_file_path, a_filter=a_filter)
                df["year_id"] = year
                for filter_vals in a_filter:
                    # same assignment as above
                    if filter_vals[1] != "=":
                        raise ValueError("Only equals filter operator is supported")
                    df[filter_vals[0]] = filter_vals[2]
                # append df with all cols to return list
                df_list.append(df)
    logger.debug(f"{len(df_list)} ICD-Mart files were read for this bundle")
    return cast_col_dtypes(pd.concat(df_list, sort=False), fh.get_col_dtypes())


def get_icd_group_lookup() -> pd.DataFrame:
    icd_group_obj = UseIcdGroups()
    icd_group_obj.get_icd_group_lookup()
    return icd_group_obj.icd_groups


def pull_bundle_data(
    filepath: str,
    bundle_id: int,
    estimate: IdentEstimate,
    map_version: int,
    reader: str,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Main function for reading the Marketscan ICD-mart data into a Pandas dataframe.

    Args:
        filepath: Location of the ICD-mart to read.
        bundle_id: Bundle used to determine which icd_group_ids to pull from the mart.
        estimate: Used to determine whether or not outpatient data
                  should be read.
        map_version: Standard clinical map_version.
        reader: Underlying read method. Can be "ms_wrapper" or "pandas". ms_wrapper is
                much faster but needs to build a series of filepaths to read from. 
        columns: Subset of columns to return. None will return all columns present in
                 the ICD-mart. Defaults to None.

    Returns:
        Marketscan ICD-mart data.
    """
    filters = [("is_duplicate", "=", 0), ("in_std_sample", "=", 1)]

    if not set(estimate.is_otp_vals).symmetric_difference((0,)):
        filters.append(("is_otp", "=", 0))

    icd_group_obj = UseIcdGroups()
    group_ids = icd_group_obj.get_bundle_group_ids(
        bundle_id=bundle_id, map_version=map_version
    )
    group_ids = tuple(int(i) for i in group_ids)

    filters.append(("icd_group_id", "in", group_ids))
    if len(group_ids) == 0:
        raise ValueError("There's no data for these filters")

    logger.debug(f"The {reader} method will be used to read in the ICD-mart data")
    if reader == "pandas":
        df = pd.read_parquet(path=filepath, filters=filters, columns=columns)
    elif reader == "ms_wrapper":
        df = read_marketscan_parquet(filepath=filepath, filters=filters)
    else:
        raise ValueError(f"Can't read with {reader}")
    logger.debug(f"A dataframe with shape {df.shape} was read from the ICD-mart")
    # Attach code system id.
    icd_group_df = get_icd_group_lookup()[["icd_group_id", "code_system_id"]]
    df = df.merge(icd_group_df, how="left", on="icd_group_id", validate="m:1")

    nulled = df["location_id"].isnull().sum()
    df = fh.fill_null_locations(df=df)
    logger.info(f"Filled {nulled} location values, {100 * nulled / len(df)}% of dx's.")

    cast_col_dtypes(df=df, dtypes=fh.get_col_dtypes())

    if columns is not None:
        return df[columns]
    else:
        return df


def pull_denominator_data(filepath: str) -> pd.DataFrame:
    """Given a filepath of ICD mart denominator data with sample sizes read and return after
    renaming and modifying year dtype (categorical due to being a parquet partition)."""
    denominator_df = pd.read_parquet(filepath)
    denominator_df = denominator_df.rename(columns={"year": "year_id"})
    denominator_df = fh.fill_null_locations(df=denominator_df)
    denominator_df = fh.downcast_col_dtypes(denominator_df)
    return denominator_df


def write_wrapper(
    df: pd.DataFrame, write_path: str, file_format: str, overwrite: bool = True
) -> None:
    """Wraps various pandas.DataFrame.to_{format} methods with a few preferred args."""
    extensions = {"parquet": ".parquet", "hdf": ".H5", "csv": ".csv"}
    write_path += extensions[file_format]

    # check for existing data
    if Path(write_path).exists() and not overwrite:
        raise RuntimeError(
            f"The underlying filepath {write_path} exists and overwrite is not set to True"
        )

    if file_format == "parquet":
        df.to_parquet(write_path, index=False, compression="zstd")
    elif file_format == "hdf":
        df.to_hdf(write_path, mode="w", complib="blosc", complevel=6)
    elif file_format == "csv":
        df.to_csv(write_path, index=False)
    else:
        raise ValueError(f"Unknown storage file format {file_format}")


def compile_gbd_results(run_id: int) -> None:
    """This function will compile all of the final gbd files and write them as a CSV to
    another location within a clinical run.

    The Clinical upload process expects a single csv of all Marketscan bundle estimates for
    GBD. It then performs some final renaming/cleaning and adjusts maternal denominators.
    """

    # read and compile individual task output final files
    settings = config.get_settings()
    read_dir = "FILEPATH"
    read_paths = Path(read_dir).glob(f"*.{settings.file_format}")
    df = pd.concat(
        [read_wrapper(read_path, settings.file_format) for read_path in read_paths],
        sort=False,
        ignore_index=True,
    )

    # write the results to a csv to pass to the final claims step and the Clinical Uploader
    write_path = (
        "FILEPATH"
    )
    write_wrapper(df=df, write_path=write_path, file_format="csv", overwrite=False)
