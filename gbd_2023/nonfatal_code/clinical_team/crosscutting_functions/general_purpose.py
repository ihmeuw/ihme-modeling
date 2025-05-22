import datetime
import getpass
import itertools
import os
import re
import subprocess
import time
from typing import Any, Dict, List, Optional

import pandas as pd


def cat_to_str(df: pd.DataFrame) -> pd.DataFrame:
    """Convert a set of hard-coded columns from the categorical data type to string.

    Raises:
        ValueError if any categorical datatypes remain present in the dataframe.
    """

    cats = ["source", "facility_id", "outcome_id", "icg_name", "icg_measure"]
    cats = [col for col in cats if col in df.columns]
    for col in cats:
        if isinstance(df[col].dtype, pd.CategoricalDtype):
            df[col] = df[col].astype(str)
    for col in df.columns:
        if isinstance(df[col].dtype, pd.CategoricalDtype):
            raise ValueError("We do not expect any categorical columns")

    return df


def downcast_clinical_cols(
    df: pd.DataFrame, str_to_cats: bool, verbose: bool = False
) -> pd.DataFrame:

    if verbose:
        pre = (df.memory_usage(deep=True) / 1024**2).sum()

    cols = df.columns.tolist()
    ints = [
        "year_start",
        "year_end",
        "location_id",
        "age_group_unit",
        "sex_id",
        "nid",
        "representative_id",
        "diagnosis_id",
        "metric_id",
        "icg_id",
        "age_group_id",
    ]
    floats = [
        "product",
        "upper_product",
        "lower_product",
        "mean",
        "upper",
        "lower",
        "val",
        "cause_fraction",
    ]
    cats = ["source", "facility_id", "outcome_id", "icg_name", "icg_measure"]
    ints = [col for col in ints if col in cols]
    floats = [col for col in floats if col in cols]
    cats = [col for col in cats if col in cols]

    for col in cols:
        if col in ints:
            df[col] = pd.to_numeric(df[col], downcast="integer", errors="raise")
        elif col in floats:
            df[col] = pd.to_numeric(df[col], downcast="float", errors="raise")
        elif col in cats:
            if str_to_cats:
                df[col] = df[col].astype("category")
        else:
            print(f"{col} not recognized as int or float or str")

    if verbose:
        post = (df.memory_usage(deep=True) / 1024**2).sum()
        mem_reduction = round((1 - (post / pre)) * 100, 2)
        print(
            f"We've reduced mem usage by {mem_reduction}% from"
            f" {round(pre, 0)}Mb to {round(post, 0)}Mb"
        )
    return df


def natural_sort(lst, key=lambda s: s):
    """
    Sort a list into reverse natural alphanumeric order. eg ["11", "1", "22"] becomes
    ["22", "11", "1"]
    """

    def get_alphanum_key_func(key):
        convert = lambda text: int(text) if text.isdigit() else text  # noqa
        return lambda s: [convert(c) for c in re.split("([0-9]+)", key(s))]  # noqa

    sort_key = get_alphanum_key_func(key)
    lst.sort(key=sort_key, reverse=True)


def expandgrid(*itrs: List[Any]) -> Dict[str, Any]:
    """Create a template dictionary with every possible combination of input lists."""
    product = list(itertools.product(*itrs))
    return {"Var{}".format(i + 1): [x[i] for x in product] for i in range(len(itrs))}


def write_hosp_file(df: pd.DataFrame, write_path: str, backup: bool = True) -> None:
    """
    write and optionally backup a pandas dataframe using our standard HDF
    compression and mode settings

    Args:
        df: The data to write to file
        write_path: a directory and file name to write to. The directory will be used
                    as the parent directory for the _archive folder
        backup: should the data be backed up in the _archive folder
    """

    # create the _archive dir if it doesn't exist
    if not os.path.isdir(os.path.dirname(write_path)):
        os.makedirs(os.path.dirname(write_path))

    # write df to path
    df.to_hdf(write_path, key="df", format="table", complib="blosc", complevel=5, mode="w")

    if backup:
        today = datetime.datetime.today().strftime("%Y_%m_%d")
        # write df to path/_archive
        # extract write dir and file name
        main_dir = os.path.dirname(write_path)
        file_name = os.path.basename(write_path)

        archive_dir = main_dir + "/_archive"
        # create the _archive dir if it doesn't exist
        if not os.path.isdir(archive_dir):
            os.makedirs(archive_dir)
        archive_path = archive_dir + "/{}_{}".format(today, file_name)

        # write dated archive file
        df.to_hdf(
            archive_path, key="df", format="table", complib="blosc", complevel=5, mode="w"
        )

    return


def job_holder(job_name: str, sleep_time: int, init_sleep: int = 60) -> None:
    """Checks the users squeue every n seconds to see if any jobs are
    still running

    Args:
        job_name: Name of the job.
        sleep_time: Time in seconds to sleep between squeue calls
        init_sleep: Time in seconds before initally checking squeue
    """
    print("..sleep..")
    time.sleep(init_sleep)

    status = "wait"
    while status == "wait":
        USER = getpass.getuser()
        p = subprocess.Popen(["squeue", "-u", f"{USER}"], stdout=subprocess.PIPE)
        # get squeue text
        squeue_txt = p.communicate()[0].decode("utf-8")
        # search squeue text for our job name
        job_count = squeue_txt.count(job_name)
        if job_count == 0:
            status = "go"
            print("Continue processing data")
        else:
            print("..keep sleeping..")
            time.sleep(sleep_time)
    return


def to_int(df: pd.DataFrame, integer_columns: Optional[List[str]] = None) -> pd.DataFrame:
    """Casts columns to integer datatype.

    Args:
        df (pd.DataFrame): Table with ID columns.
        integer_columns (Optional[List[str]]): Columns to cast as integer.
            Defaults to None which will operate on all columns ending in '_id'
            and the column 'nid'.

    Returns:
        pd.DataFrame: Input DataFrame with downcast ID columns.
    """
    if not integer_columns:
        integer_columns = [col for col in df.columns if col.endswith("_id")]
        if "nid" in df.columns:
            integer_columns.append("nid")

    for col in integer_columns:
        df[col] = pd.to_numeric(df[col], downcast="integer")

    return df
