"""
Tools related to storing and pulling cached populations.
"""

import glob
import os
from pathlib import PosixPath
from typing import List, Optional, Tuple, Union

import pandas as pd
from db_queries import get_population

from crosscutting_functions import constants, demographic
from crosscutting_functions.pipeline import get_release_id


def write_all_source_pops(run_id: int) -> None:
    """Generate a population cache for all sources _after_ master data has been run.

    Population caches should be created automatically within master data, so this should
    only be done afterward.

    Arguments:
        run_id: The clinical run ID.
    """
    files = get_md_files(run_id=run_id)
    for file in files:
        source_name = os.path.basename(file)[:-3]
        pop = get_source_pop(file=file, run_id=run_id)
        write_source_pop(pop=pop, run_id=run_id, source_name=source_name)


def get_cached_pop(
    run_id: int, sum_under1: bool = False, drop_duplicates: bool = True
) -> pd.DataFrame:
    """Returns a df of the full population cache for a given run_id.

    Arguments:
        run_id: Determines which cached pop run to pull.
        sum_under1: If True, aggregate population under 1.
        drop_duplicates: Some locations/years are duplicated in inp data; get rid of dupes!

    Raises:
        ValueError: If there are multiple population run_ids present in the cached pop files.

    Returns:
        The full population cache for a given run_id.
    """
    pop_files = _get_pop_filepaths(run_id=run_id)
    pop = pd.concat([pd.read_csv(f) for f in pop_files], sort=False, ignore_index=True)

    if pop["pop_run_id"].unique().size != 1:
        raise ValueError(
            f"There are multiple population run_ids present: {pop['pop_run_id'].unique()}"
        )
    if drop_duplicates:
        pop = pop.drop_duplicates()

    if sum_under1:
        groups = pop.drop("population", axis=1).columns.tolist()
        pop = demographic.sum_under1_data(
            df=pop, group_cols=groups, sum_cols=["population"], clinical_age_group_set_id=2
        )
    return pop


def check_pop_files(run_id: int, exp_sources: List[str]) -> None:
    """Compares the CSVs present for a single run against what we'd expect from the database.

    Used in `master_data` to confirm all parallel jobs finished.

    Arguments:
        run_id: Determines which cached pop run to pull.
        exp_sources: The list of sources we expect to be present.

    Raises:
        ValueError: If there are missing files.
    """
    pop_files = _get_pop_filepaths(run_id=run_id)

    pop_files = [os.path.basename(pf)[:-4] for pf in pop_files]

    err_msg = ""
    missing_pop_files = set(exp_sources).difference(pop_files)
    missing_exp_sources = set(pop_files).difference(exp_sources)
    if missing_pop_files:
        err_msg += f" Missing pop_files: {missing_pop_files}"
    if missing_exp_sources:
        err_msg += f" Missing exp_sources: {missing_exp_sources}"
    if err_msg:
        raise ValueError(err_msg)


def get_md_files(run_id: int) -> List[str]:
    """Get a list of filepaths for a master data run.

    Arguments:
        run_id: Determines which cached pop run to pull.

    Returns:
        A list of filepaths for a master data run.
    """
    ppath = _get_master_data_path(run_id=run_id)
    files = glob.glob(f"{ppath}/*.H5")
    files = sorted(files)
    return files


def get_source_pop(
    run_id: int,
    pop_run_id: Optional[int] = None,
    file: Optional[Union[str, PosixPath]] = None,
    df: Optional[pd.DataFrame] = None,
    clinical_age_group_set_id: int = 2,
) -> pd.DataFrame:
    """Get the population set for a single source.

    Arguments:
        run_id: Identifies the Clinical pipeline run_id.
        pop_run_id: Identifies the population run_id.
        file: The master data filepath to read in. If `None`, you must pass `df`.
        df: The master data file. If `None`, you must pass `file`.
        clinical_age_group_set_id: The clinical age group set ID.

    Raises:
        ValueError: If both file and df are `None`.
        ValueError: If there are overlapping age groups between pop and tmp.
        ValueError: If the columns between pop and tmp are not identical.

    Returns:
        A df of the population estimates for a single source.
    """
    if file is None and df is None:
        raise ValueError("We must have either `file` or `df`, but both are `None`.")
    if isinstance(file, str) and isinstance(df, pd.DataFrame):
        raise ValueError("We must have either `file` or `df`, but not both.")

    ages, sexes, locations, years, tmp = _get_demos(
        file=file, df=df, clinical_age_group_set_id=clinical_age_group_set_id
    )

    pop = get_population(
        release_id=get_release_id(run_id=run_id),
        run_id=pop_run_id,
        age_group_id=ages,
        sex_id=sexes,
        location_id=locations,
        year_id=years,
    )

    pop = pop.rename(columns={"run_id": "pop_run_id"})

    # Confirm pop outputs match input data.
    validate_pop_results(
        pop=pop, tmp=tmp, ages=ages, clinical_age_group_set_id=clinical_age_group_set_id
    )

    return pop


def write_source_pop(pop: pd.DataFrame, run_id: int, source_name: str) -> None:
    """Write a single pop file to a given run.

    Used in `master_data` and `write_all_source_pops`.

    Arguments:
        pop: Dataframe of GBD population estimates.
        run_id: Clinical run ID.
        source_name: The source name we use to process data.
    """
    print(f"Writing {source_name} to csv\n")
    write_path = _get_cached_pop_path(run_id=run_id, source_path=source_name)
    if os.path.exists(write_path):
        print("There is already a cached pop file present.")
        print("Appending population to existing files.")
        existing_df = pd.read_csv(write_path)
        pop = pd.concat([pop, existing_df], sort=False, ignore_index=True)

        # Drop duplicates. This should also be done as pop files are read in.
        # pop is a float and doesn't play nicely with drop duplicates.
        pop = pop.drop_duplicates(subset=constants.POP_ID_COLS)

    validate_id_cols(pop=pop, id_cols=constants.POP_ID_COLS)
    pop.to_csv(write_path, index=False)


def validate_id_cols(pop: pd.DataFrame, id_cols: list) -> None:
    """Validate that the data is square.

    Arguments:
        pop: The population data.
        id_cols: The columns to check.

    Raises:
        ValueError: If there are non-uniform unique values for any of the `id_cols`.
    """
    failed_cols = []
    for year in pop["year_id"].unique():
        year_pop = pop[pop["year_id"] == year]
        for col in id_cols:
            unique_col_values = year_pop[col].value_counts().unique()
            if unique_col_values.size != 1:
                failed_cols.append(col)
    if failed_cols:
        raise ValueError(
            f"These columns don't have uniform unique values by year {failed_cols}."
        )


def validate_pop_results(
    pop: pd.DataFrame,
    tmp: pd.DataFrame,
    ages: List[int],
    sexes: List[int] = constants.SEX_IDS,
    clinical_age_group_set_id: int = 2,
) -> None:
    """Validate that the population demographics returned match the input data.

    Merges pop back onto tmp (pop was created using tmp), among other modifications.

    Arguments:
        pop: Contains GBD population estimates.
        tmp: Contains clinical demographic info.

    Raises:
        ValueError: If there are null values in the merged df.
        ValueError: If the age groups in pop are not what we requested.
    """
    tmp_list = []
    for s in sexes:
        tmp2 = tmp.copy()
        tmp2["sex_id"] = s
        tmp_list.append(tmp2)
    tmp = pd.concat(tmp_list, sort=False, ignore_index=True)
    tmp = tmp.rename(columns={"year_start": "year_id"})

    pop_ages = pop["age_group_id"].unique().tolist()
    # All ages returned by pop should be what we requested.
    age_diff = set(pop_ages) - set(ages)
    if age_diff:
        raise ValueError(f"pop ages not in tmp ages: {age_diff}")

    # Don't need pop for non-good age groups.
    tmp = demographic.retain_good_age_groups(
        df=tmp, clinical_age_group_set_id=clinical_age_group_set_id
    )

    demo_cols = ["age_group_id", "sex_id", "location_id", "year_id"]
    m = tmp.merge(pop, how="left", on=demo_cols)
    if m.isnull().sum().sum() != 0:
        raise ValueError(f"We've got Nulls:\n{m[m.isnull().any(axis=1)]}")


def _get_pop_filepaths(run_id: int) -> List[str]:
    """In case we want to change paths just change this and the writer path below."""
    pop_files = glob.glob(_get_cached_pop_path(run_id=run_id, source_path="*"))
    pop_files = sorted(pop_files)
    return pop_files


def _get_master_data_path(run_id: int) -> str:
    """Get the master data path for a given run_id.

    Arguments:
        run_id: The run_id to get the master data path for.

    Returns:
        The master data path for a given run_id.
    """
    return "FILEPATH"


def _get_demos(
    file: Optional[Union[str, PosixPath]],
    df: Optional[pd.DataFrame],
    clinical_age_group_set_id: int = 2,
) -> Tuple[List[int], List[int], List[int], List[int], pd.DataFrame]:
    """Extract age, loc, and year from master data source file or dataframe.

    Arguments:
        file: The master data filepath to read in. If `None`, you must pass `df`.
        df: The master dataframe. If `None`, you must pass `file`.
        clinical_age_group_set_id: The clinical age group set ID.

    Raises:
        ValueError: If both file and df are `None`.
        ValueError: If both file and df are not `None`.

    Returns:
        ages: The age group IDs present in the master data, plus those used for hospital prep.
        sexes: The sex IDs we use.
        locations: The locations present in the master data.
        years: The years present in the master data.
        tmp: The master data dataframe with duplicates dropped.
    """
    demo_cols = ["age_group_id", "location_id", "year_start"]
    if file is None and df is not None:
        tmp = df[demo_cols].drop_duplicates().copy()
    elif file is not None and df is None:
        print(f"Reading in master data file for {os.path.basename(file)}")
        tmp = pd.read_hdf(file, columns=demo_cols).drop_duplicates()  # type:ignore[assignment]
    elif file is not None and df is not None:
        raise ValueError("Must have either `file` or `df`, but not both.")
    else:
        raise ValueError("Must have a value for `file` or `df`. Both are `None`.")

    # Get the "good" age groups for post age splitting.
    good_ages = (
        demographic.get_hospital_age_groups(
            clinical_age_group_set_id=clinical_age_group_set_id
        )
        .age_group_id.unique()
        .tolist()
    )

    # Each set of demo values should be a list so they play well with shared funcs.
    ages = tmp.age_group_id.unique().tolist()
    ages = list(set(ages + good_ages))
    sexes = constants.SEX_IDS
    locations = tmp.location_id.unique().tolist()
    years = tmp.year_start.unique().tolist()

    return ages, sexes, locations, years, tmp


def _get_cached_pop_path(run_id: int, source_path: str) -> str:
    """Get the cached population path for a given run_id and source_path.

    Arguments:
        run_id: The run_id to get the cached population path for.
        source_path: The source_path to get the cached population path for.

    Returns:
        The cached population path for a given run_id and source_path.
    """
    return "FILEPATH"
