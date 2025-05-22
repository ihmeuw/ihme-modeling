""""
Pull and cache GBD population.
This is the denominator for POL
"""

import itertools
from pathlib import Path
from typing import List

import pandas as pd
from crosscutting_functions.clinical_constants.pipeline import poland as constants
from crosscutting_functions.clinical_constants.pipeline.poland import POL_GBD_LOC_ID
from crosscutting_functions.clinical_constants.pipeline.schema import pol_pipeline
from crosscutting_functions.clinical_metadata_utils.pipeline_wrappers import ClaimsWrappers
from crosscutting_functions import demographic
from crosscutting_functions.get-cached-population import cached_pop_tools

from pol_nhf.constants import CURRENT_RELEASE_ID
from pol_nhf.utils.settings import PipelineAgeSettings


def pull_clinical_age_groups() -> List[int]:
    """Retrieve a list of GBD age group ids associated to a clinical age group id."""

    ages = demographic.get_hospital_age_groups(PipelineAgeSettings.clinical_age_group_id)
    return ages.age_group_id.unique().tolist()


def pull_location_ids() -> List[int]:
    """Retrieve a list of subnational and national level location ids."""
    location_ids = demographic.get_child_locations(
        location_parent_id=POL_GBD_LOC_ID, release_id=CURRENT_RELEASE_ID, return_dict=False
    )
    location_ids.append(constants.POL_GBD_LOC_ID)
    return location_ids


def create_square_demo_df() -> pd.DataFrame:
    """Create dataframe that is a square demographic data set."""
    age_group_ids = pull_clinical_age_groups()
    location_ids = pull_location_ids()
    sex_ids = list(constants.SEX_ID.values())
    product = list(itertools.product(age_group_ids, sex_ids, location_ids, constants.YEARS))
    return pd.DataFrame(
        product, columns=["age_group_id", "sex_id", "location_id", "year_start"]
    )


def get_population(run_id: int, pop_run_id: int) -> pd.DataFrame:
    """Pull population from central GBD tables.

    Args:
        run_id (int): Clinical run_id found in DATABASE.
        pop_run_id (int): Central IHME population run_id.

    Returns:
        pd.DataFrame: Male and female population estimates.
    """
    df = create_square_demo_df()
    pop = cached_pop_tools.get_source_pop(run_id=run_id, pop_run_id=pop_run_id, df=df)
    # above function mannually inserts sex_id 3
    pop = pop[pop.sex_id.isin(list(constants.SEX_ID.values()))]
    return pop


def get_cache_path(run_id: int) -> str:
    """For a given run_id build a path to a population cache.

    Args:
        run_id (int): Clinical run_id found in DATABASE.

    Returns:
        str: Path to cached population.
    """
    return (
        "FILEPATH"
    )


def save_population(df: pd.DataFrame, run_id: int) -> None:
    """Save population df in a given run.

    Args:
        df (pd.DataFrame): Population estimates to be saved.
        run_id (int): Clinical run_id found in DATABASE.
    """
    path = get_cache_path(run_id)
    df.to_parquet(path)


def check_file(run_id: int):
    path = get_cache_path(run_id)
    return Path(path).is_file()


def main(run_id: int) -> None:
    """Retrieve Poland national and subnational population estimates and save
    to run_id's run directory.

    Args:
        run_id (int): Clinical run_id found in DATABASE.
    """
    cw = ClaimsWrappers(run_id=run_id, odbc_profile="SERVER")
    run_metadata = cw.pull_run_metadata()
    pop_run_id = run_metadata.population_run_id.iloc[0]
    pop = get_population(run_id=run_id, pop_run_id=pop_run_id)
    pol_pipeline.DenominatorSchema(pop)
    save_population(df=pop, run_id=run_id)
