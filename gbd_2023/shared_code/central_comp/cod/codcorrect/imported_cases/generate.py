"""Runs imported cases generation for a given cause."""

import functools
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from typing import List

import pandas as pd

import db_queries
from save_results import save_results_cod

from imported_cases.lib import constants, core, log_utils

logger = logging.getLogger(__name__)


def run_imported_cases(
    cause_id: int, output_dir: str, release_id: int, version_id: int, test: int
) -> None:
    """Run imported cases process for a cause: generate and save.

    Args:
        cause_id: ID of the cause to run imported cases process
        output_dir: output directory
        release_id: ID of the release for the run
        version_id: ID of the run version
        test: if run is test (1) or not (0). Don't mark best if test
    """
    log_utils.configure_logging()
    output_dir = os.path.join(output_dir, str(cause_id))

    result = generate_imported_cases(
        cause_id=cause_id, output_dir=output_dir, release_id=release_id, version_id=version_id
    )

    # if we get non-0 return value, that means there's nothing to save so exit early
    if result:
        return

    save_results_cod(
        output_dir,
        constants.SAVE_FILE_PATTERN,
        cause_id=cause_id,
        description=f"Imported cases v{version_id}",
        release_id=release_id,
        model_version_type_id=constants.IMPORTED_CASES_MODEL_VERSION_TYPE_ID,
        mark_best=(not bool(test)),
    )

    logger.info("All Done")


def generate_imported_cases(
    cause_id: int, output_dir: str, release_id: int, version_id: int
) -> int:
    """Generate imported cases for the given cause.

    Uses CoD data to determine what deaths are recorded for
    causes in restricted locations. If no data is found in these locations,
    the program exits. For locations with non-zero deaths for the cause,
    1000 draws are taken based on the number of deaths and sample size of the study.
    The data set is then squared, filling missing dimensions with 0s, and uploaded
    via save_results_cod under model version type id 7 (imported cases).

    Args:
        cause_id: ID of the cause to run imported cases process
        output_dir: output directory
        release_id: ID of the GBD round for the run
        version_id: ID of the run version

    Returns:
        int: sentinal value, 0 if data was found, formatted and saved to continue to
            save_results_cod. 1 if no data was found and program should exit.
    """
    CONTINUE = 0
    END_EARLY = 1

    logger.info("Get restricted locations")
    if cause_id in constants.CAUSE_AGE_RESTRICTIONS:
        age_group_id_lower, age_group_id_upper = constants.CAUSE_AGE_RESTRICTIONS[cause_id]
        location_ids = core.get_data_rich_locations(release_id)
        age_group_ids = core.get_age_group_ids(
            release_id,
            age_group_id_lower=age_group_id_lower,
            age_group_id_upper=age_group_id_upper,
        )

        # Pull age groups from 0 to EXTEND_TO_AGE_GROUP_ID for creating square data,
        # filling with 0s if the cause is limited to younger age groups.
        square_age_group_ids = core.get_age_group_ids(
            release_id, age_group_id_upper=constants.EXTEND_TO_AGE_GROUP_ID
        )

        logger.info(
            "Age restricted cause: pulling data rich locations and age groups "
            f"{', '.join(str(i) for i in age_group_ids)}."
        )
    else:
        location_ids = core.get_cause_specific_locations(cause_id, release_id)
        age_group_ids = core.get_age_group_ids(release_id)
        square_age_group_ids = age_group_ids

    logger.info("Get CoD data for cause")
    data = db_queries.get_cod_data(
        cause_id=cause_id,
        age_group_id=age_group_ids,
        location_id=location_ids,
        release_id=release_id,
    )

    if data.empty:
        logger.info(
            "No data pulled from get_cod_data for the following inputs:\n"
            f"cause_id: {cause_id}\nage_group_id: {age_group_ids}\n"
            f"location_id: {location_ids}\nrelease_id: {release_id}\n"
            "Ending."
        )
        return END_EARLY

    # Clean up data some
    data = data[
        [
            "cause_id",
            "location_id",
            "year_id",
            "sex_id",
            "age_group_id",
            "cf",
            "deaths",
            "sample_size",
        ]
    ]

    data = core.subset_to_restricted_location_years(data, release_id)

    if data.empty:
        logger.info("No data after subsetting to restricted location-years. Ending.")
        return END_EARLY

    logger.info("Get beta distribution for data")
    data_by_year = [data[data.year_id == year_id] for year_id in data.year_id.unique()]

    beta_distribution_helper = functools.partial(core.generate_distribution)
    with ProcessPoolExecutor(constants.NUM_CORES) as pool:
        distributions = pool.map(beta_distribution_helper, data_by_year)

    data = pd.concat(distributions)
    square_and_save_data(data, cause_id, square_age_group_ids, release_id, output_dir)

    return CONTINUE


def square_and_save_data(
    data: pd.DataFrame,
    cause_id: int,
    age_group_ids: List[int],
    release_id: int,
    output_dir: str,
) -> None:
    """Make data square and save for upload.

    Note:
        This function will create a square data set but only for the age groups passed in.
        For age-restricted causes like opioids, this means that some age groups will not
        be generated.

    Args:
        data: imported cases dataset
        cause_id: ID of the cause to run imported cases process
        age_group_ids: valid age groups IDs for the cause for imported cases
        release_id: ID of the GBD round for the run
        output_dir: output directory
    """
    logger.info("Generate square data")
    square_data = core.make_square_data(cause_id, age_group_ids, release_id)

    logger.info("Combine imported cases and square data")
    data = pd.concat([data, square_data]).reset_index(drop=True)
    data = data.groupby(
        ["location_id", "year_id", "sex_id", "age_group_id", "cause_id"], as_index=False
    ).sum()

    logger.info("Saving Data")
    data.to_csv(os.path.join(output_dir, constants.SAVE_FILE_PATTERN), index=False)
