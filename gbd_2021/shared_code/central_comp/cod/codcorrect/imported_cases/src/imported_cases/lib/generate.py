"""Runs imported cases generation for a given cause."""
import functools
import logging
import os
from concurrent.futures import ProcessPoolExecutor
from typing import List

import db_queries
import pandas as pd
from save_results import save_results_cod

from imported_cases.lib import constants, core, log_utils


def run_imported_cases(
    cause_id: int, output_dir: str, gbd_round_id: int, decomp_step: str, version_id: int
) -> None:
    """Run imported cases process for a cause: generate and save.

    Args:
        cause_id: ID of the cause to run imported cases process
        output_dir: output directory
        gbd_round_id: ID of the GBD round
        decomp_step: decomp step
        version_id: ID of the run version
    """
    output_dir = os.path.join(output_dir, str(cause_id))
    log_utils.setup_logging(
        os.path.join(output_dir, "logs"), f"generate_imported_cases_{cause_id}"
    )

    result = generate_imported_cases(
        cause_id=cause_id,
        output_dir=output_dir,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
        version_id=version_id,
    )

    # if we get non-0 return value, that means there's nothing to save so exit early
    if result:
        return

    save_results_cod(
        output_dir,
        constants.SAVE_FILE_PATTERN,
        cause_id=cause_id,
        description=f"Imported cases v{version_id}",
        decomp_step=decomp_step,
        gbd_round_id=gbd_round_id,
        model_version_type_id=constants.IMPORTED_CASES_MODEL_VERSION_TYPE_ID,
        mark_best=True,
    )

    logging.info("All Done")


def generate_imported_cases(
    cause_id: int, output_dir: str, gbd_round_id: int, decomp_step: str, version_id: int
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
        gbd_round_id: ID of the GBD round
        decomp_step: decomp step
        version_id: ID of the run version

    Returns:
        int: sentinal value, 0 if data was found, formatted and saved to continue to
            save_results_cod. 1 if no data was found and program should exit.
    """
    CONTINUE = 0
    END_EARLY = 1

    logging.info("Special cause considerations")

    logging.info("Get restricted locations")
    if cause_id == constants.OPIOIDS_ID:
        location_ids = core.get_data_rich_locations(gbd_round_id, decomp_step)
        age_group_ids = core.get_age_group_ids(gbd_round_id, age_group_years_upper=15)
    else:
        location_ids = core.get_cause_specific_locations(cause_id, gbd_round_id, decomp_step)
        age_group_ids = core.get_age_group_ids(gbd_round_id)

    logging.info("Get CoD data for cause")
    data = db_queries.get_cod_data(
        cause_id=cause_id,
        age_group_id=age_group_ids,
        location_id=location_ids,
        gbd_round_id=gbd_round_id,
        decomp_step=decomp_step,
    )

    if data.empty:
        logging.info("No data found. Ending.")
        return END_EARLY

    # Clean up data some
    data = data[
        [
            "cause_id",
            "location_id",
            "year",
            "sex",
            "age_group_id",
            "cf",
            "deaths",
            "sample_size",
        ]
    ]
    data.rename(columns={"year": "year_id", "sex": "sex_id"}, inplace=True)

    logging.info("Get beta distribution for data")
    data_by_year = [data[data.year_id == year_id] for year_id in data.year_id.unique()]

    beta_distribution_helper = functools.partial(core.generate_distribution)
    with ProcessPoolExecutor(constants.NUM_CORES) as pool:
        distributions = pool.map(beta_distribution_helper, data_by_year)

    data = pd.concat(distributions)
    square_and_save_data(data, cause_id, age_group_ids, gbd_round_id, output_dir)

    return CONTINUE


def square_and_save_data(
    data: pd.DataFrame,
    cause_id: int,
    age_group_ids: List[int],
    gbd_round_id: int,
    output_dir: str,
) -> None:
    """Make data square and save for upload.

    Args:
        data: imported cases dataset
        cause_id: ID of the cause to run imported cases process
        age_group_ids: valid age groups IDs for the cause for imported cases
        gbd_round_id: ID of the GBD round
        output_dir: output directory
    """
    logging.info("Generate square data")
    square_data = core.make_square_data(cause_id, age_group_ids, gbd_round_id)

    logging.info("Combine imported cases and square data")
    data = pd.concat([data, square_data]).reset_index(drop=True)
    data = data.groupby(
        ["location_id", "year_id", "sex_id", "age_group_id", "cause_id"], as_index=False
    ).sum()

    logging.info("Saving Data")
    data.to_csv(os.path.join(output_dir, constants.SAVE_FILE_PATTERN), index=False)
