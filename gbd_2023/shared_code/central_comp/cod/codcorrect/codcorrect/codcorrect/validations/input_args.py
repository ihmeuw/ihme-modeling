"""
Helper functions for validating the input arguments
for launching CoDCorrect.
"""

from argparse import Namespace
from typing import List, Optional

import db_queries
import gbd.constants as gbd_constants
from db_tools import config

from codcorrect.legacy.utils.constants import (
    GBD,
    CauseSetId,
    Columns,
    ConnectionDefinitions,
    DataBases,
    Draws,
    LocationSetId,
)


def set_reactive_defaults(args: Namespace) -> None:
    """
    Mutates args, setting defaults for the parameters listed below.

    Defaults:
        * if a test run, if not provided, base cause and location sets are set to
            their test versions. Aggregation location sets are set to []
        * year_ids defaults to the CoD team demographics for the given
            GBD round
        * base_location_set_id defaults to the location set id for a full
            run, which varies by GBD round
        * aggregation_cause_set_ids defaults to an empty list ([])
    """
    if args.test:
        args.aggregation_location_set_ids = []

        if args.base_location_set_id is None:
            args.base_location_set_id = LocationSetId.TEST

        if args.base_cause_set_id is None:
            args.base_cause_set_id = CauseSetId.TEST

        if args.year_ids is None:
            args.year_ids = [2010]

    if args.year_ids is None:
        args.year_ids = db_queries.get_demographics(
            gbd_team="cod", release_id=args.release_id
        )[Columns.YEAR_ID]

    if args.base_location_set_id is None:
        args.base_location_set_id = LocationSetId.STANDARD

    if args.base_cause_set_id is None:
        args.base_cause_set_id = CauseSetId.COMPUTATION

    if args.aggregation_cause_set_ids is None:
        args.aggregation_cause_set_ids = []

    if args.aggregation_location_set_ids is None:
        args.aggregation_location_set_ids = []


def run_input_validations(process: str, args: Namespace) -> None:
    """
    Runs all of the validations writing for the
    inputs for CoDCorrect. There are
    no guarantees this is an exhaustive validation.

    Raises:
        ValueError if invalid inputs are detected
    """
    validate_process_name(process)
    validate_restart_or_resume(args.restart, args.resume)
    validate_restart_has_version_id(args.restart, args.resume, args.version_id, process)
    validate_test(args.test, args.restart, args.resume)
    validate_pct_change_years(args.year_ids, args.year_start_ids, args.year_end_ids)
    validate_release_and_years(args.release_id, args.year_ids)
    validate_n_draws(args.n_draws)
    validate_location_set_ids(args.base_location_set_id, args.aggregation_location_set_ids)
    validate_aggregate_cause_sets(args.base_cause_set_id, args.aggregation_cause_set_ids)
    validate_measure_ids(args.measure_ids)
    validate_databases(args.databases)
    validate_odbc_connections()


def validate_process_name(process: str) -> None:
    """Process can only be one of the allowed processes."""
    if process not in GBD.Process.Name.OPTIONS:
        raise ValueError(
            f"Process '{process}' is not an accepted process name. Options "
            f"are: {GBD.Process.Name.OPTIONS}"
        )


def validate_restart_or_resume(restart: bool, resume: bool) -> None:
    """Run can only be restarted OR resumed, not both."""
    if restart and resume:
        raise ValueError("Cannot both restart and resume run. Please select only one.")


def validate_restart_has_version_id(
    restart: bool, resume: bool, version_id: Optional[int], process: str
) -> None:
    """
    Can't restart/resume a run without knowing the run's version id.

    Raises:
        ValueError if restart or resume is True but version_id is None
    """
    if (restart or resume) and not version_id:
        raise ValueError(f"Cannot restart or resume a run without a {process} version id.")


def validate_test(test: bool, restart: bool, resume: bool) -> None:
    """
    Can't restart or resume a run as a test. That invalidates
    the state assumptions of the machinery parameters object.

    You can restart a test run (by restarting normally), but
    you cannot specify test alongside restart
    """
    if test and (restart or resume):
        raise ValueError("Cannot restart or resume a run as a test run.")


def validate_pct_change_years(
    year_ids: List[int],
    year_start_ids: Optional[List[int]],
    year_end_ids: Optional[List[int]],
) -> None:
    """
    Rules surrounding percent change:
        * both year_start_ids and year_end_ids must be passed in or neither
        * both must be the same length
        * No years are specified that aren't in year_ids
        * ith entry of year_start_ids must always be smaller (before)
          year_end_ids
    """
    if not year_start_ids and not year_end_ids:
        return

    details = (
        f"Passed: year_ids = {year_ids}, year_start_ids = "
        f"{year_start_ids}, year_end_ids = {year_end_ids}"
    )

    if bool(year_start_ids) ^ bool(year_end_ids):
        raise ValueError(
            "Both 'year_start_ids' and 'year_end_ids' must be None or "
            "both must have values.\n\n" + details
        )

    if not len(year_start_ids) == len(year_end_ids):
        raise ValueError(
            "'year_start_ids' and 'year_end_ids' have a "
            "1 to 1 correspondence and thus must be the "
            "same length.\n\n" + details
        )

    diff = set(year_start_ids + year_end_ids) - set(year_ids)
    if diff:
        raise ValueError(
            "Percent change years contain years not in year_ids, which "
            f"would be impossible to calculate. Missing years: {list(diff)}"
            "\n\n" + details
        )

    if not all(year_start_ids[i] < year_end_ids[i] for i in range(len(year_start_ids))):
        raise ValueError(
            "Each value in 'year_start_ids' must be strictly less than "
            "the corresponding value in 'year_end_ids'.\n\n" + details
        )


def validate_release_and_years(release_id: int, year_ids: List[int]) -> None:
    """
    Validate that there's no mismatch between the given years
    via year_ids and the estimated years for the release ID.
    """
    allowed_years = set(
        db_queries.get_demographics(gbd_team="cod", release_id=release_id)[Columns.YEAR_ID]
    )
    diff = set(year_ids) - allowed_years

    if diff:
        raise ValueError(
            "The following years do not fit the set of allowed years for release "
            f"id {release_id}: {list(diff)}"
        )


def validate_n_draws(n_draws: int) -> None:
    """
    Draws can take any integer value at or below the MAX_DRAWS level (1000),
    and greater than 0 (or else what's the point?)
    """
    if n_draws <= 0:
        raise ValueError(f"Number of draws must be greater than 0! Received {n_draws}")
    elif n_draws > Draws.MAX_DRAWS:
        raise ValueError(
            "Number of draws cannot be greater than the max number of draws "
            f"({Draws.MAX_DRAWS}). Received {n_draws}"
        )


def validate_location_set_ids(
    base_location_set_id: int, aggregation_location_set_ids: List[int]
) -> None:
    """
    Checks for location set ids.

        * Need a non-null value for base_location_set_id
        * base_location_set_id can't be in the aggregation loc sets
        * No duplicates in aggregation_location_set_ids
    """
    if base_location_set_id is None:
        raise ValueError("Parameter 'base_location_set_id' should never be None")

    if base_location_set_id in aggregation_location_set_ids:
        raise ValueError(
            "The base location set cannot be in the aggregation location "
            f"sets.\nBase set: {base_location_set_id}\n"
            f"Aggregation sets: {aggregation_location_set_ids}"
        )

    duplicates = set(
        [n for n in aggregation_location_set_ids if aggregation_location_set_ids.count(n) > 1]
    )
    if duplicates:
        raise ValueError(
            "Duplicate location set ids in 'aggregation_location_set_ids': " f"{duplicates}"
        )


def validate_aggregate_cause_sets(
    base_cause_set_id: int, aggregation_cause_set_ids: List[int]
) -> None:
    """
    Validates the aggregation cause sets against the base cause set.
    Namely:
        The base cause set cannot be in the aggregation cause set
    """
    if base_cause_set_id in aggregation_cause_set_ids:
        raise ValueError(
            f"The base cause set ({base_cause_set_id} cannot be in the "
            f"aggregation cause sets ({aggregation_cause_set_ids}."
        )


def validate_measure_ids(measure_ids: List[int]) -> None:
    """
    Validates measure ids:
        * Only deaths (1) and YLLs (4) can be specified as measures
        * Deaths MUST be specified (otherwise what are we doing here?)
    """
    prohibited_measure_ids = set(measure_ids) - {
        gbd_constants.measures.DEATH,
        gbd_constants.measures.YLL,
    }
    if prohibited_measure_ids:
        raise ValueError(
            f"Only deaths (1) and YLLs (4) can be specified as measure ids for the run. "
            f"The following measure id(s) are invalid: {prohibited_measure_ids}."
        )

    if gbd_constants.measures.DEATH not in measure_ids:
        raise ValueError("Deaths (measure id 1) required for CodCorrect run.")


def validate_databases(databases: List[str]) -> None:
    """
    Validates that the database(s) passed are allowed and that
    GBD is among them.
    """
    for database in databases:
        if database not in DataBases.DATABASES:
            raise ValueError(
                f"Input database '{database}' is not valid. Allowed values: "
                f"{DataBases.DATABASES}"
            )

    if DataBases.GBD not in databases:
        raise ValueError(
            f"Database '{DataBases.GBD}' missing from inputs. Databases passed: {databases}"
        )


def validate_odbc_connections() -> None:
    """
    Validates that all expected connection definitions are in the ODBC
    in the home directory and that the user is the codcorrect user (that has the
    proper permissions).
    """
    odbc = config.parse_odbc("FILEPATH")

    for conn_def in ConnectionDefinitions.EXPECTED:
        if conn_def not in odbc:
            raise ValueError(
                f"Missing connection definition for '{conn_def}' in ODBC at FILEPATH "
                "You must have this connection defined in "
                "your personal ODBC to run CodCorrect."
            )
        if odbc[conn_def]["user"] not in ConnectionDefinitions.USERS:
            raise ValueError(
                f"Connection definition for '{conn_def}' in ODBC at FILEPATH is "
                f"'{odbc[conn_def]['user']}', must be one of '{ConnectionDefinitions.USERS}' "
                "to run CodCorrect."
            )
