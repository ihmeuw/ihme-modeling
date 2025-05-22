import os
import warnings
from typing import List, Optional, Tuple

import click
import pandas as pd

import db_queries
import gbd_outputs_versions
from gbd.constants import gbd_metadata_type, gbd_process, gbd_teams

from ihme_cc_sev_calculator.lib import constants


def parse_comma_sep_to_int_list(
    ctx: click.Context, param: click.Parameter, value: Optional[str] = None
) -> Optional[List[int]]:
    """Parse a comma-separated list to ints. Allow None to pass through."""
    if value is not None:
        value = [int(i) for i in value.split(",")]
    return value


def parse_comma_sep_to_str_list(
    ctx: click.Context, param: click.Parameter, value: Optional[str] = None
) -> Optional[List[str]]:
    """Parse a comma-separated list to strs. Allow None to pass through."""
    if value is not None:
        value = [i.strip(" ") for i in value.split(",")]
    return value


def validate_resume(resume: bool, version_id: Optional[int]) -> None:
    """Validate that if resuming, a version_id is given.

    Warns if version_id is given but not resuming.
    """
    if resume and not version_id:
        raise ValueError("Cannot resume without a version_id")

    if not resume and version_id:
        warnings.warn("Not resuming a run. Ignoring version_id argument")


def validate_measures(measures: Optional[List[str]]) -> Tuple[List[str], List[int]]:
    """Validates the measures to produce.

    'rr_max' must always be provided. If it's not included, we add it anyway.
    'sev' is optional. If no measures provided, defaults to both.

    Returns:
        Validated measures and corresponding measure ids
    """
    if measures is None:
        measures = [constants.RR_MAX, constants.SEV]

    invalid_measures = set(measures) - set(constants.MEASURE_MAP.keys())
    if invalid_measures:
        raise ValueError(
            f"The following invalid measures were provided: {list(invalid_measures)}"
        )

    if constants.RR_MAX not in measures:
        warnings.warn(
            f"Measure '{constants.RR_MAX}' must always been included. Adding it anyway."
        )
        measures.append(constants.RR_MAX)

    measure_ids = [constants.MEASURE_MAP[constants.RR_MAX]]

    if constants.SEV in measures:
        measure_ids.append(constants.MEASURE_MAP[constants.SEV])

    return (measures, measure_ids)


def validate_year_ids(year_ids: Optional[List[int]], release_id: int) -> List[int]:
    """Validates year_ids fit within valid years.

    If year_ids is None, returns estimation year ids.

    Returns:
        Validated year ids
    """
    estimation_year_ids = db_queries.get_demographics(
        release_id=release_id, gbd_team=gbd_teams.EPI
    )["year_id"]
    all_year_ids = [i for i in range(min(estimation_year_ids), max(estimation_year_ids) + 1)]

    if year_ids is None:
        return estimation_year_ids

    invalid_year_ids = set(year_ids) - set(all_year_ids)
    if invalid_year_ids:
        raise ValueError(
            "The following years do not exist in get_demographics for release "
            f"{release_id}: {list(invalid_year_ids)}"
        )

    return year_ids


def validate_compare_version(
    compare_version_id: Optional[int],
    paf_version_id: Optional[int],
    como_version_id: Optional[int],
    codcorrect_version_id: Optional[int],
    measures: List[str],
) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """Validates all input machinery: PAFs, COMO, CodCorrect

    If not running for SEVs, we don't need machinery. No validation occurs.
    Any input versions will be overwritten by None.

    If given a compare version, pulls all relevant machinery versions if possible.
    If any are missing, they must be passed in or an error is raised. If any machinery
    versions exist in the compare version and are also passed in, the latter overwrite
    the former.

    For example, take compare version X that has:
        * COMO 300
        * CodCorrect 100

    At a minimum, since that compare version has no PAF version, paf_version_id must be
    passed in. If, say, como_version_id 999 is also passed in, it will overwrite
    COMO version 300 found in the compare version.

    Returns:
        Validated PAF version, COMO version, and CodCorrect version
    """
    if constants.SEV not in measures:
        return (None, None, None)

    if compare_version_id:
        cv = gbd_outputs_versions.CompareVersion(compare_version_id)
        temp_paf_version_id = retrieve_internal_version_id(cv, gbd_process.PAF)
        temp_como_version_id = retrieve_internal_version_id(cv, gbd_process.EPI)
        temp_codcorrect_version_id = retrieve_internal_version_id(cv, gbd_process.COD)
    else:
        temp_paf_version_id = None
        temp_como_version_id = None
        temp_codcorrect_version_id = None

    paf_version_id = paf_version_id or temp_paf_version_id
    como_version_id = como_version_id or temp_como_version_id
    codcorrect_version_id = codcorrect_version_id or temp_codcorrect_version_id

    missing_inputs = [
        i for i in [paf_version_id, como_version_id, codcorrect_version_id] if i is None
    ]
    if missing_inputs:
        raise ValueError(
            f"Could not determine valid versions for all required machinery: "
            f"compare_version_id {compare_version_id}, paf_version_id {paf_version_id}, "
            f"como_version_id {como_version_id}, codcorrect_version_id "
            f"{codcorrect_version_id}"
        )

    # Assert all versions are not none for type check purposes
    assert paf_version_id is not None  # nosec
    assert como_version_id is not None  # nosec
    assert codcorrect_version_id is not None  # nosec

    return paf_version_id, como_version_id, codcorrect_version_id


def retrieve_internal_version_id(
    compare_version: gbd_outputs_versions.CompareVersion, gbd_process_id: int
) -> Optional[int]:
    """Returns the internal version of the given process in the compare version, if exists."""
    if gbd_process_id in compare_version.gbd_process_version_map:
        return compare_version.gbd_process_version_map[gbd_process_id].internal_version_id
    else:
        return None


def validate_machinery_versions(
    paf_version_id: Optional[int],
    como_version_id: Optional[int],
    codcorrect_version_id: Optional[int],
    measures: List[str],
    year_ids: List[int],
    n_draws: int,
) -> None:
    """Validate years and draws in input machinery versions.

    For the PAF Aggregator:
        * Has all years requested for SEV Calculator run

    For COMO and CodCorrect:
        * Has the arbitrary year used for PAF aggregation to parent causes
            for small number of causes

    All machinery must have draws >= the number of requested draws.
    """
    if constants.SEV not in measures:
        return

    process_id_map = {
        paf_version_id: gbd_process.PAF,
        como_version_id: gbd_process.EPI,
        codcorrect_version_id: gbd_process.COD,
    }
    for version_id, gbd_process_id in process_id_map.items():
        pv_id = gbd_outputs_versions.internal_to_process_version(version_id, gbd_process_id)
        pv = gbd_outputs_versions.GBDProcessVersion(pv_id)

        if gbd_metadata_type.YEAR_IDS not in pv.metadata:
            raise ValueError(
                f"The following process version does not have year metadata: {pv}"
            )

        # Extract estimated years, converting to list if necessary
        pv_year_ids = pv.metadata[gbd_metadata_type.YEAR_IDS]
        pv_year_ids = pv_year_ids if isinstance(pv_year_ids, list) else [pv_year_ids]

        if gbd_process_id == gbd_process.PAF:
            # PAF Aggregator must have all years requested
            missing_year_ids = set(year_ids) - set(pv_year_ids)
            if missing_year_ids:
                raise ValueError(
                    f"PAF Aggregator v{version_id} is missing requested year(s) "
                    f"{missing_year_ids}.\nPAF Aggregator year(s): {pv_year_ids}\n"
                    f"Requested year(s): {year_ids}"
                )
        else:
            # COMO, CodCorrect must have arbitrary year
            if constants.ARBITRARY_MACHINERY_YEAR_ID not in pv_year_ids:
                raise ValueError(
                    "The following machinery version is missing the arbitrary year used "
                    f"to pull LBW/SG, CKD, etc. child cause burden: {pv}\n"
                    f"Machinery year(s): {pv_year_ids}\n"
                    f"Arbritrary year: {constants.ARBITRARY_MACHINERY_YEAR_ID}"
                )

        # Validate machineries have >= number of requested draws
        pv_n_draws = pv.metadata[gbd_metadata_type.N_DRAWS]
        if n_draws > pv_n_draws:
            raise ValueError(
                "The following machinery version does not have at least the requested number "
                f"of draws: {pv}\nMachinery draws: {pv_n_draws}\nRequested draws: {n_draws}"
            )


def validate_n_draws(n_draws: Optional[int]) -> None:
    """Validates draws are between 1 and 1000."""
    if n_draws is None:
        raise ValueError("Parameter 'n_draws' is required")

    if n_draws < 1 or n_draws > 1000:
        raise ValueError(f"n_draws must be between 1 - 1000, not {n_draws}")


def validate_location_set_ids(
    location_set_ids: Optional[List[int]], release_id: int
) -> Tuple[List[int], List[int]]:
    """Validates location sets, returning their corresponding location set versions.

    If no location set ids given, sets to the default location set. The default
    location set must be present in given location sets.

    Returns:
        Validated location set ids, corresponding location set version ids
    """
    if not location_set_ids:
        location_set_ids = [constants.DEFAULT_LOCATION_SET_ID]

    if constants.DEFAULT_LOCATION_SET_ID not in location_set_ids:
        raise ValueError(
            f"Default location set ({constants.DEFAULT_LOCATION_SET_ID}) not present in "
            f"given location set(s): {location_set_ids}"
        )

    location_set_version_ids = []
    for location_set_id in location_set_ids:
        metadata = db_queries.get_location_metadata(
            location_set_id=location_set_id, release_id=release_id
        )
        location_set_version_ids.append(metadata["location_set_version_id"].iat[0])

    if len(location_set_ids) != len(location_set_version_ids):
        raise RuntimeError(
            "After validating location set ids, do no have the same number of location "
            f"set versions ({location_set_version_ids}) as location sets ({location_set_ids})"
        )

    return (location_set_ids, location_set_version_ids)


def validate_percent_change(percent_change: bool, year_ids: List[int]) -> bool:
    """Validates percent change.

    We need multiple years to compute percent change, so if percent change is requested but
    only a single is asked for, we warn of this inconsistency and flip percent change to
    False.

    Returns:
        Validated percent change
    """
    if percent_change and len(year_ids) == 1:
        warnings.warn(
            "Percent change requested but only one year given. Setting percent change to "
            "False"
        )
        percent_change = False

    return percent_change


def validate_custom_rrmax_files(
    all_rei_ids: List[int], rei_metadata: pd.DataFrame, release_id: int
) -> None:
    """Validate there are custom RRmax files for all REIs that we expect them for.

    Doesn't check for contents. Only validates that the expected file(s) exist.
    """
    included_custom_rei_ids = [
        rei_id for rei_id in all_rei_ids if rei_id in constants.CUSTOM_RR_MAX_REI_IDS
    ]
    missing_file_msg = []
    for rei_id in included_custom_rei_ids:
        filtered_metadata = rei_metadata.query(f"rei_id == {rei_id}")

        # Invariant check: given rei_id should be in REI metadata
        if len(filtered_metadata) != 1:
            raise RuntimeError(
                f"REI metadata does not contain rei_id={rei_id}, a risk we expect custom "
                "RRmax for. Cannot validate whether the custom RRmax file exists or not."
            )

        # Check if the expected RRmax file exists
        rei = filtered_metadata["rei"].iat[0]
        expected_file_path = constants.CUSTOM_RR_MAX_DRAW_FILE.format(
            release_id=release_id, rei=rei
        )
        if not os.path.isfile(expected_file_path):
            missing_file_msg.append(
                f"{filtered_metadata['rei_name'].iat[0]} (REI {rei_id}): {expected_file_path}"
            )

    # Report all missing files at end
    if missing_file_msg:
        raise RuntimeError(
            "Cannot find the expected custom RRmax files for the following risks included "
            "in the SEV Calculator run:\n" + "\n".join(missing_file_msg)
        )
