from os.path import join
import pickle
from typing import Dict, List, Optional, Union

from db_queries import get_cause_metadata, get_location_metadata
from gbd.gbd_round import validate_gbd_round_id

from fauxcorrect.utils.constants import Columns, FilePaths, SpecialMappings

BackfillMapping = Dict[int, int]
BackfilledIds = Union[List[int], BackfillMapping]


def create_backfill_mapping(
        parent_dir: str,
        previous_gbd_round_id: int,
        current_gbd_round_id: int,
        location_set_id: Optional[int] = None,
        cause_set_id: Optional[int] = None
) -> None:
    """
    Store location/cause backfill as JSON file with that maps new
    location/cause ID to location/cause ID with which to backfill. Subnationals
    are backfilled according to their parent location, and special case
    locations are handled individually.

    A value of -1 is assigned to denote that the values should not be scaled.

    Arguments:

        parent_dir (str): parent fauxcorrect directory
            e.g. PATH/{version}

        previous_gbd_round_id (int): GBD round from which locations should be
            backfilled

        current_gbd_round_id (int): GBD round of this fauxcorrect run

        location_set_id (int): location set ID of locations to be backfilled

        cause_set_id (int): cause set ID of causes to be backfilled

    Raises:
        ValueError: if either GBD round ID argument is invalid, or if neither
        or both of location_set_id and cause_set_id are passed
    """
    # Validate input.
    validate_gbd_round_id(previous_gbd_round_id)
    validate_gbd_round_id(current_gbd_round_id)
    _validate_set_ids(location_set_id, cause_set_id)

    # Assign variables based on whether this is for a cause or location
    # backfill.
    is_location_backfill = location_set_id is not None
    set_id = location_set_id if is_location_backfill else cause_set_id
    id_col = Columns.LOCATION_ID if is_location_backfill else Columns.CAUSE_ID
    metadata_func = (
        get_location_metadata if is_location_backfill else get_cause_metadata
    )

    # Create mapping.
    previous_ids = set(
        metadata_func(set_id, gbd_round_id=previous_gbd_round_id)[id_col]
    )
    mapping_df = (
        metadata_func(set_id, gbd_round_id=current_gbd_round_id)
        .where(lambda df: ~df[id_col].isin(previous_ids))
        .sort_values(by=id_col)
        .dropna(how='all')
        .assign(backfilled_id=lambda df: df[Columns.PARENT_ID])
        [[id_col, Columns.BACKFILLED_ID]]
        .astype(int)
    )
    mapping = dict(zip(
        mapping_df[id_col], mapping_df[Columns.BACKFILLED_ID]
    ))

    # Special cases
    if is_location_backfill:
        # Set special location mappings as specified in SpecialMappings constant
        for location, mapped_location in SpecialMappings.LOCATIONS.items():
            mapping[location] = mapped_location
    else:
        # Set special cause mappings as specified in SpecialMappings constant
        for cause, mapped_cause in SpecialMappings.CAUSES.items():
            mapping[cause] = mapped_cause

    # Write mapping to JSON file.
    mapping_filename = _get_mapping_filename(parent_dir, is_location_backfill)
    with open(mapping_filename, 'wb') as mapping_file:
        pickle.dump(mapping, mapping_file)


def lookup_location_or_cause(
        parent_dir: str,
        loc_or_cause_id: int,
        is_loc_backfill: bool
) -> int:
    """
    Given a location/cause ID, returns the location/cause ID with which the
    location/cause should be used.

    If the location/cause ID is not present in the backfill file, there is no
    mapping and the given location/cause ID should be returned.

    Arguments:

        parent_dir (str): parent fauxcorrect directory
            e.g. PATH/{version}

        loc_or_cause_id: location/cause ID to look up

        is_loc_backfill: whether this is a location or cause backfill

    Returns:
        location/cause ID with which the current id should be backfilled.
    """
    backfill_mapping = _read_mapping(parent_dir, is_loc_backfill)
    if loc_or_cause_id in backfill_mapping:
        return backfill_mapping[loc_or_cause_id]
    return loc_or_cause_id


def backfill_location_or_cause(
        parent_dir: str,
        new_loc_or_cause_id: int,
        is_loc_backfill: bool
) -> int:
    """
    Given a new location/cause ID, returns the location/cause ID with which the
    new location/cause should be backfilled.

    Arguments:

        parent_dir (str): parent fauxcorrect directory
            e.g. PATH/{version}

        new_loc_or_cause_id: location/cause ID that did not exist before this
            GBD round

        is_loc_backfill: whether this is a location or cause backfill

    Returns:
        location/cause ID with which the new location/cause should be
        backfilled.

    Raises:
        ValueError: if new_loc_or_cause_id is missing from the mapping
    """
    backfill_mapping = _read_mapping(parent_dir, is_loc_backfill)
    _validate_location_or_cause_id(
        backfill_mapping, new_loc_or_cause_id, is_loc_backfill
    )
    return backfill_mapping[new_loc_or_cause_id]


def backfill_locations_or_causes(
        parent_dir: str,
        new_loc_or_cause_ids: List[int],
        is_loc_backfill: bool,
        as_dict: bool = False
) -> BackfilledIds:
    """
    Given a list of new location/cause IDs, returns a list or dict of
    location/cause IDs with which the new locations/causes should be backfilled.

    Arguments:

        parent_dir (str): parent fauxcorrect directory
            e.g. PATH/{version}

        new_loc_or_cause_ids: location/cause IDs that did not exist before this
            GBD round

        is_loc_backfill: whether this is a location or cause backfill

        as_dict: whether the backfilled location/cause IDs should be returned
            as a dictionary instead of a list

    Returns:
        location/cause IDs with which the new locations/causes should be
        backfilled.

    Raises:
        ValueError: if new_loc_or_cause_ids contains missing location IDs
    """
    backfill_mapping = _read_mapping(parent_dir, is_loc_backfill)
    for lc_id in new_loc_or_cause_ids:
        _validate_location_or_cause_id(
            backfill_mapping, lc_id, is_loc_backfill
        )
    return (
        {lc_id: backfill_mapping[lc_id] for lc_id in new_loc_or_cause_ids}
        if as_dict
        else [backfill_mapping[lc_id] for lc_id in new_loc_or_cause_ids]
    )


def _validate_set_ids(
        location_set_id: Optional[int],
        cause_set_id: Optional[int]
) -> None:
    neither_location_nor_cause = not location_set_id and not cause_set_id
    both_location_and_cause = location_set_id and cause_set_id
    if neither_location_nor_cause or both_location_and_cause:
        raise ValueError(
            'Must specify exactly one of location_set_id and cause_set_id. '
            f'Got location_set_id {location_set_id} and cause_set_id '
            f'{cause_set_id}.'
        )


def _get_mapping_filename(parent_dir: str, is_loc_backfill: bool) -> str:
    backfill_file = (
        FilePaths.LOCATION_BACKFILL_MAPPING
        if is_loc_backfill
        else FilePaths.CAUSE_BACKFILL_MAPPING
    )
    return join(parent_dir, FilePaths.INPUT_FILES_DIR, backfill_file)


def _read_mapping(parent_dir: str, is_loc_backfill: bool) -> BackfillMapping:
    mapping_filename = _get_mapping_filename(parent_dir, is_loc_backfill)
    with open(mapping_filename, 'rb') as mapping_file:
        return pickle.load(mapping_file)


def _validate_location_or_cause_id(
        backfill_mapping: BackfillMapping,
        loc_or_cause_id: int,
        is_loc_backfill: bool
) -> None:
    id_type = Columns.LOCATION_ID if is_loc_backfill else Columns.CAUSE_ID
    if loc_or_cause_id not in backfill_mapping:
        raise ValueError(
            f'{id_type} {loc_or_cause_id} is not present in backfill mapping.'
        )
