import logging
import os
import re
from typing import List

from db_queries import get_location_metadata


def clean_aggregation_directory(
    root_dir: str,
    file_pattern: str,
    location_set_id: int,
    gbd_round_id: int
) -> None:
    """
    Occasionally a location aggregation job can fail and leave behind
    a corrupt HDF file. Subsequent retries will fail because of the
    corrupt file left behind. In order to avoid this, we compute a list
    of h5 files that we will produce during location aggregation. If any
    exist already, we delete them.

    Args:
        root_dir (str): directory that contains draws
        file_pattern (str): file pattern for location aggregation
        location_set_id (int): location set id used during aggregation
        gbd_round_id (int): gbd_round_id

    Returns:
        None

    Raises:
        NotImplementedError: Does not support file pattern with nested folders
        ValueError: Requires location_id to be in file pattern
        ValueError: Requires h5 in file pattern

    """
    if "/" in file_pattern:
        raise NotImplementedError(
            "Cannot determine files to clean when given filepath with "
            "directories")
    if "{location_id}" not in file_pattern:
        raise ValueError("file_pattern required to have {location_id}")
    if ".h5" not in file_pattern:
        raise ValueError("file_pattern expected to be for h5 files")

    loc_df = get_location_metadata(
        location_set_id=location_set_id,
        gbd_round_id=gbd_round_id)
    aggregate_locs = loc_df.loc[loc_df.most_detailed != 1].location_id.tolist()

    files_to_remove = _get_files_to_remove(
        root_dir, file_pattern, aggregate_locs)

    logger = logging.getLogger(__name__)
    logger.info(f"Deleting {len(files_to_remove)} files")
    for file in files_to_remove:
        os.remove(os.path.join(root_dir, file))


def _get_files_to_remove(
    root_dir: str,
    file_pattern: str,
    location_list: List[int]
) -> List[str]:
    """
    Given a directory, file pattern, and list of location ids, return a list
    of files that need to be deleted
    """
    regex = _convert_file_pattern_to_regex(file_pattern, location_list)
    all_files = os.listdir(root_dir)
    files_to_remove = [f for f in all_files if re.match(regex, f)]
    return files_to_remove


def _convert_file_pattern_to_regex(
    file_pattern: str,
    location_list: List[int]
) -> str:
    r"""
    replace {location_id} with specific locations to delete, and replace any
    other key like {foo} with .* to match any value

    So given a file pattern of {sex_id}_{location_id}_1990.h5 and aggregate
    locations [1,6], the regex becomes .*_(1|6)_1990\.h5
    """
    location_substr = "|".join([str(l) for l in location_list])
    regex = file_pattern.replace('{location_id}', f"({location_substr})")
    regex = re.sub(r"\{.*\}", ".*", regex)
    regex = regex.replace(".h5", r"\.h5$")
    return regex
