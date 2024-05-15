"""This script checks that the proper entity files are output by a given job."""
import sys
from typing import List, Optional

from fhs_lib_database_interface.lib.query import cause
from fhs_lib_file_interface.lib.check_input import write_log_message
from fhs_lib_file_interface.lib.version_metadata import VersionMetadata
from tiny_structured_logger.lib import fhs_logging

logger = fhs_logging.get_logger()


def find_missing_entities(entity_dir: VersionMetadata, entities: List[str]) -> List[str]:
    """Do the work of finding entities missing in the given directory.
    """
    return [
        entity for entity in entities if not (entity_dir.data_path() / f"{entity}.nc").exists()
    ]


def check_entities_main(
    entities: tuple[str],
    gbd_round_id: int,
    past_or_future: str,
    stage: str,
    version: str,
    suffix: Optional[str],
    entities_source: Optional[str],
) -> None:
    """Check whether there are any missing entity files.

    Writes a warning file to a warnings subdirectory in the given version if entities are
    missing.

    Args:
        entities (list[str]): List of entities to check
        gbd_round_id (int): What gbd_round_id the results are saved under
        past_or_future (str): Whether we are checking past or future
        stage (str): The stage that we are checking files for (e.g. prevalence)
        version (str): The version that we are checking files for
        suffix (str | None, optional): Optionally append a suffix to the warning file name,
            e.g. "_from_pi"
        entities_source (Optional[str]): When "entities" is undefined/empty, use this "source"
            parameter to load the entities from the shared database.
    """
    suffix = suffix or ""

    if len(entities) == 0:
        entities = cause.get_stage_cause_set(stage, gbd_round_id, source=entities_source)
        logger.info(f"Called database with {stage}, {gbd_round_id}. got {len(entities)}")

    if len(entities) == 0:
        message = (
            "run-check-entities is running against 0 entities, doing nothing. "
            "That's probably a mistake."
        )
        logger.error(message)
        raise ValueError(message)

    entity_dir = VersionMetadata.parse_version(
        f"{gbd_round_id}/{past_or_future}/{stage}/{version}"
    )
    missing_entities = find_missing_entities(entity_dir, entities)

    if missing_entities:
        warn_msg = (
            f"There are missing entities! Missing: "
            f"{', '.join(map(str, missing_entities))}\n"
        )
        warn_dir = entity_dir.data_path() / "warnings"
        warn_dir.mkdir(parents=True, exist_ok=True)
        warn_file = warn_dir / f"missing_entities{suffix}.txt"

        logger.warning(warn_msg)
        write_log_message(warn_msg, warn_file)
        sys.exit(100)
    else:
        logger.info("No missing entities!")
