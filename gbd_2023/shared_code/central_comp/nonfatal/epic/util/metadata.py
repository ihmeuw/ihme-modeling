import json
import os

from epic.legacy.util.constants import FilePaths, Params


def save_model_metadata(
    parent_dir: str, modelable_entity_id: int, model_version_id: int, release_id: int
) -> None:
    """Function for saving specific model metadata on disk, as a json file."""
    metadata_dict = {
        Params.MODELABLE_ENTITY_ID: modelable_entity_id,
        Params.MODEL_VERSION_ID: model_version_id,
        Params.RELEASE_ID: release_id,
    }
    # write to disk
    with open(
        os.path.join(parent_dir, FilePaths.INPUT_FILES_DIR, f"{modelable_entity_id}.json"),
        "w",
    ) as outfile:
        json.dump(metadata_dict, outfile, sort_keys=True, indent=2)
