from typing import List, Set

import pandas as pd

import db_queries
from gbd import constants as gbd_constants
from db_queries.api.internal import get_active_sequela_set_version

from como.lib import database_io
from como.lib import resource_file_io as rfio


def get_best_model_versions(release_id) -> pd.DataFrame:
    """Returns DF of best model versions."""
    como_meids = _get_como_meids(release_id=release_id)
    mvid_list = db_queries.get_best_model_versions(
        entity="modelable_entity", ids=como_meids, release_id=release_id
    )
    mvid_list = mvid_list[
        [
            gbd_constants.columns.MODELABLE_ENTITY_ID,
            gbd_constants.columns.MODEL_VERSION_ID,
            gbd_constants.columns.RELEASE_ID,
        ]
    ]
    return mvid_list


def _get_como_meids(release_id: int) -> List[int]:
    """Returns a list of all ME IDs used as COMO inputs."""
    # Read resource files and pull ME IDs
    resource_file_mes: Set[int] = set()
    me_dfs = [
        rfio.get_birth_prevalence(),
        rfio.get_sexual_violence_sequela(),
        rfio.get_injury_sequela(),
    ]
    for df in me_dfs:
        resource_file_mes = resource_file_mes.union(
            set(df[gbd_constants.columns.MODELABLE_ENTITY_ID])
        )

    # Read the seq hierarchy history and pull ME IDs
    seq_set_version_id = get_active_sequela_set_version(
        sequela_set_id=2,
        release_id=release_id
    )
    seq_list = database_io.get_sequela_list(sequela_set_version_id=seq_set_version_id)
    seq_mes = set(seq_list[gbd_constants.columns.MODELABLE_ENTITY_ID])

    # Combine and return the two sets of MEs
    return list(resource_file_mes.union(seq_mes))
