import logging
from typing import List

import pandas as pd

from get_draws.api import get_draws

logger = logging.getLogger(__name__)


def read_model_draws(
    cause_id: int, model_version_id: int, location_ids: List[int], release_id: int
) -> pd.DataFrame:
    """Reads draws using get_draws.

    Arguments:
        cause_id: cause ID for the model
        model_version_id: model_version_id to be read from
        location_ids: list of location ids to filter the draws by
        release_id: GBD release ID
    Returns:
        Dataframe of the draws for a given list of locations
    """
    logger.info(
        f"Reading draws with get_draws for cause ID {cause_id}, model_version_id "
        f"{model_version_id}."
    )
    df = get_draws(
        gbd_id_type="cause_id",
        gbd_id=int(cause_id),
        source="codem",
        version_id=model_version_id,
        location_id=location_ids,
        release_id=release_id,
    )
    df = df.rename(columns={"pop": "population"})
    return df
