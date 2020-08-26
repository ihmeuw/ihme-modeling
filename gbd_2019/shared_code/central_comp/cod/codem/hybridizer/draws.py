import logging
from get_draws.api import get_draws
from gbd.decomp_step import decomp_step_from_decomp_step_id

logger = logging.getLogger(__name__)


def read_model_draws(cause_id, model_version_id, location_ids, decomp_step_id, gbd_round_id):
    """
    Reads draws using get_draws.
    :param cause_id:
        cause ID for the model
    :param model_version_id:
        model_version_id to be read from
    :param location_ids: list of ints
        list of location ids to filter the draws by
    :param decomp_step_id: int
        decomposition step ID
    :param gbd_round_id: int
        GBD round ID
    :return: dataframe
        pandas dataframe of the draws for a given list of locations
    """
    logger.info("Reading draws with get_draws for cause ID {},"
                "model_version_id {}.".format(cause_id, model_version_id))
    df = get_draws(gbd_id_type='cause_id',
                   gbd_id=int(cause_id),
                   source='codem',
                   version_id=model_version_id,
                   location_id=location_ids,
                   decomp_step=decomp_step_from_decomp_step_id(decomp_step_id),
                   gbd_round_id=int(gbd_round_id))
    return df

