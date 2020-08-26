import pandas as pd

from db_tools.ezfuncs import query
from gbd.gbd_round import validate_gbd_round_id

from fauxcorrect.utils.constants import ConnectionDefinitions
from fauxcorrect.queries.queries import PredEx


def get_pred_ex(gbd_round_id: int) -> pd.DataFrame:
    """
    Call a stored procedure in the mortality database to fetch
    predicted life expectancy.

    Arguments:
        gbd_round_id (int): GBD round ID

    Returns:
        DataFrame containing age_group_id (int), location_id (int),
        pred_ex (float), year_id (int), sex_id (int)

    Raises:
        ValueError: when gbd_round_id is invalid
    """
    validate_gbd_round_id(gbd_round_id)
    return query(
        PredEx.GET_PRED_EX,
        conn_def=ConnectionDefinitions.MORTALITY,
        parameters={'gbd_round_id': gbd_round_id}
    )
