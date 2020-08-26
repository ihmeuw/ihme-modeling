import pandas as pd

from db_tools.ezfuncs import query
from gbd.gbd_round import gbd_round_from_gbd_round_id

from fauxcorrect.queries.queries import SpacetimeRestrictions
from fauxcorrect.utils.constants import Columns, ConnectionDefinitions


def get_all_spacetime_restrictions(gbd_round_id: int) -> pd.DataFrame:
    """
    Fetch all spacetime restrictions from the codcorrect database.

    Arguments:
        gbd_round_id: GBD round ID for this codcorrect run

    Returns:
        DataFrame containing cause_id (int), location_id (int), and
            year_id (int)
    """
    gbd_round = int(gbd_round_from_gbd_round_id(gbd_round_id))
    return query(
        SpacetimeRestrictions.GET_ALL,
        conn_def=ConnectionDefinitions.CODCORRECT,
        parameters={Columns.GBD_ROUND: gbd_round}
    )
