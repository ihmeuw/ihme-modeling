import pandas as pd

from db_tools.ezfuncs import query

from fauxcorrect.queries.queries import RegionalScalars
from fauxcorrect.utils.constants import Columns, ConnectionDefinitions


def get_all_regional_scalars(gbd_round_id: int) -> pd.DataFrame:
    """
    Fetch all regional scalars from the mortality database.

    Arguments:
        gbd_round_id: GBD round ID for this fauxcorrect run

    Returns:
        DataFrame containing year_id (int), location_id (int), and mean (float)
    """
    return query(
        RegionalScalars.GET_ALL,
        conn_def=ConnectionDefinitions.MORTALITY,
        parameters={Columns.GBD_ROUND_ID: gbd_round_id}
    )
