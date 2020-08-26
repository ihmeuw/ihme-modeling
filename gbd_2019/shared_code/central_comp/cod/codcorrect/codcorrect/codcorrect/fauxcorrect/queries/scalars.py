import pandas as pd

from db_tools.ezfuncs import query

from fauxcorrect.queries.queries import Scalars
from fauxcorrect.utils.constants import ConnectionDefinitions


def get_all_scalars() -> pd.DataFrame:
    """
    Fetch all scalars from COD database.
    Constraints on the scalar table ensure that all values are numeric
    and non-null, so there is no need for further validation.

    Returns:
        DataFrame containing output_version_id (int), location_id (int),
        year_id (int), sex_id (int), age_group_id (int), cause_id (int),
        and scalar (float)
    """
    return query(Scalars.GET_ALL, conn_def=ConnectionDefinitions.COD)
