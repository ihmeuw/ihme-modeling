import sqlalchemy as sql
import pandas as pd
from codcorrect.error_check import check_data_format
import logging


def read_hdf_draws(draws_filepath, location_id, key="draws"):
    """
    Read in CODEm/custom model draws from a given filepath and filter by
    location_id.

    :param draws_filepath: str
        the path to the hdf to read from
    :param location_id: int
        the location id for the draws to read in
    :param key: str
        specifies what to read in, dUSERts to draws
    :return: dataframe
        pandas dataframe containing the draws for the location specified
    """
    data = pd.read_hdf(draws_filepath, key=key,\
        where=["location_id=={location_id}".format(location_id=location_id)])
    return data
