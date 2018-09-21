import sqlalchemy as sql
import pandas as pd
from hybridizer.core import run_query
import logging

def get_cause_hierarchy_version(cause_set_id=2, gbd_round_id=4,
                                server='SERVER'):
    """
    Get the IDs associated with best version of a cause set

    :param cause_set_id: int
        the set of causes to use
    :param gbd_round: int
        the year of the gbd_round to use
    :param server: str
        db server to use
    :return cause_set_version_id: int
        the version id for the cause_set and year that were input
    :return cause_metadata_version_id: in
        the version id for the metadata of the cause_set and year that were
        input
    """
    print server
    sql_statement = """
        SELECT
	      a.cause_set_version_id, v.cause_metadata_version_id
        FROM shared.cause_set_version_active a
        JOIN shared.cause_set_version v
          ON a.cause_set_version_id = v.cause_set_version_id
        WHERE a.cause_set_id = {cause_set_id}
          AND a.gbd_round_id = {gbd_round_id}
        """.format(cause_set_id=cause_set_id, gbd_round_id=gbd_round_id)
    result_df = run_query(sql_statement, server)

    # make sure exactly one cause set version is returned
    if len(result_df) > 1:
        exception_text = """
            This returned more than 1 cause_set_version_id ({returned_ids})
        """.format(returned_ids=", ".join(str(result_df['cause_set_version_id'].\
                drop_duplicates().tolist())))
        raise LookupError(exception_text)
    elif len(result_df) < 1:
        raise LookupError("No cause set versions returned")

    return result_df.ix[0, 'cause_set_version_id'], \
            result_df.ix[0, 'cause_metadata_version_id']

# DUSERts 35 and 2015 for the current version
def get_location_hierarchy_version(location_set_id=35, gbd_round_id=4,
                                                    server='SERVER'):
    """
    Get the IDs associated with best version of a location set

    :param location_set_id: int
        the set of locations to use
    :param gbd_round: int
        the year of the gbd round to use
    :param server: str
        db server to use - either
    :return location_set_version_id: int
        the version id for the location set and year that were input
    :return location_metadata_version_id: int
        the version id for the metadata of the location and year that were
        input
    """
    sql_statement = """
        SELECT
    	    a.location_set_version_id, v.location_metadata_version_id
        FROM shared.location_set_version_active a
        JOIN shared.location_set_version v
            ON a.location_set_version_id = v.location_set_version_id
        WHERE a.location_set_id = {location_set_id}
            AND a.gbd_round_id = {gbd_round_id}
    """.format(location_set_id=location_set_id, gbd_round_id=gbd_round_id)
    result_df = run_query(sql_statement, server)

    if len(result_df) > 1:
        exception_text = """
            This returned more than 1 location_set_version_id ({returned_ids})
        """.format(returned_ids=", ".join(result_df['location_set_version_id'].\
                drop_duplicates().to_list()))
        raise LookupError(exception_text)
    elif len(result_df) < 1:
        raise LookupError("No location set versions returned")
    return result_df.ix[0, 'location_set_version_id'], \
            result_df.ix[0, 'location_metadata_version_id']
