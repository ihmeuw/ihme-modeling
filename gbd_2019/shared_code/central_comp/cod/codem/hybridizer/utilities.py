import logging

import hybridizer.reference.db_connect as db_connect

from hybridizer.database import check_model_attribute
import hybridizer.database as database
from hybridizer import emails

logger = logging.getLogger(__name__)


def get_cause_dict(conn_def):
    """
    Returns information stored in the shared.cause table with a key of cause_id.
    This does not include hierarchy-specific information like age start or age
    end.

    :return: dict
        cause information
    """
    logger.info("Getting cause dictionary")
    df = db_connect.query('SELECT * FROM shared.cause;',
                          conn_def=conn_def).set_index('cause_id')
    df = df.to_dict('index')
    return df


def get_locations(location_set_version_id, conn_def):
    """
    Get a DataFrame of locations and their hierarchy data from the
    specified location hierarchy version

    :param location_set_version_id: int
        the version id for the set of locations to retrieve
    :param conn_def: str
        name of string
    :return: dataframe
        pandas dataframe containing the location hierarchy information
    """
    logger.info("Get locations for location_set_version_id {}".format(location_set_version_id))
    call = "Call shared.view_location_hierarchy_history({loc_set_vid})".\
        format(loc_set_vid=location_set_version_id)
    return db_connect.query(call, conn_def=conn_def)


def get_model_properties(model_version_id, conn_def):
    """
    Gets a dictionary of the model information for a specified model_version_id

    :param model_version_id: int
        specification for which model version to retrieve
    :param conn_def: str
        which full server name to use
    :return: dict
        dictionary where each entry is a column of the model_version table
    """
    logger.info("Getting model properties for {}".format(model_version_id))
    call = "SELECT * FROM cod.model_version WHERE model_version_id = {};".\
        format(model_version_id)
    model_data = db_connect.query(call, conn_def=conn_def)
    output = {}
    for c in model_data.columns:
        output[c] = model_data.loc[0, c]
    return output


def get_excluded_locations(developed_model, conn_def):
    """
    Get a list of the excluded locations from the developed model

    :param developed_model: int
        model_version_id for the developed model of interest
    :param conn_def:
        which full server name to use
    :return: list of ints
        list of the location ids that are excluded in the given developed model
    """
    logger.info("Getting excluded locations for {}.".format(developed_model))
    model_data = get_model_properties(developed_model, conn_def)
    return model_data['locations_exclude'].split(' ')


def validate_params(global_model_version_id,
                    developed_model_version_id,
                    feeder_model_data,
                    conn_def,
                    user):
    """
    Validates that the main parameters in the global
    and the developed model match up.
    """

    # Isolate the variables that have been read in as a dataframe
    sex_id = feeder_model_data['sex_id'].iloc[0]
    cause_id = feeder_model_data['cause_id'].iloc[0]
    age_start = feeder_model_data['age_start'].iloc[0]
    age_end = feeder_model_data['age_end'].iloc[0]
    gbd_round_id = feeder_model_data['gbd_round_id'].iloc[0]
    refresh_id = feeder_model_data['refresh_id'].max()
    decomp_step_id = feeder_model_data['decomp_step_id'].iloc[0]

    attributes_dict = {'sex_id': sex_id,
                       'cause_id': cause_id,
                       'age_start': age_start,
                       'age_end': age_end,
                       'gbd_round_id': gbd_round_id,
                       'refresh_id': refresh_id,
                       'decomp_step_id': decomp_step_id}

    for model in [global_model_version_id, developed_model_version_id]:
        for att in attributes_dict.keys():
            try:
                check_model_attribute(model_version_id=model,
                                      model_attribute_name=att,
                                      model_attribute=attributes_dict[att],
                                      conn_def=conn_def)
            except ValueError:
                emails.send_mismatch_email(global_model_version_id,
                                           developed_model_version_id,
                                           att, user, conn_def=conn_def)
                raise RuntimeError("Exiting hybridizer because of {} mismatch "
                                   "between global and developed models.".format(att))

    return cause_id, sex_id, age_start, age_end, refresh_id, decomp_step_id


def get_params(global_model_version_id,
               developed_model_version_id,
               conn_def, user):
    """
    Get parameters for hybrid model version feeders --> hybrid model.
    """
    logger.info("Getting model parameters for GLB {}, DR {}".format(global_model_version_id,
                                                                    developed_model_version_id))
    feeder_model_data = database.read_input_model_data(global_model_version_id,
                                                       developed_model_version_id,
                                                       conn_def)

    params = validate_params(global_model_version_id=global_model_version_id,
                             developed_model_version_id=developed_model_version_id,
                             feeder_model_data=feeder_model_data,
                             conn_def=conn_def, user=user)

    return params


def tag_location_from_path(path, location_id):
    """
    Tag whether or not a location ID is in a path

    :param path: list of ints
        list of location id's to search
    :param location_id: int
        location_id to search for in path
    :return: boolean
        True if location_id is in path, False otherwise
    """
    return location_id in path


def get_locations_for_models(developed_model_id, location_set_version_id, conn_def):
    """
    Marks the locations in a given location set verion as either developed
    (data rich) or global (not data rich) and returns a list of the locations
    in each of those two categories

    :param developed_model_id: int
        model from which certain locations were excluded
    :param location_set_version_id: int
        which location set version the location_ids come from
    :param conn_def: str
        which server name to use (full name)
    :return: tuple(list of ints, list of ints)
        list of data-rich location ids and list of global location ids
        corresponding to the input developed model
    """
    logger.info("Getting locations for both models.")
    excluded_locations = get_excluded_locations(developed_model_id, conn_def)
    loc_hierarchy_data = get_locations(location_set_version_id, conn_def)
    # Set the 'global_model' variable to False initially, then update to True
    # for non-data rich locations
    loc_hierarchy_data['global_model'] = False
    for location_id in excluded_locations:
        # if an excluded location is in the path to the top parent, mark the
        # model as global
        loc_hierarchy_data.ix[loc_hierarchy_data['global_model'] == False, 'global_model'] = \
            loc_hierarchy_data['path_to_top_parent'].\
            map(lambda x: tag_location_from_path(x.split(','), location_id))
        loc_hierarchy_data = \
            loc_hierarchy_data.ix[(loc_hierarchy_data['is_estimate'] == 1)]

    developed_location_list = \
        loc_hierarchy_data.ix[loc_hierarchy_data['global_model'] == False, 'location_id'].tolist()
    global_location_list = \
        loc_hierarchy_data.ix[loc_hierarchy_data['global_model'] == True, 'location_id'].tolist()

    return global_location_list, developed_location_list



