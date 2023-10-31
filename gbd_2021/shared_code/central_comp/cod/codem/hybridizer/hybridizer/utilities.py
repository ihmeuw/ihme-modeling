import logging

from db_queries.api.internal import get_location_hierarchy_by_version
from db_tools import ezfuncs

import hybridizer.database as database
from hybridizer import emails
from hybridizer.database import check_model_attribute

logger = logging.getLogger(__name__)


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
    call = """
    SELECT *
    FROM cod.model_version
    WHERE model_version_id = :model_version_id
    """
    model_data = ezfuncs.query(
        call, conn_def=conn_def, parameters={"model_version_id": model_version_id}
    )
    output = {}
    for c in model_data.columns:
        output[c] = model_data.loc[0, c]
    return output


def get_excluded_locations(datarich_model, conn_def):
    """
    Get a list of the excluded locations from the data-rich model

    We will use these IDs to figure out which locations to pull from the global
    model and which ones to pull from the data-rich model.

    :param datarich_model: int
        model_version_id for the data-rich model of interest
    :param conn_def:
        which full server name to use
    :return: list of ints
        list of the location ids that are excluded in the given data-rich model
    """
    logger.info("Getting excluded locations for {}.".format(datarich_model))
    model_data = get_model_properties(datarich_model, conn_def)
    return model_data['locations_exclude'].split(' ')


def validate_params(global_model_version_id,
                    datarich_model_version_id,
                    feeder_model_data,
                    conn_def,
                    user):
    """
    Validates that the main parameters in the global
    and the data-rich model match up.
    """

    # Isolate the variables that have been read in as a dataframe
    cause_id = feeder_model_data['cause_id'].iloc[0]
    sex_id = feeder_model_data['sex_id'].iloc[0]
    age_start = feeder_model_data['age_start'].iloc[0]
    age_end = feeder_model_data['age_end'].iloc[0]
    refresh_id = feeder_model_data['refresh_id'].max()
    envelope_proc_version_id = feeder_model_data['envelope_proc_version_id'].max()
    population_proc_version_id = feeder_model_data['population_proc_version_id'].max()
    gbd_round_id = feeder_model_data['gbd_round_id'].iloc[0]
    decomp_step_id = feeder_model_data['decomp_step_id'].iloc[0]

    attributes_dict = {'cause_id': cause_id,
                       'sex_id': sex_id,
                       'age_start': age_start,
                       'age_end': age_end,
                       'refresh_id': refresh_id,
                       'envelope_proc_version_id': envelope_proc_version_id,
                       'population_proc_version_id': population_proc_version_id,
                       'gbd_round_id': gbd_round_id,
                       'decomp_step_id': decomp_step_id,
                       }

    for model in [global_model_version_id, datarich_model_version_id]:
        for att in attributes_dict.keys():
            try:
                check_model_attribute(model_version_id=model,
                                      model_attribute_name=att,
                                      model_attribute=attributes_dict[att],
                                      conn_def=conn_def)
            except ValueError:
                emails.send_mismatch_email(global_model_version_id,
                                           datarich_model_version_id,
                                           att, user, conn_def=conn_def)
                raise RuntimeError("Exiting hybridizer because of {} mismatch "
                                   "between global and data-rich models.".format(att))

    return (cause_id, sex_id, age_start, age_end, refresh_id,
            envelope_proc_version_id, population_proc_version_id, decomp_step_id,
            gbd_round_id)


def get_params(global_model_version_id,
               datarich_model_version_id,
               conn_def, user):
    """
    Get parameters for hybrid model version feeders --> hybrid model.
    """
    logger.info("Getting model parameters for GLB {}, DR {}".format(
        global_model_version_id, datarich_model_version_id)
    )
    feeder_model_data = database.read_input_model_data(
        global_model_version_id, datarich_model_version_id, conn_def
    )

    params = validate_params(global_model_version_id=global_model_version_id,
                             datarich_model_version_id=datarich_model_version_id,
                             feeder_model_data=feeder_model_data,
                             conn_def=conn_def,
                             user=user)

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


def get_locations_for_models(datarich_model_id,
                             location_set_version_id,
                             conn_def):
    """
    Marks the locations in a given location set version as either data-rich
    (data rich) or global (not data rich) and returns a list of the locations
    in each of those two categories

    :param datarich_model_id: int
        model from which certain locations were excluded
    :param location_set_version_id: int
        which location set version the location_ids come from
    :param conn_def: str
        which server name to use (full name)
    :return: tuple(list of ints, list of ints)
        list of data-rich location ids and list of global location ids
        corresponding to the input data-rich model
    """
    logger.info("Getting locations for both models.")
    excluded_locations = get_excluded_locations(datarich_model_id, conn_def)
    loc_hierarchy_data = get_location_hierarchy_by_version(location_set_version_id)
    # Set the 'global_model' variable to False initially, then update to True
    # for non-data rich locations
    loc_hierarchy_data['global_model'] = False
    for location_id in excluded_locations:
        # if an excluded location is in the path to the top parent, mark the
        # model as global
        loc_hierarchy_data.loc[
            loc_hierarchy_data['global_model'] == False, 'global_model'
        ] = loc_hierarchy_data['path_to_top_parent'].map(
            lambda x: tag_location_from_path(x.split(','), location_id)
        )
        loc_hierarchy_data = loc_hierarchy_data.loc[
            loc_hierarchy_data['is_estimate'] == 1
        ]

    datarich_location_list = loc_hierarchy_data.loc[
        loc_hierarchy_data['global_model'] == False, 'location_id'
    ].tolist()
    global_location_list = loc_hierarchy_data.loc[
        loc_hierarchy_data['global_model'] == True, 'location_id'
    ].tolist()

    return global_location_list, datarich_location_list
