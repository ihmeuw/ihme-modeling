import argparse
import logging
from typing import List

import pandas as pd

import ihme_cc_perd_translation
from db_queries.api.internal import get_location_hierarchy_by_version
from ihme_cc_perd_translation.cli import translate
from ihme_cc_rules_client import ResearchAreas, Rules, RulesManager, Tools
from jobmon.client import api as jobmon

import hybridizer.database as database
from hybridizer import emails
from hybridizer.database import check_model_attribute, get_latest_release_id
from hybridizer.reference import db_connect

logger = logging.getLogger(__name__)


def get_model_properties(model_version_id: int, conn_def: str) -> dict:
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
    model_data = db_connect.execute_select(
        call, conn_def=conn_def, parameters={"model_version_id": model_version_id}
    )
    output = {}
    for c in model_data.columns:
        output[c] = model_data.loc[0, c]
    return output


def parse_locations_exclude_string(locations_exclude: str) -> List[str]:
    """Parse cod.model_version.locations_exclude by ' ' delimiter into a list."""
    return [] if locations_exclude == "" else locations_exclude.split(" ")


def get_excluded_locations(datarich_model: int, conn_def: str) -> list:
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
    return parse_locations_exclude_string(model_data["locations_exclude"])


def validate_params(
    global_model_version_id: int,
    datarich_model_version_id: int,
    feeder_model_data: pd.DataFrame,
    conn_def: str,
    user: str,
) -> tuple:
    """
    Validates that the main parameters in the global
    and the data-rich model match up.
    """
    # Isolate the variables that have been read in as a dataframe
    cause_id = feeder_model_data["cause_id"].iloc[0]
    sex_id = feeder_model_data["sex_id"].iloc[0]
    age_start = feeder_model_data["age_start"].iloc[0]
    age_end = feeder_model_data["age_end"].iloc[0]
    refresh_id = feeder_model_data["refresh_id"].max()
    envelope_proc_version_id = feeder_model_data["envelope_proc_version_id"].max()
    population_proc_version_id = feeder_model_data["population_proc_version_id"].max()

    global_release_id = feeder_model_data[
        feeder_model_data["model_version_id"] == global_model_version_id
    ]["release_id"].iloc[0]
    datarich_release_id = feeder_model_data[
        feeder_model_data["model_version_id"] == datarich_model_version_id
    ]["release_id"].iloc[0]

    # if GBD release IDs are different, set choose most recent
    release_id = (
        get_latest_release_id(
            global_release_id=global_release_id,
            datarich_release_id=datarich_release_id,
            conn_def=conn_def,
        )
        if global_release_id != datarich_release_id
        else datarich_release_id
    )

    # TODO (CCOMP-6945): add back check that refresh, envelope, and population versions align
    attributes_dict = {
        "cause_id": cause_id,
        "sex_id": sex_id,
        "age_start": age_start,
        "age_end": age_end,
        # 'refresh_id': refresh_id,
        # 'envelope_proc_version_id': envelope_proc_version_id,
        # 'population_proc_version_id': population_proc_version_id,
    }

    # check if GBD release ID mismatch is valid with rules
    if (datarich_release_id != release_id) | (
        global_release_id
        not in RulesManager(
            research_area=ResearchAreas.COD, tool=Tools.CODEM, release_id=release_id
        ).get_rule_value(Rules.GLOBAL_MODELS_IN_RELEASES_CAN_HYBRIDIZE)
    ):
        attributes_dict["release_id"] = release_id

    for model in [global_model_version_id, datarich_model_version_id]:
        for att in attributes_dict.keys():
            try:
                check_model_attribute(
                    model_version_id=model,
                    model_attribute_name=att,
                    model_attribute=attributes_dict[att],
                    conn_def=conn_def,
                )
            except ValueError:
                emails.send_mismatch_email(
                    global_model_version_id,
                    datarich_model_version_id,
                    att,
                    user,
                    conn_def=conn_def,
                )
                raise RuntimeError(  # noqa: B904
                    "Exiting hybridizer because of {} mismatch "
                    "between global and data-rich models.".format(att)
                )

    return (
        cause_id,
        sex_id,
        age_start,
        age_end,
        refresh_id,
        envelope_proc_version_id,
        population_proc_version_id,
        release_id,
    )


def get_params(
    global_model_version_id: int, datarich_model_version_id: int, conn_def: str, user: str
) -> tuple:
    """Get parameters for hybrid model version feeders --> hybrid model."""
    logger.info(
        "Getting model parameters for GLB {}, DR {}".format(
            global_model_version_id, datarich_model_version_id
        )
    )
    feeder_model_data = database.read_input_model_data(
        global_model_version_id, datarich_model_version_id, conn_def
    )

    params = validate_params(
        global_model_version_id=global_model_version_id,
        datarich_model_version_id=datarich_model_version_id,
        feeder_model_data=feeder_model_data,
        conn_def=conn_def,
        user=user,
    )

    return params


def tag_location_from_path(path: str, location_id: int) -> bool:
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


def get_locations_for_models(
    datarich_model_id: int, location_set_version_id: int, conn_def: str
) -> tuple:
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
    loc_hierarchy_data["global_model"] = False
    for location_id in excluded_locations:
        # if an excluded location is in the path to the top parent, mark the
        # model as global
        loc_hierarchy_data.loc[
            ~loc_hierarchy_data["global_model"], "global_model"
        ] = loc_hierarchy_data["path_to_top_parent"].map(
            lambda x, location_id=location_id: tag_location_from_path(
                x.split(","), location_id
            )
        )
        loc_hierarchy_data = loc_hierarchy_data.loc[loc_hierarchy_data["is_estimate"] == 1]

    datarich_location_list = loc_hierarchy_data.loc[
        ~loc_hierarchy_data["global_model"], "location_id"
    ].tolist()
    global_location_list = loc_hierarchy_data.loc[
        loc_hierarchy_data["global_model"], "location_id"
    ].tolist()

    return global_location_list, datarich_location_list


def translate_to_perd(model_version_id: int, release_id: int) -> None:
    """Translate estimates and draws to point estimates and reduced draws (PERD).

    Used as an interim utility during the transition to PERD for GBD 2024.
    """
    try:
        perd_translation_id = ihme_cc_perd_translation.read_default_perd_translation_id()
        translate.translate(
            model_version_id=model_version_id,
            perd_translation_id=perd_translation_id,
            release_id=release_id,
            logging_level="INFO",
        )
    except Exception as err:
        logger.warning("Failed to translate to point estimates and reduced draws.")
        logger.info(
            f"Translation Error:\n{err}\n" "This error won't affect saving your model."
        )


def get_args() -> argparse.Namespace:
    """Get arguments from CODEm model."""
    parser = argparse.ArgumentParser(description="Run CODEm Hybridizer")
    parser.add_argument("--user", help="user launching hybrid", type=str)
    parser.add_argument("--model_version_id", help="the model version id of hybrid", type=int)
    parser.add_argument(
        "--global_model_version_id", help="global model version id to hybridize", type=int
    )
    parser.add_argument(
        "--datarich_model_version_id",
        help="data rich model version id to hybridize",
        type=int,
    )
    parser.add_argument("--release_id", help="GBD release ID of hybrid model", type=int)
    parser.add_argument("--conn_def", help="connection definition", type=str, required=True)
    args = parser.parse_args()
    return args


def get_jobmon_tool() -> jobmon.Tool:
    """Gets the jobmon tool for the CODEm Hybridizer."""
    return jobmon.Tool(name="CODEm Hybridizer")
