import json
import logging
from typing import Union

import pandas as pd

from db_queries.api.internal import (
    get_active_cause_set_version,
    get_active_location_set_version,
)
from gbd.release import get_gbd_round_id_from_release

import hybridizer
from hybridizer.reference import db_connect

logger = logging.getLogger(__name__)

REFRESH_ID_EXCEPTIONS = {
    29: [28],
    32: [31],
    33: [32, 31],
    36: [35],
    37: [36],
    38: [37, 36],
    57: [55, 58],
    58: [57],
}


def invalid_attribute(
    attribute_to_check: Union[int, str],
    model_attribute_name: str,
    model_attribute: Union[int, str],
) -> bool:
    """
    Validate logic for one single attribute to check against the model attribute
    it should be. Put this in its own function because of the refresh ID checking
    logic.

    REFRESH_ID_EXCEPTIONS is a global dictionary that indicates refreshes
    that can be hybridized together.

    Briefly, the logic is this:
    - If we *aren't* checking refresh_id, then just let us know if it's a general mismatch
    - If we *are* checking refresh_id:
        - If there is a raw mismatch, but there is an exception list entered for the
          true model_attribute, then check to see if our attribute_to_check is in that list
        - If there is a mismatch, but there is not an exception list entered
          for the true model attribute, then return the raw mismatch
        - If there is not a raw mismatch, then return that there is not a mismatch
    :param attribute_to_check: the attribute to cross-check with model_attribute
    :param model_attribute_name: the name of the model attribute to check
    :param model_attribute: what the attribute to check *should* be, except in certain
                            situations (refresh ID exceptions)
    :return: whether or not this is a valid attribute
    """
    checking_refresh_id = model_attribute_name == "refresh_id"
    raw_mismatch = attribute_to_check != model_attribute
    refresh_id_mismatch = checking_refresh_id and (
        attribute_to_check not in REFRESH_ID_EXCEPTIONS[model_attribute]
        if (raw_mismatch and model_attribute in REFRESH_ID_EXCEPTIONS)
        else raw_mismatch
    )
    general_id_mismatch = not checking_refresh_id and raw_mismatch
    return refresh_id_mismatch or general_id_mismatch


def check_model_attribute(
    model_version_id: int,
    model_attribute_name: str,
    model_attribute: Union[int, str],
    conn_def: str,
) -> None:
    """
    Checks that the specific model version is truly associated with the model attribute
    that is specified.
    :param model_version_id:
    :param model_attribute_name:
    :param model_attribute:
    :param conn_def:
    :return:
    """
    call = f"""
        SELECT {model_attribute_name}
        FROM cod.model_version
        WHERE model_version_id = :model_version_id
        """  # nosec: B608
    attribute_to_check = db_connect.execute_select(
        call, conn_def=conn_def, parameters={"model_version_id": model_version_id}
    )[model_attribute_name].iloc[0]
    if invalid_attribute(attribute_to_check, model_attribute_name, model_attribute):
        raise ValueError(
            "The model attribute for {} in model_version {} does not match up!".format(
                model_attribute_name, model_version_id
            )
        )


def read_input_model_data(
    global_model_version_id: int, datarich_model_version_id: int, conn_def: str
) -> pd.DataFrame:
    """Read input model data from SQL."""
    logger.info("Reading input model data.")
    sql_query = """
        SELECT
             mv.model_version_id,
             mv.cause_id,
             c.acause,
             mv.sex_id,
             mv.age_start,
             mv.age_end,
             mv.gbd_round_id,
             mv.refresh_id,
             mv.envelope_proc_version_id,
             mv.population_proc_version_id,
             mv.release_id
         FROM
             cod.model_version mv
         JOIN
             shared.cause c USING (cause_id)
         WHERE
             model_version_id IN (:global_model_version_id, :datarich_model_version_id)
         """
    feeder_model_data = db_connect.execute_select(
        sql_query,
        conn_def=conn_def,
        parameters={
            "global_model_version_id": global_model_version_id,
            "datarich_model_version_id": datarich_model_version_id,
        },
    )
    return feeder_model_data


def acause_from_id(model_version_id: int, conn_def: str) -> str:
    """
    Given a valid model version id returns the acause associated with it.

    :param model_version_id: int
        valid model version id
    :param conn_def: str
    :return: str
        string representing an acause
    """
    acause = db_connect.execute_select(
        """
        SELECT acause
        FROM shared.cause
        WHERE cause_id = (
            SELECT cause_id
            FROM cod.model_version
            WHERE model_version_id = :model_version_id
        )
        """,
        conn_def=conn_def,
        parameters={"model_version_id": model_version_id},
    )["acause"][0]
    return acause


def upload_hybrid_metadata(
    global_model_version_id: int,
    datarich_model_version_id: int,
    cause_id: int,
    sex_id: int,
    age_start: int,
    age_end: int,
    refresh_id: int,
    envelope_proc_version_id: int,
    population_proc_version_id: int,
    release_id: int,
    conn_def: str,
    user: str,
) -> int:
    """
    Creates a hybrid model entry and uploads all associated metadata.

    Returns:
        model_version_id of the hybrid model
    """
    logger.info(
        "Creating model version for hybrid model from {} and {}".format(
            global_model_version_id, datarich_model_version_id
        )
    )
    cause_set_version_id = get_active_cause_set_version(cause_set_id=2, release_id=release_id)
    # Location set 35 corresponds to the set ID for the location hierarchy
    # used by the CODEm hybridizer
    location_set_version_id = get_active_location_set_version(35, release_id=release_id)
    description = (
        "Hybrid of models {global_model_version_id} and {datarich_model_version_id}".format(
            global_model_version_id=global_model_version_id,
            datarich_model_version_id=datarich_model_version_id,
        )
    )
    metadata = pd.DataFrame(
        {
            "gbd_round_id": [get_gbd_round_id_from_release(release_id)],
            "cause_id": [cause_id],
            "description": [description],
            "location_set_version_id": [location_set_version_id],
            "cause_set_version_id": [cause_set_version_id],
            "previous_model_version_id": [global_model_version_id],
            "status": [0],
            "is_best": [0],
            "environ": [20],
            "sex_id": [sex_id],
            "age_start": [age_start],
            "age_end": [age_end],
            "locations_exclude": [""],
            "model_version_type_id": [3],
            "refresh_id": [refresh_id],
            "envelope_proc_version_id": [envelope_proc_version_id],
            "population_proc_version_id": [population_proc_version_id],
            "release_id": [release_id],
            "inserted_by": [user],
            "last_updated_by": [user],
            "last_updated_action": ["INSERT"],
        }
    )

    model_version_id = db_connect.write_metadata(
        df=metadata, db="cod", table="model_version", conn_def=conn_def
    )
    if len(model_version_id) != 1:
        raise RuntimeError(
            "Error in uploading metadata, returning more or less than one row for hybrids!"
        )
    model_version_id = int(model_version_id[0])

    code_version = json.dumps({"branch": "main", "tag": hybridizer.__version__})
    call = """
       UPDATE cod.model_version
       SET code_version = :code_version
       WHERE model_version_id = :model_version_id
       """
    db_connect.execute_call(
        call=call,
        conn_def=conn_def,
        parameters={"code_version": code_version, "model_version_id": model_version_id},
    )

    parent_child = pd.DataFrame(
        {
            "parent_id": [model_version_id, model_version_id],
            "child_id": [global_model_version_id, datarich_model_version_id],
            "model_version_relation_note": ["Hybrid Model", "Hybrid Model"],
            "inserted_by": [user, user],
            "last_updated_by": [user, user],
            "last_updated_action": ["INSERT", "INSERT"],
        }
    )

    db_connect.write_data(
        df=parent_child, db="cod", table="model_version_relation", conn_def=conn_def
    )

    logger.info("All metadata uploaded: new model version is {}".format(model_version_id))
    return model_version_id


def update_model_status(model_version_id: int, status: int, conn_def: str) -> None:
    """Update status on model_version table."""
    logger.info("Updating status of {} to {}".format(model_version_id, status))
    call = """
        UPDATE cod.model_version
        SET status = :status, last_updated = NOW(), last_updated_action = 'UPDATE'
        WHERE model_version_id = :model_version_id
        """
    db_connect.execute_call(
        call=call,
        conn_def=conn_def,
        parameters={"status": status, "model_version_id": model_version_id},
    )


def get_latest_release_id(
    global_release_id: int, datarich_release_id: int, conn_def: str
) -> int:
    """
    Given two release_ids, returns the release_id that is more recent. Validates
    that both releases are within the same round.
    """
    call = """
        SELECT release_id, gbd_round_id, release_sort_order
        FROM shared.release
        JOIN shared.release_gbd_round USING (release_id)
        WHERE release_id IN :release_id
    """

    df = db_connect.execute_select(
        query=call,
        conn_def=conn_def,
        parameters={"release_id": [global_release_id, datarich_release_id]},
    )

    if len(df["gbd_round_id"].unique()) != 1:
        global_gbd_round_id = df[df["release_id"] == global_release_id]["gbd_round_id"].iloc[
            0
        ]
        data_rich_gbd_round_id = df[df["release_id"] == datarich_release_id][
            "gbd_round_id"
        ].iloc[0]
        raise ValueError(
            f"CODEm model versions from different GBD rounds are not allowed to be "
            f"hybridized, given global model version from GBD round {global_gbd_round_id} "
            f"and data rich model version from GBD round {data_rich_gbd_round_id}"
        )
    # shared.release has primary key on release_id, length of df should always be 2
    return df[df["release_sort_order"] == df["release_sort_order"].max()]["release_id"].iloc[
        0
    ]


def get_mortality_run_id(proc_version_id: int) -> int:
    """Get run_id associated with process_version_id in the mortality database"""
    return db_connect.execute_select(
        """SELECT run_id
        FROM mortality.process_version
        WHERE proc_version_id = :proc_version_id""",
        conn_def="mortality",
        parameters={"proc_version_id": proc_version_id},
    )["run_id"].iloc[0]
