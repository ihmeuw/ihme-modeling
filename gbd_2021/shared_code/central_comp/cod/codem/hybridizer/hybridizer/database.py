import json
import logging
import os

import pandas as pd

from db_queries.api.internal import (
    get_active_cause_set_version,
    get_active_location_set_version,
)
from db_tools import ezfuncs, query_tools
from gbd.decomp_step import decomp_step_from_decomp_step_id

import hybridizer
import hybridizer.reference.db_connect as db_connect

logger = logging.getLogger(__name__)

REFRESH_ID_EXCEPTIONS = {29: [28],
                         32: [31],
                         33: [32, 31],
                         36: [35],
                         37: [36],
                         38: [37, 36],
                         57: [55, 58],
                         58: [57]
                         }


def invalid_attribute(attribute_to_check, model_attribute_name,
                      model_attribute):
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
    checking_refresh_id = model_attribute_name == 'refresh_id'
    raw_mismatch = attribute_to_check != model_attribute
    refresh_id_mismatch = checking_refresh_id and (
        attribute_to_check not in REFRESH_ID_EXCEPTIONS[model_attribute] if (
            raw_mismatch and model_attribute in REFRESH_ID_EXCEPTIONS
        ) else raw_mismatch
    )
    general_id_mismatch = not checking_refresh_id and raw_mismatch
    return refresh_id_mismatch or general_id_mismatch


def check_model_attribute(model_version_id, model_attribute_name,
                          model_attribute, conn_def):
    """
    Checks that the specific model version is truly associated with the model attribute
    that is specified.
    :param model_version_id:
    :param model_attribute_name:
    :param model_attribute:
    :param conn_def:
    :return:
    """
    call = '''
        SELECT {model_attribute_name}
        FROM cod.model_version
        WHERE model_version_id = :model_version_id
        '''.format(model_attribute_name=model_attribute_name)
    attribute_to_check = ezfuncs.query(
        call, conn_def=conn_def, parameters={
            'model_version_id': model_version_id
        }
    )[model_attribute_name].iloc[0]
    if invalid_attribute(attribute_to_check, model_attribute_name, model_attribute):
        raise ValueError('The model attribute for {} in model_version {} does not match up!'.
                         format(model_attribute_name, model_version_id))


def read_input_model_data(global_model_version_id,
                          datarich_model_version_id, conn_def):
    logger.info("Reading input model data.")
    # Read input model data from SQL
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
                     mv.decomp_step_id
                 FROM
                     cod.model_version mv
                 JOIN
                     shared.cause c USING (cause_id)
                 WHERE
                     model_version_id IN (:global_model_version_id,
                                          :datarich_model_version_id)
                 """
    feeder_model_data = ezfuncs.query(
        sql_query, conn_def=conn_def, parameters={
            'global_model_version_id': global_model_version_id,
            'datarich_model_version_id': datarich_model_version_id
        }
    )
    return feeder_model_data


def acause_from_id(model_version_id, conn_def):
    """
    Given a valid model version id returns the acause associated with it.

    :param model_version_id: int
        valid model version id
    :param conn_def: str
    :return: str
        string representing an acause
    """
    acause = ezfuncs.query(
        """
        SELECT acause
        FROM shared.cause
        WHERE cause_id = (SELECT cause_id
                      FROM cod.model_version
                      WHERE model_version_id = :model_version_id)
        """,
        conn_def=conn_def,
        parameters={'model_version_id': model_version_id}
    )["acause"][0]
    return acause


def upload_hybrid_metadata(global_model_version_id, datarich_model_version_id,
                           cause_id, sex_id, age_start, age_end,
                           refresh_id, envelope_proc_version_id,
                           population_proc_version_id, decomp_step_id,
                           gbd_round_id, conn_def, user):
    """
    Creates a hybrid model entry and uploads all associated metadata.

    :return: model_version_id of the hybrid model
    """
    logger.info(
        "Creating model version for hybrid model from {} and {}".format(
            global_model_version_id, datarich_model_version_id
        )
    )
    # Get version id's for cause and location sets and metadata
    decomp_step = decomp_step_from_decomp_step_id(decomp_step_id)
    cause_set_version_id = get_active_cause_set_version(cause_set_id=2,
                                                        gbd_round_id=gbd_round_id,
                                                        decomp_step=decomp_step)
    # Location set 35 corresponds to the set ID for the location hierarchy
    # used by the CODEm hybridizer
    location_set_version_id = (
        get_active_location_set_version(
            35, gbd_round_id=gbd_round_id, decomp_step=decomp_step
        )
    )
    description = "Hybrid of models {global_model_version_id} and {datarich_model_version_id}".format(global_model_version_id=global_model_version_id,
               datarich_model_version_id=datarich_model_version_id)
    metadata = pd.DataFrame({
        'gbd_round_id': [gbd_round_id],
        'cause_id': [cause_id],
        'description': [description],
        'location_set_version_id': [location_set_version_id],
        'cause_set_version_id': [cause_set_version_id],
        'previous_model_version_id': [global_model_version_id],
        'status': [0],
        'is_best': [0],
        'environ': [20],
        'sex_id': [sex_id],
        'age_start': [age_start],
        'age_end': [age_end],
        'locations_exclude': [''],
        'model_version_type_id': [3],
        'refresh_id': [refresh_id],
        'envelope_proc_version_id': [envelope_proc_version_id],
        'population_proc_version_id': [population_proc_version_id],
        'decomp_step_id': [decomp_step_id],
        'inserted_by': [user],
        'last_updated_by': [user],
        'last_updated_action': ['INSERT']
    })

    model_version_id = db_connect.write_metadata(df=metadata,
                                                 db='cod',
                                                 table='model_version',
                                                 conn_def=conn_def)
    if len(model_version_id) != 1:
        raise RuntimeError("Error in uploading metadata, returning more or less than one row for hybrids!")
    model_version_id = model_version_id[0]

    code_version = json.dumps(
        {'branch': os.environ['CONDA_DEFAULT_ENV'].replace("codem_", ""),
         'tag': hybridizer.__version__
         })
    call = """
       UPDATE cod.model_version
       SET code_version = :code_version
       WHERE model_version_id = :model_version_id
       """
    with ezfuncs.session_scope(conn_def) as scoped_session:
        query_tools.exec_query(
            call, session=scoped_session,
            parameters={
                'code_version': code_version,
                'model_version_id': model_version_id
            }
        )

    parent_child = pd.DataFrame({
        'parent_id': [model_version_id, model_version_id],
        'child_id': [global_model_version_id, datarich_model_version_id],
        'model_version_relation_note': ["Hybrid Model", "Hybrid Model"],
        'inserted_by': [user, user],
        'last_updated_by': [user, user],
        'last_updated_action': ['INSERT', 'INSERT']
    })

    db_connect.write_data(df=parent_child, db='cod', table='model_version_relation', conn_def=conn_def)

    logger.info("All metadata uploaded: new model version is {}".format(model_version_id))
    return model_version_id


def update_model_status(model_version_id, status, conn_def):
    logger.info("Updating status of {} to {}".format(model_version_id,
                                                     status))
    # Update status on model_version table
    call = """
        UPDATE cod.model_version
        SET status = :status, last_updated = NOW()
        WHERE model_version_id = :model_version_id
        """
    with ezfuncs.session_scope(conn_def) as scoped_session:
        query_tools.exec_query(
            call, session=scoped_session,
            parameters={
                'status': status,
                'model_version_id': model_version_id
            }
        )
