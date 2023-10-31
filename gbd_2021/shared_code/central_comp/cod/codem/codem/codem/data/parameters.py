"""
Functions to query the model parameters for CODEm models.
Any new parameters that need to be queried should be put in get_model_parameters dictionary
"""
import logging

import numpy as np
import pandas as pd

import gbd.constants as gbd
from db_queries import get_envelope, get_population
from db_queries.api.internal import get_active_location_set_version
from gbd.decomp_step import decomp_step_from_decomp_step_id, decomp_step_id_from_decomp_step

import codem.reference.db_connect as db_connect
from codem.devQueries.utilities import age_id_to_name

logger = logging.getLogger(__name__)

OUTLIERS_FOR_BEST_CUSTOM_MODELS = {
    # Dictionary containing model versions to pull outliers from when a custom
    # model from a previous step is marked best. For example, when a custom
    # model is marked best for step2, a step3 model cannot look to that step2
    # model's outliers because they don't exist. Instead, pull outliers from
    # the model versions defined here, to be added to as necessary.
    # These are indexed by: [decomp_step_id][cause_id][sex_id][
    # model_version_type_id] -> [model_version_id]
    2: {
        319: {gbd.sex.MALE: {2: [603320]},
              gbd.sex.FEMALE: {2: [603323]}},
        320: {gbd.sex.MALE: {2: [604937]},
              gbd.sex.FEMALE: {2: [604934]}},
        338: {gbd.sex.MALE: {1: [600503],
                             2: [600503]},
              gbd.sex.FEMALE: {1: [600506],
                               2: [600506]}},
        339: {gbd.sex.MALE: {2: [600491]},
              gbd.sex.FEMALE: {2: [600494]}},
        341: {gbd.sex.MALE: {2: [603896]},
              gbd.sex.FEMALE: {2: [603911]}},
        342: {gbd.sex.MALE: {2: [600497]},
              gbd.sex.FEMALE: {2: [600500]}},
        366: {gbd.sex.FEMALE: {1: [604283],
                               2: [604286]}}
    },
    # TODO: Fill with models when a GBD 2020 step2 custom model is marked best

}


def check_if_new_cause(cause_id, gbd_round_id, db_connection):
    """
    Check if something is a new cause being modeled by CODEm during
    the methods change step for decomp (step3)

    :param cause_id: (int)
    :param gbd_round_id (int)
    :param db_connection (str)
    :return: (bool)
    """
    # Dictionary of decomp_step_ids per GBD round that need to be checked for
    # completed models. References steps prior to the step that is adding new
    # causes for the GBD round
    steps_to_check_for_models = {
        6: [gbd.decomp_step.ONE, gbd.decomp_step.TWO],
        7: [gbd.decomp_step.ONE, gbd.decomp_step.TWO]
    }

    steps = steps_to_check_for_models[gbd_round_id]
    steps = ', '.join([str(decomp_step_id_from_decomp_step(
        step, gbd_round_id=gbd_round_id)) for step in steps])
    call = (
        """SELECT COUNT(*) AS count
        FROM cod.model_version
        WHERE cause_id = {cause_id}
        AND gbd_round_id = {gbd_round_id}
        AND model_version_type_id IN (0, 1, 2, 11)
        AND decomp_step_id IN ({decomp_steps})
        """.format(cause_id=cause_id,
                   gbd_round_id=gbd_round_id,
                   decomp_steps=steps)
    )
    count = db_connect.query(call, db_connection)['count'][0]
    return count == 0


def get_run_id(process_version_id):
    """
    Query the mortality database to get the run_id
    associated

    :param process_version_id: (int) process version ID for mortality DB
    :return: (int) run_id
    """
    call = ("SELECT run_id FROM mortality.process_version "
            "WHERE proc_version_id = {}")
    run_id = db_connect.query(call.format(process_version_id),
                              'ADDRESS')['run_id'][0]
    return run_id


def get_modeler(cause_id, gbd_round_id, db_connection):
    """
    Gets the modeler from the cod.modeler table that is associated with the
    given cause and GBD round
    """
    call = f'''SELECT username
               FROM cod.modeler
               WHERE cause_id = '{cause_id}'
               AND gbd_round_id = {gbd_round_id}'''
    modeler = db_connect.query(call, db_connection)
    return modeler.iloc[0, 0].split(', ')


def get_cluster_project(cause_id, gbd_round_id, db_connection):
    """
    Gets the cluster project from the cod.codem_cluster_project table that is
    associated with the given cause and GBD round
    """
    call = f'''SELECT cluster_project
               FROM cod.codem_cluster_project
               WHERE cause_id = '{cause_id}'
               AND gbd_round_id = {gbd_round_id}'''
    cluster_project = db_connect.query(call, db_connection)
    if cluster_project.empty:
        cluster_project = "proj_codem"
    else:
        cluster_project = cluster_project.cluster_project.iat[0]
    return cluster_project


def get_best_model_version(gbd_round_id,
                           decomp_step_id,
                           cause_id,
                           sex_id,
                           model_version_type,
                           db_connection):
    """
    Get the best model version by decomp step and GBD round and demographics.
    :param gbd_round_id: (int)
    :param decomp_step_id: (int)
    :param cause_id: (int)
    :param sex_id: (int)
    :param model_version_type: (int)
    :param db_connection: (str)
    :return:
    """
    call = (
           "SELECT cmv.model_version_id as model_version_id "
           "FROM cod.model_version cmv "
           "INNER JOIN cod.model_version_relation cmr "
           "ON cmr.child_id = cmv.model_version_id "
           "WHERE cmr.parent_id IN "
           "(SELECT model_version_id FROM cod.model_version "
           f"WHERE cause_id = {cause_id} "
           f"AND sex_id = {sex_id} "
           f"AND gbd_round_id = {gbd_round_id} "
           f"AND decomp_step_id = {decomp_step_id} "
           "AND is_best = 1 "
           "AND model_version_type_id IN (3, 11) ) "
           f"AND cmv.model_version_type_id = {model_version_type}"
    )
    versions = db_connect.query(call, db_connection)['model_version_id']
    if len(versions) in [1, 2]:
        versions = versions.tolist()
    elif cause_id in OUTLIERS_FOR_BEST_CUSTOM_MODELS[decomp_step_id]:
        versions = OUTLIERS_FOR_BEST_CUSTOM_MODELS[decomp_step_id][cause_id][
            sex_id][model_version_type
        ]
    else:
        raise RuntimeError(f"Can't find a model version marked best to use "
                           f"for outliers, for given decomp step ID "
                           f"{decomp_step_id}. Fix!")
    return versions


def outlier_decomp_step_from_decomp_step(decomp_step_id, db_connection):
    """
    Get the outlier step associated with decomp step ID passed.

    :param decomp_step_id: (int)
    :param db_connection: (str)
    :return:
    """
    call = ("SELECT outlier_decomp_step_id AS o FROM cod.decomp_outlier_step "
            f"WHERE decomp_step_id = {decomp_step_id}")
    outlier_decomp_step_id = db_connect.query(call, db_connection)['o'][0]
    return outlier_decomp_step_id


def get_outlier_decomp_step_id(decomp_step,
                               decomp_step_id,
                               iterative_data_decomp_step_id,
                               db_connection):
    """
    Get the outlier decomp step ID to use for *this* model. May return
    the same decomp step (e.g. for step 2).

    :param decomp_step:
    :param decomp_step_id:
    :param iterative_data_decomp_step_id:
    :param db_connection:
    :return:
    """
    if decomp_step == gbd.decomp_step.ITERATIVE:
        outlier_decomp_step_id = outlier_decomp_step_from_decomp_step(
            decomp_step_id=iterative_data_decomp_step_id,
            db_connection=db_connection
        )
    else:
        outlier_decomp_step_id = outlier_decomp_step_from_decomp_step(
            decomp_step_id=decomp_step_id,
            db_connection=db_connection
        )
    return outlier_decomp_step_id


def get_outlier_model_version_id(model_version_id,
                                 decomp_step,
                                 outlier_decomp_step_id,
                                 gbd_round_id,
                                 cause_id,
                                 sex_id,
                                 model_version_type, db_connection):
    """
    Get the outlier model version to use. If we're pulling the same
    outlier decomp step ID as the decomp step ID, then we just want to make
    the outlier model version the same as the model version. Same if it's a new
    cause.

    :param model_version_id: (int)
    :param decomp_step: (str)
    :param outlier_decomp_step_id: (int)
    :param gbd_round_id: (int)
    :param cause_id: (int)
    :param sex_id: (int)
    :param model_version_type: (int)
    :param db_connection: (str)
    :return:
    """
    logger.info(f"Getting outlier model version ID for {model_version_id}.")
    new_cause = check_if_new_cause(cause_id=cause_id,
                                   gbd_round_id=gbd_round_id,
                                   db_connection=db_connection)
    decomp_step_id = decomp_step_id_from_decomp_step(decomp_step,
                                                     gbd_round_id=gbd_round_id)
    # For models run in iterative, we want to pull active outliers from
    # the decomp_step_id specified by outlier_decomp_step_id.
    # We set the model version ID to use for outliers, to the new model
    # version ID of the current model running in iterative

    if (outlier_decomp_step_id == decomp_step_id) or (
            new_cause and decomp_step == gbd.decomp_step.THREE) or (
            decomp_step == gbd.decomp_step.ITERATIVE
    ):
        outlier_model_version_id = [model_version_id]
    else:
        outlier_model_version_id = get_best_model_version(
            gbd_round_id=gbd_round_id,
            decomp_step_id=outlier_decomp_step_id,
            cause_id=cause_id,
            sex_id=sex_id,
            model_version_type=model_version_type,
            db_connection=db_connection
        )
    return outlier_model_version_id


def get_model_parameters(model_version_id, db_connection, update=False):
    """
    integer -> dictionary

    Given an integer that indicates a valid model version id  the function will
    return a dictionary with keys indicating the model parameters start age,
    end age, sex, start year, cause, and whether to run covariate selection or
    not. "update" indicates whether during the querying process the database
    should be updated to running during the querying process, default is False.
    True should be used when running CODEm.
    """
    call = f"""SELECT * FROM cod.model_version
            WHERE model_version_id = {model_version_id}"""
    df = db_connect.query(call.format(model_version_id), db_connection)
    model = {k: df[k][0] for k in df.columns}
    model["time_weight_method"] = "tricubic"
    model["all_data_holdout"] = 'ko%02d' % (model["holdout_number"] + 1)
    model["db_connection"] = db_connection
    model["start_year"] = 1980
    model["deaths_measure_id"] = 1
    model['all_age_group_id'] = gbd.age.ALL_AGES
    model['global_location_id'] = 1

    cause_id = model["cause_id"]
    call = f"""SELECT acause, cause_name FROM shared.cause
            WHERE cause_id = {cause_id}"""
    acause = db_connect.query(call, db_connection)["acause"][0]
    cause_name = db_connect.query(call, db_connection)["cause_name"][0]
    model["acause"] = acause
    model["cause_name"] = cause_name

    model['age_start_name'] = age_id_to_name(model["age_start"])
    model['age_end_name'] = age_id_to_name(model["age_end"])

    sex_id = model["sex_id"]
    call = f"SELECT sex FROM shared.sex WHERE sex_id = {sex_id}"
    sex = db_connect.query(call, db_connection)["sex"][0]
    model["sex"] = sex.lower()

    gbd_round_id = model["gbd_round_id"]
    call = f"""
            SELECT gbd_round
            FROM shared.gbd_round
            WHERE gbd_round_id = {gbd_round_id}
            """
    gbd_round = int(db_connect.query(call, db_connection)["gbd_round"][0])
    model["gbd_round"] = gbd_round

    model["decomp_step"] = decomp_step_from_decomp_step_id(
        model["decomp_step_id"]
    )

    model["standard_location_set_version_id"] = get_active_location_set_version(
        101, model['gbd_round_id'], model['decomp_step']
    )

    model["modeler"] = get_modeler(cause_id=cause_id,
                                   gbd_round_id=gbd_round_id,
                                   db_connection=db_connection)
    model["cluster_project"] = get_cluster_project(
        cause_id=cause_id,
        gbd_round_id=gbd_round_id,
        db_connection=db_connection)

    model_version_type_id = model["model_version_type_id"]
    call = f"""
            SELECT model_version_type FROM cod.model_version_type
            WHERE model_version_type_id = {model_version_type_id}
            """
    model_version_type = db_connect.query(call, db_connection)[
        "model_version_type"][0]
    model["model_version_type"] = model_version_type

    model["pop_run_id"] = get_run_id(model["population_proc_version_id"])
    model["env_run_id"] = get_run_id(model["envelope_proc_version_id"])

    model["psi_values"] = np.arange(model["psi_weight_min"],
                                    model["psi_weight_max"] +
                                    model["psi_weight_int"],
                                    model["psi_weight_int"])

    model["keep_vars"] = ["level_1", "level_2", "level_3", "level_4",
                          "location_id", "age", "year", "cf", "envelope", "pop"]

    model['draw_cols'] = ["draw_%d" % i for i in range(0, 1000)] + [
        "envelope", "pop", "age", "sex", "year", "location_id", "level_1",
        "level_2", "level_3", "level_4"
    ]

    model['upload_cols'] = ["year_id", "location_id", "sex_id", "age_group_id",
                            "mean_cf", "lower_cf", "upper_cf"]

    outlier_decomp_step_id = get_outlier_decomp_step_id(
        decomp_step=model['decomp_step'],
        decomp_step_id=model['decomp_step_id'],
        iterative_data_decomp_step_id=model['iterative_data_decomp_step_id'],
        db_connection=db_connection
    )
    model['outlier_decomp_step_id'] = outlier_decomp_step_id

    outlier_model_version_id = get_outlier_model_version_id(
        model_version_id=model_version_id,
        gbd_round_id=model["gbd_round_id"],
        decomp_step=model["decomp_step"],
        outlier_decomp_step_id=model["outlier_decomp_step_id"],
        cause_id=model["cause_id"],
        sex_id=model["sex_id"],
        model_version_type=model["model_version_type_id"],
        db_connection=db_connection
    )
    model["outlier_model_version_id"] = outlier_model_version_id
    call = (f"UPDATE cod.model_version "
            f"SET outlier_model_version_id = {max(outlier_model_version_id)} "
            f"WHERE model_version_id = {model_version_id}")
    db_connect.query(call, db_connection)

    call = f"""UPDATE cod.model_version SET status = 0
               WHERE model_version_id = {model_version_id}"""

    if update:
        db_connect.query(call.format(model_version_id), db_connection)
    return model
