"""
Functions to query the model parameters for CODEm models.
Any new parameters that need to be queried should be put in get_model_parameters dictionary
"""

import numpy as np
import logging

from gbd.decomp_step import decomp_step_from_decomp_step_id
from db_queries import get_location_metadata

import codem.reference.db_connect as db_connect

logger = logging.getLogger(__name__)

EXCEPTION_OUTLIER_CAUSES = {
    2: {
        319: {1: {2: [603320]},
              2: {2: [603323]}},
        320: {1: {2: [604937]},
              2: {2: [604934]}},
        338: {1: {1: [600503],
                  2: [600503]},
              2: {1: [600506],
                  2: [600506]}},
        339: {1: {2: [600491]},
              2: {2: [600494]}},
        341: {1: {2: [603896]},
              2: {2: [603911]}},
        342: {1: {2: [600497]},
              2: {2: [600500]}},
        366: {2: {1: [604283],
                  2: [604286]}}
    }
}


def check_if_new_cause_for_step_3(cause_id, gbd_round_id, db_connection):
    """
    Check if something is a new cause being modeled by CODEm during
    the methods change step for decomp (decomp_step_id=3)

    :param cause_id: (int)
    :param gbd_round_id (int)
    :param db_connection (str)
    :return: (bool)
    """
    call = (
        f'SELECT COUNT(*) AS count '
        f'FROM cod.model_version '
        f'WHERE cause_id = {cause_id} '
        f'AND gbd_round_id = {gbd_round_id} '
        f'AND model_version_type_id IN (0, 1, 2) '
        f'AND decomp_step_id IN (1, 2)'
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
    call = "SELECT run_id FROM mortality.process_version WHERE proc_version_id = {}"
    run_id = db_connect.query(call.format(process_version_id), 'ADDRESS')['run_id'][0]
    return run_id


def get_modeler(cause_id, gbd_round_id, db_connection):
    """
    Gets the modelers from the cod.modeler table.
    """
    call = f'''SELECT username
               FROM cod.modeler
               WHERE cause_id = '{cause_id}'
               AND gbd_round_id = {gbd_round_id}'''
    modeler = db_connect.query(call, db_connection)
    return modeler.ix[0, 0].split(', ')


def get_best_model_version(gbd_round_id, decomp_step_id, cause_id,
                           sex_id, model_version_type, db_connection):
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
           "SELECT cmv.model_version_id as model_version_id FROM cod.model_version cmv "
           "INNER JOIN cod.model_version_relation cmr "
           "ON cmr.child_id = cmv.model_version_id "
           "WHERE cmr.parent_id IN "
           "(SELECT model_version_id FROM cod.model_version "
           f"WHERE cause_id = {cause_id} "
           f"AND sex_id = {sex_id} "
           f"AND gbd_round_id = {gbd_round_id} "
           f"AND decomp_step_id = {decomp_step_id} "
           "AND is_best = 1 "
           "AND model_version_type_id = 3) "
           f"AND cmv.model_version_type_id = {model_version_type}"
    )
    versions = db_connect.query(call, db_connection)['model_version_id']
    if len(versions) in [1, 2]:
        versions = versions.tolist()
    elif cause_id in EXCEPTION_OUTLIER_CAUSES[decomp_step_id]:
        versions = EXCEPTION_OUTLIER_CAUSES[decomp_step_id][cause_id][sex_id][model_version_type]
    else:
        raise RuntimeError(f"Can't find a model version to use for best outliers "
                           f"from decomp step {decomp_step_id}.")
    return versions


def outlier_decomp_step_from_decomp_step(decomp_step_id, db_connection):
    """
    Get the outlier step associated with decomp step ID passed.

    :param decomp_step_id: (int)
    :param db_connection: (str)
    :return:
    """
    call = ("SELECT outlier_decomp_step_id AS o "
            "FROM cod.decomp_outlier_step "
            f"WHERE decomp_step_id = {decomp_step_id}")
    outlier_decomp_step_id = db_connect.query(call, db_connection)['o'][0]
    return outlier_decomp_step_id


def get_outlier_decomp_step_id(decomp_step, decomp_step_id,
                               iterative_data_decomp_step_id, db_connection):
    """
    Get the outlier decomp step ID to use for *this* model. May return
    the same decomp step (e.g. for step 2).

    :param decomp_step:
    :param decomp_step_id:
    :param iterative_data_decomp_step_id:
    :param db_connection:
    :return:
    """
    if decomp_step == "iterative":
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
                                 decomp_step_id,
                                 outlier_decomp_step_id,
                                 gbd_round_id, cause_id,
                                 sex_id,
                                 model_version_type, db_connection):
    """
    Get the outlier model version to use. If we're pulling the same
    outlier decomp step ID as the decomp step ID, then we just want to make
    the outlier model version the same as the model version. Same if it's a new cause.

    :param model_version_id: (int)
    :param decomp_step_id: (int)
    :param outlier_decomp_step_id: (int)
    :param gbd_round_id: (int)
    :param cause_id: (int)
    :param sex_id: (int)
    :param model_version_type: (int)
    :param db_connection: (str)
    :return:
    """
    logger.info(f"Getting outlier model version ID for {model_version_id}.")
    new_cause = check_if_new_cause_for_step_3(cause_id=cause_id,
                                              gbd_round_id=gbd_round_id,
                                              db_connection=db_connection)
    if (outlier_decomp_step_id == decomp_step_id) or (new_cause and decomp_step_id == 3) or decomp_step_id == 7:
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
    call = f"SELECT * FROM cod.model_version WHERE model_version_id = {model_version_id}"
    df = db_connect.query(call.format(model_version_id), db_connection)
    model = {k: df[k][0] for k in df.columns}
    model["start_year"] = 1980
    model["all_data_holdout"] = 'ko%02d' % (model["holdout_number"] + 1)
    model["db_connection"] = db_connection

    cause_id = model["cause_id"]
    gbd_round_id = model["gbd_round_id"]
    sex_id = model["sex_id"]
    model_version_type_id = model["model_version_type_id"]

    call = f"SELECT acause, cause_name FROM shared.cause WHERE cause_id = {cause_id}"
    acause = db_connect.query(call, db_connection)["acause"][0]
    cause_name = db_connect.query(call, db_connection)["cause_name"][0]
    model["acause"] = acause
    model["cause_name"] = cause_name

    model["modeler"] = get_modeler(cause_id=cause_id,
                                   gbd_round_id=gbd_round_id,
                                   db_connection=db_connection)

    call = f"SELECT sex FROM shared.sex WHERE sex_id = {sex_id}"
    sex = db_connect.query(call, db_connection)["sex"][0]
    model["sex"] = sex.lower()

    call = f"SELECT model_version_type FROM cod.model_version_type " \
        f"WHERE model_version_type_id = {model_version_type_id}"
    model_version_type = db_connect.query(call, db_connection)["model_version_type"][0]
    model["model_version_type"] = model_version_type

    call = f"SELECT gbd_round FROM shared.gbd_round WHERE gbd_round_id = {gbd_round_id}"
    gbd_round = int(db_connect.query(call, db_connection)["gbd_round"][0])
    model["gbd_round"] = gbd_round
    model["decomp_step"] = decomp_step_from_decomp_step_id(model["decomp_step_id"])

    model["pop_run_id"] = get_run_id(model["population_proc_version_id"])
    model["env_run_id"] = get_run_id(model["envelope_proc_version_id"])

    model["psi_values"] = np.arange(model["psi_weight_min"], model["psi_weight_max"] +
                                    model["psi_weight_int"], model["psi_weight_int"])

    model["deaths_measure_id"] = 1
    model["keep_vars"] = ["super_region", "region", "country_id", "location_id",
                          "age", "year", "cf", "envelope", "pop"]

    model['draw_cols'] = ["draw_%d" % i for i in range(0, 1000)] + \
                         ["envelope", "pop", "age", "sex", "year", "location_id",
                          "country_id", "region", "super_region"]

    model['upload_cols'] = ["year_id", "location_id", "sex_id", "age_group_id",
                            "mean_cf", "lower_cf", "upper_cf"]

    model['all_age_group_id'] = 22
    model['global_location_id'] = 1

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
        decomp_step_id=model["decomp_step_id"],
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

    model["standard_location_set_version_id"] = get_location_metadata(
        location_set_id=101,
        gbd_round_id=model['gbd_round_id']
    )['location_set_version_id'][0]

    call = f"""UPDATE cod.model_version SET status = 0
               WHERE model_version_id = {model_version_id}"""

    if update:
        db_connect.query(call.format(model_version_id), db_connection)
    return model
