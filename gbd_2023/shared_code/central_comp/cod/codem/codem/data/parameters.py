"""
Functions to query the model parameters for CODEm models.
Any new parameters that need to be queried should be put in get_model_parameters dictionary
"""

import logging
from typing import Any, Dict, List

import numpy as np
from sqlalchemy import orm

import db_tools_core
import gbd.constants as gbd
from db_queries.api.internal import get_active_location_set_version
from gbd import conn_defs

from codem.data import queryStrings as QS
from codem.data.utilities import age_id_to_name

logger = logging.getLogger(__name__)


def get_run_id(process_version_id: int, session: orm.Session) -> int:
    """
    Query the mortality database to get the run_id
    associated

    :param process_version_id: (int) process version ID for mortality DB
    :return: (int) run_id
    """
    call = (
        "SELECT run_id FROM mortality.process_version "
        "WHERE proc_version_id = :process_version_id"
    )
    run_id = db_tools_core.query_2_df(
        call, session=session, parameters={"process_version_id": process_version_id}
    )["run_id"][0]
    return run_id


def get_modeler(cause_id: int, gbd_round_id: int, session: orm.Session) -> List[str]:
    """
    Gets the modeler from the cod.modeler table that is associated with the
    given cause and GBD round.
    """
    call = """SELECT username
               FROM cod.modeler
               WHERE cause_id = :cause_id
               AND gbd_round_id = :gbd_round_id"""
    modeler = db_tools_core.query_2_df(
        call, session=session, parameters={"cause_id": cause_id, "gbd_round_id": gbd_round_id}
    )
    if modeler.empty:
        modeler = []
    else:
        modeler = modeler.iloc[0, 0].split(", ")
    return modeler


def get_cluster_project(cause_id: int, gbd_round_id: int, session: orm.Session) -> str:
    """
    Gets the cluster project from the cod.codem_cluster_project table that is
    associated with the given cause and GBD round
    """
    call = """SELECT cluster_project
               FROM cod.codem_cluster_project
               WHERE cause_id = :cause_id
               AND gbd_round_id = :gbd_round_id"""
    cluster_project = db_tools_core.query_2_df(
        call, session=session, parameters={"cause_id": cause_id, "gbd_round_id": gbd_round_id}
    )
    if cluster_project.empty:
        cluster_project = "proj_codem"
    else:
        cluster_project = cluster_project.cluster_project.iat[0]
    return cluster_project


def get_submodel_metric_parameters(
    submodel_metric_id: int, session: orm.Session
) -> List[int]:
    """
    Gets the metric id list from cod.submodel_metric that is associated
    with the given submodel metric ID.
    """
    call = """SELECT metric_id
               FROM cod.submodel_metric
               WHERE submodel_metric_id = :submodel_metric_id"""
    metric_id = db_tools_core.query_2_df(
        call, session=session, parameters={"submodel_metric_id": submodel_metric_id}
    ).iloc[0, 0]
    return [int(metric) for metric in metric_id.split(",")]


def get_model_parameters(
    model_version_id: int, conn_def: str, update: bool = False
) -> Dict[str, Any]:
    """
    integer -> dictionary

    Given an integer that indicates a valid model version id  the function will
    return a dictionary with keys indicating the model parameters start age,
    end age, sex, start year, cause, and whether to run covariate selection or
    not. "update" indicates whether during the querying process the database
    should be updated to running during the querying process, default is False.
    True should be used when running CODEm.
    """
    with db_tools_core.session_scope(
        conn_def=conn_def
    ) as scoped_cod_session, db_tools_core.session_scope(
        conn_def=conn_defs.MORTALITY
    ) as scoped_mortality_session:
        df = db_tools_core.query_2_df(
            QS.getDatabaseParamsStr,
            session=scoped_cod_session,
            parameters={"model_version_id": [model_version_id]},
        )
        model = {k: df[k][0] for k in df.columns}
        model["all_data_holdout"] = "ko%02d" % (model["holdout_number"] + 1)
        model["conn_def"] = conn_def
        model["start_year"] = 1980
        model["deaths_measure_id"] = 1
        model["all_age_group_id"] = gbd.age.ALL_AGES
        model["global_location_id"] = 1

        metric_id_list = get_submodel_metric_parameters(
            model["submodel_metric_id"], scoped_cod_session
        )
        model["include_counts"] = gbd.metrics.NUMBER in metric_id_list
        model["include_cf"] = gbd.metrics.PERCENT in metric_id_list
        model["include_rates"] = gbd.metrics.RATE in metric_id_list

        cause_id = model["cause_id"]
        call = """SELECT acause, cause_name FROM shared.cause
                WHERE cause_id = :cause_id"""
        df = db_tools_core.query_2_df(
            call, session=scoped_cod_session, parameters={"cause_id": cause_id}
        )
        acause, cause_name = df["acause"][0], df["cause_name"][0]
        model["acause"] = acause
        model["cause_name"] = cause_name

        model["age_start_name"] = age_id_to_name(
            model["age_start"], session=scoped_cod_session
        )
        model["age_end_name"] = age_id_to_name(model["age_end"], session=scoped_cod_session)

        sex_id = model["sex_id"]
        call = "SELECT sex FROM shared.sex WHERE sex_id = :sex_id"
        sex = db_tools_core.query_2_df(
            call, session=scoped_cod_session, parameters={"sex_id": sex_id}
        )["sex"][0]
        model["sex"] = sex.lower()
        model["standard_location_set_version_id"] = get_active_location_set_version(
            101, release_id=model["release_id"], session=scoped_cod_session
        )

        model["modeler"] = get_modeler(
            cause_id=cause_id, gbd_round_id=model["gbd_round_id"], session=scoped_cod_session
        )
        model["cluster_project"] = get_cluster_project(
            cause_id=cause_id, gbd_round_id=model["gbd_round_id"], session=scoped_cod_session
        )

        model_version_type_id = model["model_version_type_id"]
        call = """
                SELECT model_version_type FROM cod.model_version_type
                WHERE model_version_type_id = :model_version_type_id
                """
        model_version_type = db_tools_core.query_2_df(
            call,
            session=scoped_cod_session,
            parameters={"model_version_type_id": model_version_type_id},
        )["model_version_type"][0]
        model["model_version_type"] = model_version_type

        model["pop_run_id"] = get_run_id(
            model["population_proc_version_id"], session=scoped_mortality_session
        )
        model["env_run_id"] = get_run_id(
            model["envelope_proc_version_id"], session=scoped_mortality_session
        )
        model["with_hiv"] = _get_envelope_with_hiv(refresh_id=model["refresh_id"])
        model["psi_values"] = np.arange(
            model["psi_weight_min"],
            model["psi_weight_max"] + model["psi_weight_int"],
            model["psi_weight_int"],
        )

        model["keep_vars"] = [
            "level_1",
            "level_2",
            "level_3",
            "level_4",
            "location_id",
            "age_group_id",
            "year_id",
            "cf",
            "envelope",
            "population",
        ]
        model["draw_cols"] = ["draw_%d" % i for i in range(0, 1000)] + [
            "envelope",
            "population",
            "age_group_id",
            "sex_id",
            "year_id",
            "location_id",
            "level_1",
            "level_2",
            "level_3",
            "level_4",
        ]
        model["upload_cols"] = [
            "year_id",
            "location_id",
            "sex_id",
            "age_group_id",
            "mean_cf",
            "lower_cf",
            "upper_cf",
        ]
        if update:
            call = """
                    UPDATE cod.model_version
                    SET status = 0, last_updated = NOW()
                    WHERE model_version_id = :model_version_id
                    AND status IN (1, 2, 7)"""
            scoped_cod_session.execute(call, params={"model_version_id": model_version_id})

    return model


def _get_envelope_with_hiv(refresh_id) -> int:
    """Returns whether to include HIV in envelope."""
    _FIRST_HIV_REFRESH = 69
    with_hiv = 1 if refresh_id >= _FIRST_HIV_REFRESH else 0
    return with_hiv
