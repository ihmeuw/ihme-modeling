import getpass
import json
import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import orm

import db_tools_core
from db_queries import get_best_model_versions
from db_queries.api.internal import (
    get_active_cause_set_version,
    get_active_location_set_version,
    get_mortality_run_id,
)
from gbd import conn_defs
from gbd import constants as gbd_constants
from gbd.release import get_gbd_round_id_from_release
from jobmon.client import api as jobmon

import codem
import codem.reference.db_connect as db_connect
from codem.data import queryStrings as QS
from codem.data.shared import get_ages_in_range
from codem.data.utilities import list_check

logger = logging.getLogger(__name__)

COVARIATE_REFERENCE_DICT = {
    "level": 1,
    "direction": 0,
    "reference": None,
    "site_specific": None,
    "lag": None,
    "offset": None,
    "transform_type_id": 0,
    "p_value": 0.05,
    "selected": None,
}

# Columns in cod.model_version needed to propogate forward to
# new re-run model version.
_PREVIOUS_MODEL_VERSION_COLS = [
    "model_version_id",
    "release_id",
    "cause_id",
    "sex_id",
    "age_start",
    "age_end",
    "model_version_type_id",
    "description",
    "environ",
    "holdout_number",
    "holdout_ensemble_prop",
    "holdout_submodel_prop",
    "linear_floor_rate",
    "submodel_metric_id",
    "spacetime_only",
    "time_weight_method",
    "lambda_time_smooth",
    "lambda_time_smooth_nodata",
    "zeta_space_smooth",
    "zeta_space_smooth_nodata",
    "omega_age_smooth",
    "gpr_year_corr",
    "psi_weight_max",
    "psi_weight_min",
    "psi_weight_int",
    "rmse_window",
    "use_trusted_data",
    "banned_cov_pairs",
    "amplitude_scalar",
]


def get_logged_branch(
    model_version_id: int, conn_def: str, session: Optional[orm.Session] = None
) -> str:
    """
    Get the logged branch from cod.model_version.code_version in the database.

    Arguments:
        model_version_id: model version ID
        conn_def: connection definition of database to connect to
        session: optional session to use for connection instead of conn_def

    :return:
    """
    with db_tools_core.session_scope(conn_def=conn_def, session=session) as scoped_session:
        logged_version = scoped_session.execute(
            """
            SELECT code_version
            FROM cod.model_version
            WHERE model_version_id = :model_version_id
            """,
            params={"model_version_id": model_version_id},
        ).scalar()
    if logged_version is None:
        branch = None
    else:
        branch = json.loads(logged_version)["branch"]
    return branch


def update_code_version(model_version_id: int, conn_def: str) -> None:
    """
    Update branch and version of CODEm model is running on in cod.model_version.code_version.

    :param model_version_id: (int) model version ID
    :param conn_def: (str) database connection
    :return:
    """
    logged_branch = get_logged_branch(model_version_id=model_version_id, conn_def=conn_def)
    current_version = json.dumps({"branch": logged_branch, "tag": codem.__version__})
    logger.info(f"This model is running with code version {current_version}.")
    with db_tools_core.session_scope(conn_def=conn_def) as session:
        session.execute(
            """
            UPDATE cod.model_version
            SET code_version = :code_version
            WHERE model_version_id = :model_version_id
            """,
            params={"model_version_id": model_version_id, "code_version": current_version},
        )


def change_model_status(model_version_id: int, status: int, conn_def: str) -> None:
    if model_version_id is None:
        raise ValueError("Cannot pass a model version ID of None.")
    with db_tools_core.session_scope(conn_def=conn_def) as session:
        session.execute(
            """
            UPDATE cod.model_version
            SET status = :status, last_updated = NOW()
            WHERE model_version_id = :model_version_id
            """,
            params={"status": status, "model_version_id": model_version_id},
        )


def get_covariate_id(model_version_id: int) -> int:
    """Given a covariate model version ID find the covariate ID."""
    logger.info("Getting covariate ID for model version {}".format(model_version_id))
    with db_tools_core.session_scope(conn_def=conn_defs.COVARIATES) as session:
        covariate_id = session.execute(
            """
            SELECT covariate_id
            FROM covariate.model_version
            WHERE model_version_id = :model_version_id
            """,
            params={"model_version_id": model_version_id},
        ).scalar()
    return covariate_id


def add_covariates(
    df: pd.DataFrame, add_covs: Dict[int, Optional[Dict[str, int]]], release_id: int
) -> pd.DataFrame:
    """
    Adds covariates with parameters specified in add_covs to the
    covariate df. If a covariate ID from add_covs already exists in the df,
    will prioritize the add_covs instead. Returns a covariate df
    with the added covariates and parameters (or default if left unspecified).

    Arguments:
        df: existing covariate df
        add_covs: dictionary with a key for each additional covariate ID to add. If you want to
            over-ride the covariate parameters listed in the COVARIATE_REFERENCE_DICT global
            variable, pass them as dictionary items keyed to the covariate ID key.
            Drop duplicates at the end so that we won't have duplicated covariates in the
            model. Examples: add_covs={1099, 1001}
            add_cov={1099: {'direction': 1, 'level': 2}, 1001}
        release_id: GBD release ID
    Returns:
        new_df with rows for each covariate passed in add_covs
    """
    df = df.loc[~df.covariate_id.isin(add_covs)]
    cov_dict = dict.fromkeys(add_covs, {})
    for c in add_covs:
        for k in COVARIATE_REFERENCE_DICT:
            if k in add_covs[c]:
                cov_dict[c][k] = [add_covs[c][k]]
            else:
                cov_dict[c][k] = [COVARIATE_REFERENCE_DICT[k]]
        cov_df = pd.DataFrame.from_dict(cov_dict[c], orient="columns")
        cov_df["covariate_id"] = c
        cov_df["covariate_model_version_id"] = get_best_model_versions(
            "covariate", ids=c, release_id=release_id
        )["model_version_id"].iat[0]
        df = pd.concat([df, cov_df], axis=0, sort=True)
    return df


def get_old_covariates(
    old_model_version_id: int,
    conn_def: str,
    release_id: int,
    delete_covs: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Get existing covariate data frame for old model version.

    Arguments:
        old_model_version_id: model version ID to pull covariates from
        conn_def: database connection definition
        release_id: GBD release ID
        delete_covs: whether to include deleted covariates
    Returns:
    """
    df = db_connect.execute_select(
        """SELECT * FROM cod.model_covariate WHERE model_version_id = :model_version_id""",
        conn_def=conn_def,
        parameters={"model_version_id": old_model_version_id},
    )
    df["covariate_id"] = df.covariate_model_version_id.map(lambda x: get_covariate_id(x))
    if delete_covs is not None:
        df = df.loc[~df.covariate_id.isin(delete_covs)]
    df.drop(columns=["covariate_model_version_id"], inplace=True)
    covariate_best_model_versions = get_best_model_versions(
        "covariate", ids=df.covariate_id.tolist(), release_id=release_id
    )
    df = df.merge(
        covariate_best_model_versions[["covariate_id", "model_version_id"]].rename(
            columns={"model_version_id": "covariate_model_version_id"}
        ),
        on="covariate_id",
        how="inner",
    )
    df.drop(
        [
            "model_covariate_id",
            "date_inserted",
            "inserted_by",
            "last_updated",
            "last_updated_by",
            "last_updated_action",
        ],
        axis=1,
        inplace=True,
    )
    return df


def new_covariate_df(
    old_model_version_id: int,
    new_model_version_id: int,
    conn_def: str,
    release_id: int,
    add_covs: Optional[Dict[int, Optional[Dict[str, int]]]] = None,
    delete_covs: Optional[bool] = None,
) -> pd.DataFrame:
    """
    (int, int) -> None

    Given two integers representing a previous model version ID used for CODEm
    and a new model, update the new model to use the newest version of the
    covariates by inputting those values in the production database.
    """
    df = get_old_covariates(
        old_model_version_id=old_model_version_id,
        conn_def=conn_def,
        release_id=release_id,
        delete_covs=delete_covs,
    )
    if add_covs is not None:
        df = add_covariates(df, add_covs, release_id)
    df["model_version_id"] = new_model_version_id
    df = df[np.isfinite(df["covariate_model_version_id"].tolist())]
    if len(df.covariate_id.unique()) < len(df):
        raise RuntimeError("There are duplicate covariate rows. Can't upload!")
    df.drop("covariate_id", inplace=True, axis=1)
    return df


def new_covariates(
    old_model_version_id: int,
    new_model_version_id: int,
    conn_def: str,
    release_id: int,
    add_covs: Optional[Dict[int, Optional[Dict[str, int]]]] = None,
    delete_covs: Optional[bool] = None,
):
    """
    Gets the new covariate data frame and uploads to cod.model_covariate.

    Arguments:
        old_model_version_id: Old model version ID to pull covariates from
        new_model_version_id: New model version ID to upload under
        conn_def: Database connection definition
        release_id: GBD release ID
        add_covs: dict of covariate params/IDs to be added
        delete_covs: list of covariate IDs to delete
    """
    df = new_covariate_df(
        old_model_version_id=old_model_version_id,
        new_model_version_id=new_model_version_id,
        conn_def=conn_def,
        release_id=release_id,
        add_covs=add_covs,
        delete_covs=delete_covs,
    )
    db_connect.write_df_to_sql(df, table="model_covariate", conn_def=conn_def)


def set_new_covariates(
    models: List[int],
    conn_def: str,
    release_id: int,
    additional_covariates: Optional[List[int]] = None,
    delete_covariates: Optional[List[int]] = None,
) -> None:
    """
    Sets the covariates for all the new models using their prior selected covariates

    Arguments:
        models: list of models to add covariates for
        conn_def: database connection definition
        release_id: GBD release ID
        additional_covariates: additional covariates to add, and any non-default features
            that are needed
        delete_covariates: list of covariate IDs to delete from the model
    """
    logger.info("Setting new covariates.")
    df = db_connect.execute_select(
        """
        SELECT model_version_id, previous_model_version_id
        FROM cod.model_version
        WHERE model_version_id IN :models
        """,
        conn_def=conn_def,
        parameters={"models": models},
    )
    for i in range(df.shape[0]):
        new_covariates(
            df["previous_model_version_id"][i],
            df["model_version_id"][i],
            conn_def=conn_def,
            release_id=release_id,
            add_covs=additional_covariates,
            delete_covs=delete_covariates,
        )


def excluded_location_string(refresh_id: int, conn_def: str) -> str:
    with db_tools_core.session_scope(conn_def=conn_def) as session:
        locs_exclude = session.execute(
            """
            SELECT locations_exclude
            FROM cod.locations_exclude_by_refresh_id
            WHERE refresh_id = :refresh_id
            """,
            params={"refresh_id": refresh_id},
        ).scalar()
    return locs_exclude


def get_excluded_locations(df: pd.DataFrame, refresh_id: int, conn_def: str) -> List[int]:
    """
    Gets the locations to exclude based on the input model parameters. if
    the model_version_type_id is 2/11 (data rich or US R/E), the locations are
    pulled from an external source according to their data completeness ranking
    (3 stars or fewer)

    :param refresh_id: int
        the refresh ID to read for
    :param conn_def: str

    :return: pandas Series
        the list of locations to exclude
    """
    locs_to_exclude = excluded_location_string(refresh_id=refresh_id, conn_def=conn_def)
    for index, row in df.iterrows():
        if row["model_version_type_id"] in [2, 11]:
            df.at[(index, "locations_exclude")] = locs_to_exclude
        elif row["model_version_type_id"] == 1:
            df.at[(index, "locations_exclude")] = ""
        elif row["model_version_type_id"] == 0:
            pass
        else:
            raise ValueError("Model version type ID must be 0, 1, 2, or 11!")
    return df.locations_exclude


def get_refresh_id(release_id: int, conn_def: str) -> int:
    """
    Function to pull the best refresh ID for a given release.
    :param release_id: (int) GBD release ID
    :param conn_def: (str) database to connect to
    :return:
    """
    with db_tools_core.session_scope(conn_def=conn_def) as session:
        refresh_id = session.execute(
            """
            SELECT refresh_id
            FROM cod.release_refresh_version
            WHERE release_id = :release_id AND is_best = 1
            """,
            params={"release_id": release_id},
        ).scalar()
    return refresh_id


def get_mortality_process_version_id(process_id: int, release_id: int, conn_def: str) -> int:
    """Gets the mortality process version ID for a given process and GBD release.

    Arguments:
        process_id: process ID for envelope or population
        release_id: GBD release ID
        conn_def: database connection definition
    Returns:
        process version ID
    """
    with db_tools_core.session_scope(conn_def=conn_def) as scoped_session:
        run_id = get_mortality_run_id(
            process_id=process_id, release_id=release_id, session=scoped_session
        )
        process_version_id = scoped_session.execute(
            """
            SELECT proc_version_id
            FROM mortality.process_version
            WHERE process_id = :process_id AND run_id = :run_id
            """,
            params={"process_id": process_id, "run_id": run_id},
        ).scalar()
    return process_version_id


def get_linear_floor_rate(
    refresh_id: int,
    cause_id: int,
    sex_id: int,
    release_id: int,
    age_start: int,
    age_end: int,
    conn_def: str,
) -> int:
    with db_tools_core.session_scope(conn_def=conn_def) as session:
        linear_floor_rate = session.execute(
            """
            SELECT
                EXP(LN(MIN(floor))-2) as linear_floor_rate
            FROM
                cod.nonzero_floor_by_refresh_id
            WHERE
                refresh_id = :refresh_id AND
                cause_id = :cause_id AND
                age_group_id IN :age_group_ids AND
                sex_id = :sex_id
            """,
            params={
                "refresh_id": refresh_id,
                "cause_id": cause_id,
                "age_group_ids": get_ages_in_range(
                    age_start=age_start, age_end=age_end, release_id=release_id
                ),
                "sex_id": sex_id,
            },
        ).scalar()
    return linear_floor_rate


def set_rerun_models(
    list_of_models: List[int],
    release_id: int,
    conn_def: str,
    desc: Optional[str] = None,
    run_covariate_selection: int = 1,
    refresh_id: Optional[int] = None,
    custom_locs_exclude: Optional[List[int]] = None,
    attributes: Optional[Dict[str, int]] = None,
    mortality_conn_def: str = conn_defs.MORTALITY,
    use_new_desc: bool = True,
    default_linear_floor_rate: bool = True,
) -> List[int]:
    """
    Replicates the parameters for old models in new models.

    Args:
        list_of_models: List of old models to re-run
        release_id: GBD release ID
        conn_def: database connection definition
        desc: Description to save model with
        run_covariate_selection: Whether or not to run covariate selection
        refresh_id: ID of GBD refresh
        custom_locs_exclude: Locations to exclude
        attributes: Attributes to run model with
        mortality_conn_def: Connection definition of mortality DB
        use_new_desc: Whether to use a new
            description, if False, use old description  with datetime appended
        default_linear_floor_rate: Whether to use a default linear floor rate
    """
    logger.info("Setting the models to be re-run.")
    list_of_models = list_check(list_of_models)
    df = db_connect.execute_select(
        QS.getDatabaseParamsStr,
        conn_def=conn_def,
        parameters={"model_version_id": list_of_models},
    )[_PREVIOUS_MODEL_VERSION_COLS]
    if set(list_of_models) - set(df["model_version_id"]):
        raise ValueError(
            f"Model version ID(s) {set(list_of_models) - set(df['model_version_id'])} "
            "not found."
        )

    df[["inserted_by", "last_updated_by"]] = getpass.getuser()
    df[["status", "is_best"]] = 0
    df = df.rename(columns={"model_version_id": "previous_model_version_id"})

    if type(use_new_desc) != bool:
        raise TypeError(
            "use_new_desc parameter must be either True or False, "
            "given {use_new_desc}".format(use_new_desc=use_new_desc)
        )
    if not use_new_desc:
        descs = [str(x) + ", central relaunch of model" for x in df["description"]]
        df["description"] = descs
    else:
        df["description"] = desc

    location_set_id = 105 if release_id == gbd_constants.release.USRE else 35
    df["location_set_version_id"] = get_active_location_set_version(
        location_set_id, release_id=release_id
    )
    cause_set_id = 15 if release_id == gbd_constants.release.USRE else 2
    df["cause_set_version_id"] = get_active_cause_set_version(
        cause_set_id, release_id=release_id
    )

    # Update age groups for GBD 2021 if previous model is GBD 2019. Includes USRE release 15
    # * starts with post neonatal (age group id 4) -> 1-5 months (age group id 388)
    # * starts with 1-4 years (age group id 5) -> 12-23 months (age group id 238)
    # * ends with post neonatal (age group id 4) -> 6-11 months (age group id 389)
    # * ends with 1-4 years (age group id 5) -> 2-4 years (age group id 34)
    if release_id in [8, 9, 10, 11, 15]:
        df.loc[(df["age_start"] == 4) & (df["release_id"].isin([6, 7])), "age_start"] = 388
        df.loc[(df["age_start"] == 5) & (df["release_id"].isin([6, 7])), "age_start"] = 238
        df.loc[(df["age_end"] == 4) & (df["release_id"].isin([6, 7])), "age_end"] = 389
        df.loc[(df["age_end"] == 5) & (df["release_id"].isin([6, 7])), "age_end"] = 34

    df["gbd_round_id"] = get_gbd_round_id_from_release(release_id)
    df["release_id"] = release_id

    if refresh_id is None:
        df["refresh_id"] = get_refresh_id(release_id, conn_def=conn_def)
    else:
        if type(refresh_id) is int:
            df["refresh_id"] = refresh_id
        else:
            raise TypeError("Refresh ID must be integer!")

    if custom_locs_exclude is None:
        df["locations_exclude"] = get_excluded_locations(
            df, refresh_id=df["refresh_id"][0], conn_def=conn_def
        )
    else:
        if not all(df["model_version_type_id"] == 0):
            raise RuntimeError(
                "Cannot pass custom locations to any model verison type other than default."
            )
        df["locations_exclude"] = custom_locs_exclude

    df["run_covariate_selection"] = 1 if run_covariate_selection else 0

    # pull best envelope and population run IDs
    df["envelope_proc_version_id"] = get_mortality_process_version_id(
        process_id=26, release_id=release_id, conn_def=mortality_conn_def
    )
    df["population_proc_version_id"] = get_mortality_process_version_id(
        process_id=17, release_id=release_id, conn_def=mortality_conn_def
    )

    # Replace any model attributes with attributes dict
    if attributes is not None:
        for att in attributes:
            df[att] = attributes[att]

    if default_linear_floor_rate and df["gbd_round_id"].iat[0] > 6:
        df["linear_floor_rate"] = df.apply(
            lambda x: get_linear_floor_rate(
                refresh_id=x.refresh_id,
                cause_id=x.cause_id,
                sex_id=x.sex_id,
                release_id=release_id,
                age_start=x.age_start,
                age_end=x.age_end,
                conn_def=conn_def,
            ),
            axis=1,
        )

    # Set to main like CoDViz does
    df["code_version"] = json.dumps({"branch": "main"})

    logger.info("Uploading the new models to database.")
    models = db_connect.write_df_to_sql(
        df, table="model_version", conn_def=conn_def, return_key=True
    )
    return models


def new_models(
    list_of_models: List[int],
    conn_def: str,
    release_id: int,
    desc: Optional[str] = None,
    run_covariate_selection: int = 1,
    refresh_id: Optional[int] = None,
    add_covs: Optional[List[int]] = None,
    custom_locs_exclude: Optional[List[str]] = None,
    delete_covs: Optional[List[int]] = None,
    attributes: Optional[Dict[str, int]] = None,
    use_new_desc: bool = True,
    default_linear_floor_rate: bool = True,
) -> List[int]:
    """
    Uploads a set of new models to the database for them to be run at a later time by Jobmon.
    """
    list_of_models = list_check(list_of_models)
    models = set_rerun_models(
        list_of_models,
        release_id=release_id,
        conn_def=conn_def,
        desc=desc,
        run_covariate_selection=run_covariate_selection,
        refresh_id=refresh_id,
        custom_locs_exclude=custom_locs_exclude,
        attributes=attributes,
        use_new_desc=use_new_desc,
        default_linear_floor_rate=default_linear_floor_rate,
    )
    set_new_covariates(
        models,
        conn_def=conn_def,
        release_id=release_id,
        additional_covariates=add_covs,
        delete_covariates=delete_covs,
    )
    return models


def get_jobmon_tool():
    """Gets the jobmon tool for CODEm."""
    return jobmon.Tool(name="CODEm Modeling")
