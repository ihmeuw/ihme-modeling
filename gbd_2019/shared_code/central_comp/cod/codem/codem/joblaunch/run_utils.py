import datetime as date
import numpy as np
import pandas as pd
import os
import json
import subprocess

from db_queries.core.covariate import is_valid_covariate

import codem.reference.db_connect as db_connect
from codem.devQueries.utilities import list_check

import logging
logger = logging.getLogger(__name__)

ENVELOPE_PROCESS_ID = 26
POPULATION_PROCESS_ID = 17

COVARIATE_REFERENCE_DICT = {'level': 1,
                            'direction': 0,
                            'reference': '',
                            'site_specific': '',
                            'lag': None,
                            'offset': 0.0,
                            'transform_type_id': 0,
                            'p_value': 0.05,
                            'selected': None}


def get_acause(model_version_id, db_connection):
    call = f"SELECT sc.acause FROM cod.model_version cmv " \
        f"INNER JOIN shared.cause sc ON sc.cause_id = cmv.cause_id " \
        f"WHERE cmv.model_version_id = {model_version_id}"
    df = db_connect.query(call, db_connection)['acause'][0]
    return df


def get_logged_branch(model_version_id, db_connection):
    """
    Get the logged branch in the database.
    :param model_version_id:
    :param db_connection:
    :return:
    """
    call = f"SELECT code_version FROM cod.model_version WHERE model_version_id = {model_version_id}"
    logged_version = db_connect.query(call, connection=db_connection)['code_version'][0]
    if logged_version is None or logged_version == 'null':
        branch = None
    else:
        branch = json.loads(logged_version)['branch']
    return branch


def get_current_branch():
    """
    Get the current branch in the environment.
    """
    return os.environ['CONDA_DEFAULT_ENV'].replace("codem_", "")


def log_branch(model_version_id, db_connection, branch_name=None):
    """
    Logs which branch a model is running on. Only should log if it
    wasn't submitted via CodViz. If it already has a branch,
    check to make sure it's running in the right environment.
    :param model_version_id: (int) model version ID
    :param db_connection: (str) database connection
    :param branch_name: (str) optional branch name to use for testing
    :return:
    """
    if branch_name is None:
        branch_name = get_current_branch()
    logged_branch = get_logged_branch(model_version_id, db_connection)
    if logged_branch is None:
        current_version = '{"branch": "' + branch_name + '"}'
        update_call = f"UPDATE cod.model_version SET code_version = '{current_version}' " \
            f"WHERE model_version_id = {model_version_id}"
        db_connect.query(update_call,
                         connection=db_connection)
    else:
        logger.info(f"This model is running under {branch_name}.")
        if logged_branch != branch_name:
            raise RuntimeError(f"This model is running under {branch_name} but it should be"
                               f" running under {logged_branch} according to cod.model_version table.")
    return branch_name


def change_model_status(model_version_id, status,
                        db_connection):
    if model_version_id is None:
        raise ValueError("Cannot pass a model version ID of None.")
    call = """
            UPDATE cod.model_version SET status = {}
            WHERE model_version_id = {}
            """.format(status, model_version_id)
    db_connect.query(call, db_connection)


def get_model_dir(model_version_id, db_connection):
    """
    Grabs the model directory for saving intermediate steps,
    results, and configuring logger.
    """
    return 'FILEPATH'.format(get_acause(model_version_id, db_connection), model_version_id)


def setup_dir(model_version_id, db_connection):
    model_dir = get_model_dir(model_version_id, db_connection)
    if not os.path.exists(model_dir):
        os.makedirs(model_dir)
    subprocess.call('chmod 777 -R {}'.format('/'.join(model_dir.split('/'))), shell=True)
    return model_dir


def get_covariate_id(model_version_id):
    """Given a covariate model version ID find the covariate ID."""
    logger.info("Getting covariate ID for model version {}".format(model_version_id))
    call = '''
    SELECT DISTINCT
        sc.covariate_id, sc.covariate_name
    FROM
        covariate.model_version cmv
            INNER JOIN
        shared.covariate sc ON sc.covariate_id = cmv.covariate_id
    WHERE
        cmv.model_version_id = {mvid}
    '''.format(mvid=model_version_id)
    covariate_id = db_connect.query(call, 'ADDRESS')["covariate_id"][0]

    return covariate_id


def get_latest_covariate(covariate_id, gbd_round_id, decomp_step_id):
    """
    Given a covariate ID find the best covariate_model_version_id for that
    covariate.

    :param covariate_id: int
        covariate id to look up
    :param gbd_round_id: int
        gbd round id of the CURRENT model run
    :param decomp_step_id: int
        decomposition step ID
    :return: int
        new model version id
    """
    if not is_valid_covariate(covariate_id):
        new_model = np.nan
    elif covariate_id is not None:
        call = '''
            SELECT
                mv.model_version_id
            FROM
                covariate.model_version mv
            JOIN
                covariate.decomp_model_version dmv
                ON mv.model_version_id = dmv.model_version_id
            WHERE
                mv.covariate_id = {cid}
                AND mv.gbd_round_id = {gbd}
                AND dmv.decomp_step_id = {dsid}
                AND dmv.is_best = 1
            '''.format(cid=covariate_id, gbd=gbd_round_id, dsid=decomp_step_id)
        try:
            new_model = db_connect.query(call, 'ADDRESS')["model_version_id"][0]
        except IndexError:
            logger.info("There is NO best covariate for GBD round ID {} for covariate ID {}"
                        "and decomp step {}".format(gbd_round_id,
                                                    covariate_id, decomp_step_id))
            raise RuntimeError("Cannot run model because there is no best covariate ID {} for "
                               "GBD round ID {} and decomp step {}".format(covariate_id,
                                                                           gbd_round_id,
                                                                           decomp_step_id))
    else:
        new_model = np.nan
    return new_model


def current_location_set_id(gbd_round_id, db_connection):
    """
    Looks up the latest 2015 location set id for codem

    :return: int
        latest 2015 location set id
    """
    call = '''
    SELECT location_set_version_id FROM shared.location_set_version_active
    WHERE location_set_id = 35 # this location set id refres specifically to the hierarchy that codem uses
    AND gbd_round_id = {};
    '''.format(gbd_round_id)
    location_set_version_id = \
        db_connect.query(call, db_connection)["location_set_version_id"][0]
    return location_set_version_id


def current_cause_set_id(gbd_round_id, db_connection):
    """
    Looks up the latest 2015 cause set id for codem

    :return: int
        latest 2015 cause set id
    """
    call = '''
    SELECT cause_set_version_id FROM shared.cause_set_version_active
    WHERE cause_set_id = 4 # referes to the set of causes specific to codem
    AND gbd_round_id = {};
    '''.format(gbd_round_id)
    cause_set_version_id = \
        db_connect.query(call, db_connection)["cause_set_version_id"][0]
    return cause_set_version_id


def add_covariates(df, add_covs, gbd_round_id, decomp_step_id):
    """
    Adds covariates with parameters specified in add_covs to the
    covariate df. If a covariate ID from add_covs already exists in the df,
    will prioritize the add_covs instead. Returns a covariate df
    with the added covariates and parameters (or default if left unspecified).

    :param df: existing covariate df
    :param add_covs: dictionary with a key for each additional covariate ID
                     to add. If you want to over-ride the covariate parameters
                     listed in the COVARIATE_REFERENCE_DICT global variable,
                     pass them as dictionary items keyed to the covariate ID key.

                     Drop duplicates at the end so that we won't have duplicated
                     covariates in the model.

                     Examples:
                        add_covs={1099, 1001}
                        add_cov={1099: {'direction': 1, 'level': 2}, 1001}
    :param gbd_round_id: gbd round ID
    :param decomp_step_id: decomp step ID
    :return: new_df with rows for each covariate passed in add_covs
    """
    df = df.loc[~df.covariate_id.isin(add_covs)]
    cov_dict = dict.fromkeys(add_covs, {})
    for c in add_covs:
        for k in COVARIATE_REFERENCE_DICT:
            if k in add_covs[c]:
                cov_dict[c][k] = [add_covs[c][k]]
            else:
                cov_dict[c][k] = [COVARIATE_REFERENCE_DICT[k]]
        cov_df = pd.DataFrame.from_dict(cov_dict[c], orient='columns')
        cov_df['covariate_id'] = c
        cov_df['covariate_model_version_id'] = get_latest_covariate(c, gbd_round_id=gbd_round_id,
                                                                    decomp_step_id=decomp_step_id)
        df = pd.concat([df, cov_df], axis=0)
    return df


def get_old_covariates(old_model_version_id, db_connection,
                       gbd_round_id, decomp_step_id, delete_covs=None):
    """
    Get existing covariate data frame for old model version.
    :param old_model_version_id: int
    :param db_connection: str
    :param gbd_round_id: int
    :param decomp_step_id: int
    :return:
    """
    call = '''SELECT * FROM cod.model_covariate where model_version_id = {model}
           '''.format(model=old_model_version_id)
    df = db_connect.query(call, db_connection)
    df["covariate_id"] = df.covariate_model_version_id.map(lambda x: get_covariate_id(x))
    if delete_covs is not None:
        df = df.loc[~df.covariate_id.isin(delete_covs)]
    df["covariate_model_version_id"] = df.covariate_id.map(lambda x: get_latest_covariate(x, gbd_round_id,
                                                                                          decomp_step_id))
    df.drop(["model_covariate_id", "date_inserted", "inserted_by",
             "last_updated", "last_updated_by", "last_updated_action"], axis=1, inplace=True)
    return df


def new_covariate_df(old_model_version_id, new_model_version_id, db_connection, gbd_round_id,
                     decomp_step_id, add_covs=None, delete_covs=None):
    """
    (int, int) -> None

    Given two integers representing a previous model version ID used for CODEm
    and a new model, update the new model to use the newest version of the
    covariates by inputting those values in the production database.
    """
    df = get_old_covariates(old_model_version_id=old_model_version_id,
                            db_connection=db_connection,
                            gbd_round_id=gbd_round_id,
                            decomp_step_id=decomp_step_id,
                            delete_covs=delete_covs)
    if add_covs is not None:
        df = add_covariates(df, add_covs, gbd_round_id, decomp_step_id)
    df["model_version_id"] = new_model_version_id
    df = df[np.isfinite(df['covariate_model_version_id'])]
    if len(df.covariate_id.unique()) < len(df):
        raise RuntimeError("There are duplicate covariate rows. Can't upload!")
    df.drop('covariate_id', inplace=True, axis=1)
    return df


def new_covariates(old_model_version_id, new_model_version_id, db_connection, gbd_round_id,
                   decomp_step_id, add_covs=None, delete_covs=None):
    """
    Gets the new covariate data frame and uploads to cod.model_covariate.
    :param old_model_version_id: Old model version ID to pull covariates from
    :param new_model_version_id: New model version ID to upload under
    :param db_connection: Database connection
    :param gbd_round_id: gbd round ID
    :param decomp_step_id: decomp step ID
    :param add_covs: dict of covariate params/IDs to be added
    :param delete_covs: list of covaraiate IDs to delete
    """
    df = new_covariate_df(old_model_version_id=old_model_version_id,
                          new_model_version_id=new_model_version_id,
                          db_connection=db_connection, gbd_round_id=gbd_round_id,
                          decomp_step_id=decomp_step_id, add_covs=add_covs, delete_covs=delete_covs)
    db_connect.write_df_to_sql(df, db="cod", table="model_covariate",
                               connection=db_connection, creds=None)


def set_new_covariates(models, db_connection, gbd_round_id, decomp_step_id, additional_covariates=None, delete_covariates=None):
    """
    Sets the covariates for all the new models using their prior selected covariates

    :param models: list of int
        list of models to add covariates for
    :param db_connection: str
        db to connect to
    :param gbd_round_id: int
        gbd round ID
    :param decomp_step_id: int
        decomp step ID
    :param additional_covariates: dict
        dictionary of additional covariates to add, and any non-default features that they need.
    :param delete_covariates: list of ints
        list of covariate IDs to delete from the model
    """
    logger.info("Setting new covariates.")
    call = '''SELECT model_version_id, previous_model_version_id
              FROM cod.model_version
              WHERE model_version_id IN ({model_str});
              '''.format(model_str=', '.join([str(x) for x in models]))
    df = db_connect.query(call, db_connection)
    for i in range(df.shape[0]):
        new_covariates(df['previous_model_version_id'][i], df['model_version_id'][i], db_connection,
                       gbd_round_id=gbd_round_id, decomp_step_id=decomp_step_id,
                       add_covs=additional_covariates, delete_covs=delete_covariates)


def excluded_location_string(refresh_id, db_connection):
    call = f"SELECT locations_exclude FROM cod.locations_exclude_by_refresh_id WHERE refresh_id = {refresh_id}"
    locs_exclude = db_connect.query(call, db_connection)['locations_exclude'].iloc[0]
    return locs_exclude


def get_excluded_locations(df, refresh_id, db_connection):
    """
    gets the locations to exclude based on the input model parameters. if
    the model_version_type_id is 2 (data rich), the locations are pulled from
    an external source according to their data completeness ranking (3 stars or
    fewer)
    :param refresh_id: int
        the refresh ID to read for
    :param db_connection: str
    :return: pandas Series
        the list of locations to exclude
    """
    locs_to_exclude = excluded_location_string(refresh_id=refresh_id, db_connection=db_connection)
    for index, row in df.iterrows():
        if row['model_version_type_id'] == 2:
            df.at[(index, 'locations_exclude')] = locs_to_exclude
        elif row['model_version_type_id'] == 1:
            df.at[(index, 'locations_exclude')] = ''
        elif row['model_version_type_id'] == 0:
            pass
        else:
            raise ValueError("Model version type ID must be 0, 1 or 2!")
    return df.locations_exclude


def get_refresh_id(decomp_step_id, db_connection):
    """
    Function to pull the best refresh ID for a given decomp step.
    :param decomp_step_id: (int) decomp step ID
    :param db_connection: (str) database to connect to
    :return:
    """
    call = '''
           SELECT refresh_id FROM cod.decomp_refresh_version
           WHERE decomp_step_id = {} AND is_best = 1'''.format(decomp_step_id)
    refresh_id = db_connect.query(call, db_connection)['refresh_id'][0]
    return refresh_id


def get_best_process_version_id(decomp_step_id, process_id, gbd_round_id, db_connection):
    """
    Get the best process version from the mortality database.
    :param decomp_step_id: (int) decomp step
    :param process_id: (int) process ID for envelope or population
    :param gbd_round_id: (int) gbd round ID
    :param db_connection: (int) database connection
    :return: (int) process version ID
    """
    call = '''
            SELECT mdp.proc_version_id FROM mortality.decomp_process_version mdp
            INNER JOIN mortality.process_version mp
            ON mp.proc_version_id = mdp.proc_version_id
            WHERE mdp.decomp_step_id = {}
            AND mdp.is_best = 1
            AND mp.gbd_round_id = {}
            AND mp.process_id = {}
            '''.format(decomp_step_id, gbd_round_id, process_id)
    process_version_id = db_connect.query(call, db_connection)['proc_version_id'][0]
    return process_version_id


def set_rerun_models(list_of_models, gbd_round_id, decomp_step_id, db_connection, desc=None,
                     run_covariate_selection=True, refresh_id=None,
                     custom_locs_exclude=None, attributes=None,
                     mortality_db_connection='ADDRESS',
                     use_new_desc=True):
    """
    Replicates the parameters for old models in new models.

    Args:
        list_of_models (TYPE): List of old models to re-run
        gbd_round_id (TYPE): Description
        decomp_step_id (int): 1-5 decomposition step
        db_connection (TYPE): Description
        desc (None, optional): Description
        db_connection_upload (str, optional): Description
        run_covariate_selection (int, optional): Whether or not to
                                                        run covariate selection
        attributes: (dictionary) if you want to add attributes, pass as a dictionary
        use_new_desc (True, optional): Boolean specifying whether to use a new
            description, if False, use old description  with datetime appended

    Returns:
        TYPE: list of models
    """
    logger.info("Setting the models to be re-run.")
    list_of_models = list_check(list_of_models)
    models_string = "(" + ",".join([str(l) for l in set(list_of_models)]) + ")"
    drop_cols = ["model_version_id", "date_inserted", "inserted_by", "last_updated",
                 "last_updated_by", "last_updated_action"]
    null_cols = ["pv_rmse_in", "pv_rmse_out", "pv_coverage_in", "pv_coverage_out",
                 "pv_trend_in", "pv_trend_out", "pv_psi", "best_start", "best_end", ]
    call = '''
    SELECT *
    FROM
    cod.model_version
    WHERE
    model_version_id IN {ms}
    '''.format(ms=models_string)
    df = db_connect.query(call, db_connection)
    df['code_version'] = 'null'
    df[['status', 'is_best']] = 0
    df['previous_model_version_id'] = df['model_version_id']

    if type(use_new_desc) != bool:
        raise TypeError("use_new_desc parameter must be either True or False, "
                        "was given {use_new_desc}".format(use_new_desc=
                                                          use_new_desc))
    if not use_new_desc:
        descs = [str(x) + ", relaunch of model on {date}"
                          "".format(date=date.date.today())
                 for x in df['description']]
        df['description'] = descs
    else:
        df['description'] = desc

    df['location_set_version_id'] = current_location_set_id(gbd_round_id, db_connection)
    df['cause_set_version_id'] = current_cause_set_id(gbd_round_id, db_connection)
    df['gbd_round_id'] = gbd_round_id
    if refresh_id is None:
        df['refresh_id'] = get_refresh_id(decomp_step_id, db_connection)
    else:
        if type(refresh_id) is int:
            df['refresh_id'] = refresh_id
        else:
            raise TypeError("Refresh ID must be integer!")
    if custom_locs_exclude is None:
        df['locations_exclude'] = get_excluded_locations(df, refresh_id=df['refresh_id'][0],
                                                         db_connection=db_connection)
    else:
        if df['model_version_type_id'] != 0:
            raise RuntimeError("Cannot pass custom locations to any other model type than default.")
        df['locations_exclude'] = custom_locs_exclude
    if run_covariate_selection:
        df['run_covariate_selection'] = 1
    else:
        df['run_covariate_selection'] = 0
    df['decomp_step_id'] = decomp_step_id
    df['envelope_proc_version_id'] = get_best_process_version_id(decomp_step_id=decomp_step_id,
                                                                 process_id=ENVELOPE_PROCESS_ID,
                                                                 gbd_round_id=gbd_round_id,
                                                                 db_connection=mortality_db_connection)
    df['population_proc_version_id'] = get_best_process_version_id(decomp_step_id=decomp_step_id,
                                                                   process_id=POPULATION_PROCESS_ID,
                                                                   gbd_round_id=gbd_round_id,
                                                                   db_connection=mortality_db_connection)
    if attributes is not None:
        for att in attributes:
            df[att] = attributes[att]
    df[null_cols] = np.NaN
    df.drop(drop_cols, axis=1, inplace=True)
    logger.info("Uploading the new models to database.")
    models = db_connect.write_df_to_sql(df, db="cod", table="model_version",
                                        connection=db_connection,
                                        creds=None, return_key=True)
    return models


def new_models(list_of_models, gbd_round_id, decomp_step_id, db_connection, desc=None,
               run_covariate_selection=1, refresh_id=None, add_covs=None, custom_locs_exclude=None, delete_covs=None,
               attributes=None, use_new_desc=True):
    """
    Uploads a set of new models to the database for them to be run
    at a later time by Jobmon.
    """
    list_of_models = list_check(list_of_models)
    models = set_rerun_models(list_of_models, gbd_round_id, decomp_step_id, db_connection, desc=desc,
                              run_covariate_selection=run_covariate_selection,
                              refresh_id=refresh_id, custom_locs_exclude=custom_locs_exclude,
                              attributes=attributes, use_new_desc=use_new_desc,)
    set_new_covariates(models, db_connection, gbd_round_id, decomp_step_id, additional_covariates=add_covs, delete_covariates=delete_covs)
    return models
