import logging
import os
import shutil
from datetime import datetime
from typing import List

import pandas as pd

import gbd.constants as gbd
from db_queries import get_restrictions
from gbd.decomp_step import decomp_step_from_decomp_step_id
from hybridizer.joblaunch.HybridTask import HybridTask

from codem.devQueries.best_query import get_cause_ids, get_feeders
from codem.joblaunch.CODEmTask import CODEmBaseTask, CODEmTask
from codem.joblaunch.CODEmTriple import CODEmTriple
from codem.joblaunch.CODEmWorkflow import CODEmWorkflow
from codem.joblaunch.run_utils import change_model_status, list_check, new_models
from codem.reference import db_connect, paths

logger = logging.getLogger(__name__)


def check_completed_cov_selection(model_version_id, db_connection):
    model_dir = paths.get_base_dir(model_version_id, db_connection=db_connection)
    return os.path.exists(os.path.join(model_dir, 'cv_selected.txt'))


def check_uploaded_submodels(model_version_id, db_connection):
    call = f"SELECT submodel_version_id FROM cod.submodel_version WHERE model_version_id = {model_version_id}"
    ids = db_connect.query(call, db_connection)
    return not ids.empty


def get_hybrids_to_launch(central_run, decomp_step_id, gbd_round_id, sex_id,
                          db_connection='ADDRESS'):
    """
    Get all hybrids that need to be run

    Args:
        central_run (int): central run number
        decomp_step_id (int): ID of decomp step
        gbd_round_id (int): ID of GBD round
        sex_id (int): ID of sex
        db_connection (str): database connection to pull failed hybrids

    Returns:
        dataframe of hybrid models to launch
    """
    call = f'''
            SELECT MAX(cmv.model_version_id) AS model_version_id, cmv.cause_id, cmv.sex_id,
                cmv.model_version_type_id, cmv.age_start, cmv.age_end
            FROM cod.model_version cmv
            WHERE cmv.model_version_id NOT IN (
              SELECT cmr.child_id FROM cod.model_version_relation cmr
              INNER JOIN cod.model_version cmvv
              ON cmvv.model_version_id = cmr.parent_id
              WHERE cmvv.status = 1
            )
            AND cmv.description LIKE "central codem run {central_run}%%"
            AND cmv.gbd_round_id = {gbd_round_id}
            AND cmv.decomp_step_id = {decomp_step_id}
            AND cmv.status = 1
            AND cmv.sex_id = {sex_id}
            AND model_version_type_id IN (1, 2)
            GROUP BY cmv.cause_id, cmv.sex_id, cmv.model_version_type_id, cmv.age_start, cmv.age_end
           '''
    df = db_connect.query(call, db_connection)
    if df.empty:
        return pd.DataFrame()
    df = df.pivot_table(index=['cause_id', 'sex_id', 'age_start', 'age_end'],
                        columns=['model_version_type_id'],
                        values=['model_version_id'])
    df.columns = df.columns.droplevel()
    df.reset_index(inplace=True)
    df.columns = ['cause_id', 'sex_id', 'age_start', 'age_end', 'global', 'data_rich']
    valid_df = df.loc[(~df['global'].isnull()) & (~df['data_rich'].isnull())]
    return valid_df


def launch_hybrids(central_run, decomp_step_id, gbd_round_id,
                   db_connection="ADDRESS", conn_def='codem', user='codem'):
    """
    Relaunches the hybrid models for a central run that failed

    Args:
        central_run (int): central run number
        decomp_step_id (int): ID of decomp step
        gbd_round_id (int): ID of GBD round
        db_connection (str): database connection to pull failed hybrids
        conn_def (str): database connection to submit hybrids
        user (str): user

    Returns:
        None
    """
    hybrids = pd.concat(
        [get_hybrids_to_launch(
            central_run=central_run,
            decomp_step_id=decomp_step_id,
            sex_id=sex_id,
            gbd_round_id=gbd_round_id,
            db_connection=db_connection) for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]])
    if hybrids.empty:
        raise RuntimeError("No hybrids found to launch!")
    tasks = []
    try:
        for _, row in hybrids.iterrows():
            tasks.append(HybridTask(
                global_model_version_id=int(row['global']),
                datarich_model_version_id=int(row['data_rich']),
                conn_def=conn_def,
                user=user)
            )
        wf = CODEmWorkflow(
            name=f'hybrid_launch_{datetime.today().strftime("%Y%d%m_%H%M%S")}',
            description=f'hybrid_launch_{datetime.today().strftime("%Y%d%m_%H%M%S")}',
            reset_running_jobs=True,
            resume=True
        )
        wf.add_tasks(tasks)
        exit_status = wf.run()
        if exit_status:
            logger.info(f"The workflow failed, returning exit status {exit_status}, see workflow logs {wf.stderr}.")
        else:
            logger.info("The workflow successfully completed!")
            shutil.rmtree(wf.stderr)
    except:
        logger.info("Launching hybrids has failed. Deleting all the hybrids!")
        for t in tasks:
            change_model_status(t.model_version_id, 3, db_connection)


def batch_relaunch(model_version_id, decomp_step_id, description='', wf_desc='',
                   db_connection='ADDRESS', use_covariates=True,
                   gbd_round_id=gbd.GBD_ROUND_ID,
                   gb_padding=20, min_padding=60, num_cores=20,
                   create_new_models=False):
    logger.info("Batch relaunching.")
    model_versions = list_check(model_version_id)
    wf = CODEmWorkflow(name=f'failed_relaunch' + wf_desc, description=wf_desc)
    tasks = []
    for m in model_versions:
        uploaded = check_uploaded_submodels(m, db_connection)
        if uploaded:
            logger.info(f"This model {m} has uploaded submodel info already. "
                        f"Need a fresh model version.")
        if create_new_models or uploaded:
            logger.info(f"Creating a new model version for {m}.")
            desc = description + f", relaunch of failed model {m}"
            completed_covariate_selection = check_completed_cov_selection(
                m, db_connection
            )
            skip_covariate_selection = completed_covariate_selection and use_covariates
            run_covariate_selection = not skip_covariate_selection
            if run_covariate_selection:
                logger.info("Will be running covariate selection for this new model.")
            else:
                logger.info("This new model will go not through covariate selection.")
                desc = desc + ' using its already completed covariates'
            new_m = new_models(list_of_models=[m], db_connection=db_connection, gbd_round_id=gbd_round_id,
                               decomp_step_id=decomp_step_id, desc=desc,
                               run_covariate_selection=run_covariate_selection)[0]
            logger.info(f"Change model status for {m} to failed.")
            change_model_status(m, status=6, db_connection=db_connection)
        else:
            logger.info(f"DO NOT NEED A NEW MODEL, REUSING {m}")
            new_m = m
        tasks.append(CODEmTask(model_version_id=new_m, gbd_round_id=gbd_round_id, db_connection=db_connection,
                               cores=num_cores, gb_padding=gb_padding, min_padding=min_padding))
    wf.add_tasks(tasks)
    return wf


def check_model_attribute(model_version_id,
                          model_attribute_name,
                          model_attribute,
                          db_connection='ADDRESS') -> None:
    """
    Checks that the specific model version is truly associated with the model
    attribute that is specified.

    Args:
        model_version_id (int): ID of a given model version
        model_attribute_name (str): title of model attribute
        model_attribute (int): value of model attribute
        db_connection (str): database connection

    Returns:
        None
    """
    call = '''
        SELECT {} FROM cod.model_version WHERE model_version_id = {}
        '''.format(model_attribute_name, model_version_id)
    if not db_connect.query(call, db_connection)[model_attribute_name].iloc[0] == model_attribute:
        raise ValueError(
            'The model attribute for {} in model_version {} does not match up!'.
            format(model_attribute_name, model_version_id)
        )


def check_sex_restrictions(cause_id,
                           sex_id,
                           decomp_step_id,
                           gbd_round_id) -> bool:
    """
    Checks a cause for sex-restriction. Calls get_restrictions() from db_queries

    Args:
        cause_id (int): ID of a given cause
        sex_id (int): ID of the sex
        decomp_step_id (int): ID of the decomp step
        gbd_round_id (int): ID of the GBD round

    Returns:
        Boolean
    """
    decomp_step=decomp_step_from_decomp_step_id(decomp_step_id)
    cause_set_id = 15 if decomp_step == gbd.decomp_step.USA_HEALTH_DISPARITIES else 2
    cause_restrictions = get_restrictions(
        "sex",
        cause_id=cause_id,
        gbd_round_id=gbd_round_id,
        cause_set_id=cause_set_id,
        decomp_step=decomp_step
    )

    if cause_restrictions[cause_restrictions['sex_id'] == sex_id].empty:
        # If get_restrictions() returns an empty dataframe, the cause is not
        # sex restricted
        return True
    else:
        return False


def demographic_launch(acause,
                       gbd_round_id,
                       decomp_step_id,
                       age_start=None,
                       age_end=None,
                       sex_id=None,
                       description='null',
                       db_connection='ADDRESS',
                       pre_gbd_round_id=None,
                       pre_decomp_step_id=None,
                       sleep_time=0) -> List[CODEmBaseTask]:
    """
    For each cause in a list of given causes, creates global, data-rich, and
    hybrid models for each sex for each each cause, and then returns CODEmTasks
    to run each of these models

    Args:
        acause (List[str]): list of acauses to create models for
        gbd_round_id (int): ID of GBD round
        decomp_step_id (int): ID of decomp step
        age_start (int): starting age group ID of age range for models
        age_end (int): ending age group ID of age range for models
        sex_id (int): ID of sex
        description (str): description of new models
        db_connection (str): database connection
        pre_decomp_step_id (int): Decomp step ID to pull best models from
        pre_gbd_round_id (int): GBD round ID to pull best models from
        sleep_time (int): time to sleep before CODEmTasks run

    Returns:
        list of CODEmTasks and HybridTasks
    """
    logger.info("Demographic launch for {}".format(acause))
    sex_id = list_check(sex_id)
    acause = list_check(acause)
    if pre_gbd_round_id is None:
        pre_gbd_round_id = gbd_round_id
    if pre_decomp_step_id is None:
        pre_decomp_step_id = decomp_step_id
    if age_start is None and age_end is None:
        age_range = None
    elif age_start is not None and age_end is not None:
        age_range = [age_start, age_end]
    else:
        raise ValueError("Need to pass either both valid age start / ends, or "
                         "both as None.")
    tasks = []
    for cause in acause:
        logger.info(f"Getting models for {cause}")
        if len(get_cause_ids(cause)) > 1:
            raise RuntimeError("There is more than 1 cause ID for this "
                               "acause! Fix!")
        cause_id = get_cause_ids(cause)[0]
        for s in sex_id:
            global_model = get_feeders(
                gbd_round_id=pre_gbd_round_id,
                decomp_step_id=pre_decomp_step_id,
                cause_id=cause_id,
                age_range=age_range,
                sex_id=s,
                model_version_type_id=1,
                db_connection=db_connection
            )['child_id'].iloc[0]
            datarich_model = get_feeders(
                gbd_round_id=pre_gbd_round_id,
                decomp_step_id=pre_decomp_step_id,
                cause_id=cause_id,
                age_range=age_range,
                sex_id=s,
                model_version_type_id=2,
                db_connection=db_connection
            )['child_id'].iloc[0]
            task_list = CODEmTriple(
                datarich_model_version_id=datarich_model,
                global_model_version_id=global_model,
                description=description,
                db_connection=db_connection,
                gbd_round_id=gbd_round_id,
                decomp_step_id=decomp_step_id,
                sleep_time=sleep_time
            ).task_list()
            tasks.extend(task_list)
    return tasks


def version_launch(global_model_version_id,
                   datarich_model_version_id,
                   description,
                   gbd_round_id,
                   decomp_step_id,
                   db_connection='ADDRESS',
                   sleep_time=0) -> List[CODEmBaseTask]:
    """
    Given a global and data-rich model version ID, launch new models for each,
    and their hybrid model, from a CODEmTriple

    Args:
        global_model_version_id: ID of the global model version
        datarich_model_version_id: ID of the data-rich model version
        description (str): description of new model versions to create
        gbd_round_id (int): ID of GBD round
        decomp_step_id (int): ID of decomp step
        db_connection (str): database connection
        sleep_time (int): time to sleep before CODEmTasks run

    Returns:
        a list of two CODEmTasks and a HybridTask
    """
    logger.info(
        f"Version launch for {global_model_version_id} and "
        f"{datarich_model_version_id}"
    )
    task_list = CODEmTriple(
        datarich_model_version_id=datarich_model_version_id,
        global_model_version_id=global_model_version_id,
        db_connection=db_connection,
        description=description,
        gbd_round_id=gbd_round_id,
        decomp_step_id=decomp_step_id,
        sleep_time=sleep_time
    ).task_list()
    return task_list


def individual_launch(model_version_id,
                      description,
                      gbd_round_id,
                      decomp_step_id,
                      db_connection) -> List[CODEmTask]:
    """
    Inserts a new model version ID into a database at db_connection, creates
    a CODEmTask from it, and returns it inside of a List
    Args:
        model_version_id (List[int]): ID of model version to make a new model
            version from, as a single int in a list
        description (str): description of new model version to create
        db_connection (str): database connection
        gbd_round_id (int): ID of the GBD round
        decomp_step_id (int): ID of the decomp_step

    Returns:
        List containing one CODEmTask
    """
    model_versions = new_models(model_version_id,
                                db_connection=db_connection,
                                desc=description,
                                gbd_round_id=gbd_round_id,
                                decomp_step_id=decomp_step_id)
    task_list = [CODEmTask(model_version_id=m,
                           db_connection=db_connection) for m in model_versions]
    return task_list

