import argparse
import logging
import os
from typing import List

import pandas as pd

import gbd.constants as gbd
from gbd.decomp_step import decomp_step_id_from_decomp_step

from codem.joblaunch.batch_launch import (
    check_model_attribute,
    check_sex_restrictions,
    demographic_launch,
    individual_launch,
    version_launch,
)
from codem.joblaunch.CODEmTask import CODEmBaseTask
from codem.joblaunch.CODEmWorkflow import CODEmWorkflow

logging.getLogger().setLevel(logging.INFO)

# We divide up the central run list of tasks into batches that will sleep before
# running after a set amount of time. BATCH_SIZE represents the number of causes
# to run at the same time together, BATCH_SIZE * 6 = # of WFs created at once
BATCH_SIZE = 8
# Time for batch to sleep in seconds, additive for subsequent waves
# (ie. index % BATCH_SIZE * SLEEP_INCREMENT) is the amount of time to sleep
SLEEP_INCREMENT = 10


def read_file(wave, branch, filepath) -> pd.DataFrame:
    """
    Reads from a filepath and returns a dataframe that has been subsetted down
    to only include causes for a particular wave
    Required Columns:
        cause_id, acause, age_start, age_end, wave
    Optional Columns:
        global_male, global_female, datarich_male, datarich_female
    All other columns will be ignored.

    Args:
        wave (int): number of the wave were currently launching models for
        branch (str): branch of the models to run on
        filepath (str): filepath to a CSV of cause-wave mappings

    Returns:
        Dataframe
    """
    df = pd.read_csv(filepath)
    df = df.loc[(df.wave == wave) & (df.branch == branch)]
    return df


def get_tasks(df, db_connection,
              gbd_round_id,
              decomp_step_id,
              description,
              pre_decomp_step_id=None,
              pre_gbd_round_id=None) -> List[CODEmBaseTask]:
    """
    Iterates over a dataframe's cause-wave mappings and creates a list of CODEm
    tasks for every CODEm triple that is present.

    Args:
        df (Dataframe): dataframe of cause-wave mappings
        db_connection (str): database connection
        gbd_round_id (int): ID of the GBD round
        decomp_step_id: ID of the decomp step
        description: description of new model versions
        pre_decomp_step_id (int): Decomp step ID to pull best models from
        pre_gbd_round_id (int): GBD round ID to pull best models from

    Returns:
        a list of CODEmTasks and HybridTasks
    """
    model_cols = ['global_male', 'global_female',
                  'datarich_male', 'datarich_female']
    for col in model_cols:
        if not col in df.columns:
            df[col] = None

    manual = df.loc[~df[model_cols].isnull().all(axis=1)]
    auto = df.loc[df[model_cols].isnull().all(axis=1)]
    auto = auto.reset_index(drop=True)

    logging.info("Beginning to make a list of CODEm tasks to run")
    task_list = []

    # sleep 0 seconds for the first batch of CODEm tasks added to workflow
    sleep_time = 0
    for index, row in auto.iterrows():
        logging.info("Creating CODEmTasks for models with unspecified best models")
        # increment sleep time if on next batch
        if not (index + 1) % BATCH_SIZE:
            sleep_time += SLEEP_INCREMENT
        for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
            # If a cause is sex-restricted, skip making a task for now
            if not check_sex_restrictions(row['cause_id'],
                                          sex_id,
                                          decomp_step_id=decomp_step_id,
                                          gbd_round_id=gbd_round_id):
                continue
            task_list.extend(
                demographic_launch(
                    acause=row['acause'],
                    gbd_round_id=gbd_round_id,
                    decomp_step_id=decomp_step_id,
                    age_start=row['age_start'],
                    age_end=row['age_end'],
                    sex_id=sex_id,
                    db_connection=db_connection,
                    pre_decomp_step_id=pre_decomp_step_id,
                    pre_gbd_round_id=pre_gbd_round_id,
                    description=description,
                    sleep_time=sleep_time
                )
            )

    for index, row in manual.iterrows():
        logging.warning("Creating CODEmTasks for models with specified best models")
        for sex_id in [gbd.sex.MALE, gbd.sex.FEMALE]:
            # If a cause is sex-restricted, skip making a task for now
            if not check_sex_restrictions(row['cause_id'],
                                          sex_id,
                                          decomp_step_id=decomp_step_id,
                                          gbd_round_id=gbd_round_id):
                continue
            if sex_id == gbd.sex.MALE:
                datarich_model_version_id = row['datarich_male']
                global_model_version_id = row['global_male']
            elif sex_id == gbd.sex.FEMALE:
                datarich_model_version_id = row['datarich_female']
                global_model_version_id = row['global_female']
            else:
                raise ValueError(f"Need to pass sex_id {gbd.sex.MALE} or {gbd.sex.FEMALE}!")
            for mv in [global_model_version_id, datarich_model_version_id]:
                check_model_attribute(mv, 'sex_id', sex_id)
                check_model_attribute(mv, 'age_start', row['age_start'])
                check_model_attribute(mv, 'age_end', row['age_end'])
                check_model_attribute(mv, 'cause_id', row['cause_id'])
            check_model_attribute(
                global_model_version_id, 'model_version_type_id', 1
            )
            check_model_attribute(
                datarich_model_version_id, 'model_version_type_id', 2
            )
            task_list.extend(
                version_launch(
                    global_model_version_id=global_model_version_id,
                    datarich_model_version_id=datarich_model_version_id,
                    description=description,
                    gbd_round_id=gbd_round_id,
                    decomp_step_id=decomp_step_id,
                    db_connection=db_connection,
                    sleep_time=sleep_time
                )
            )
    return task_list


def collect_tasks(wave, branch, wave_filepath,
                  db_connection, gbd_round_id,
                  decomp_step_id,
                  description,
                  addmodels_filepath=None,
                  pre_decomp_step_id=None,
                  pre_gbd_round_id=None) -> List[CODEmBaseTask]:
    """
    Calls get_tasks() and individual_launch() to create CODEm tasks for a given
    central run CSV and a CSV of additional models to launch, then returns them
    together as a list

    Args:
        wave (int): number of the wave for the given central run
        branch (str): branch of the models to run on
        wave_filepath (str): Filepath containig model versions with cause to
            wave mappings
        db_connection (str): database to connect to
        gbd_round_id (int): ID of the GBD round
        decomp_step_id (int): ID of the decomp step for given GBD round
        description (str): description of new model versions
        addmodels_filepath (str): filepath of additional model versions to
            launch
        pre_decomp_step_id (int): decomp step ID to pull best models from
        pre_gbd_round_id (int): GBD round ID to pull best models from

    Returns:
        List of CODEmTasks and HybridTasks
    """
    df = read_file(wave=wave, branch=branch, filepath=wave_filepath)
    tasks = get_tasks(df, db_connection=db_connection,
                      gbd_round_id=gbd_round_id,
                      decomp_step_id=decomp_step_id,
                      pre_gbd_round_id=pre_gbd_round_id,
                      pre_decomp_step_id=pre_decomp_step_id,
                      description=description)

    if addmodels_filepath is not None:
        add_models = pd.read_csv(addmodels_filepath)['model_version_id'].tolist()
        add_ons = individual_launch(add_models,
                                    description=description,
                                    gbd_round_id=gbd_round_id,
                                    decomp_step_id=decomp_step_id,
                                    db_connection=db_connection)
        tasks.extend(add_ons)

    return tasks


def central_run_workflow(wave, decomp_step, workflow_name=None,
                         workflow_description=None) -> CODEmWorkflow:
    """
    Creates and returns a CODEm Workflow, named after the given central run and
    wave
    """
    name = f'central_codem_run_{decomp_step}_wave{wave}'
    if workflow_name:
        name = f'central_codem_run_{decomp_step}_wave{wave}_{workflow_name}'
    description = f'CODEm central run for {decomp_step}, Wave {wave}'
    if workflow_description:
        description = f'{description}: {workflow_description}'
    wf = CODEmWorkflow(
        name=name,
        description=description,
        project=f'proj_codem_wave{wave}',
        resume=True,
        reset_running_jobs=True
    )
    return wf


def central_run_tasks(central_run, wave, branch, gbd_round_id, decomp_step,
                      wave_filepath, pre_decomp_step=None, pre_gbd_round_id=None,
                      addmodels_filepath=None) -> List[CODEmBaseTask]:
    """
    Main function for running the central run. Creates a list of tasks that
    can be added to a workflow. Arguments are read in from the
    command line using arg_parse()

    Args:
        central_run (int): number of the central run
        wave (int): number of the wave for the given central run
        branch (str): branch of the models to run on
        gbd_round_id (int): ID of the current GBD round
        decomp_step (str): decomp step for the given GBD round
        wave_filepath (str): filepath containing model versions with cause to
            wave mappings
        addmodels_filepath (str): filepath of additional model versions to launch
        pre_decomp_step_id (int): decomp step ID to pull best models from
        pre_gbd_round_id (int): GBD round ID to pull best models from

    Returns:
        List of CODEm and Hybrid Tasks
    """
    logging.basicConfig(level=logging.DEBUG)
    # env_branch is the branch of the current environment we're running in,
    # assertion is dependent on syntax: 'codem_{branch_name}'
    env_branch = os.environ['CONDA_DEFAULT_ENV']
    if f'codem_{branch}' != env_branch:
        raise RuntimeError(
            f"CODEm environment branch does not match branch of models to run, "
            f"was given branch {branch}, and is running in CODEm environment: "
            f"{env_branch}"
        )

    pre_gbd_round_id = (
        gbd_round_id if isinstance(pre_gbd_round_id, type(None))
        else pre_gbd_round_id
    )
    decomp_step_id = decomp_step_id_from_decomp_step(decomp_step, gbd_round_id)
    pre_decomp_step_id = (
        decomp_step_id if isinstance(pre_decomp_step, type(None))
        else decomp_step_id_from_decomp_step(pre_decomp_step, pre_gbd_round_id)
    )

    addmodels_filepath = (None if
                          isinstance(addmodels_filepath, type(None))
                          else str(addmodels_filepath))

    logging.info("Collecting central run CODEmTasks for workflow!")
    tasks = collect_tasks(
        wave=wave,
        branch=branch,
        wave_filepath=wave_filepath,
        addmodels_filepath=addmodels_filepath,
        description=f'central codem run {central_run}, wave {wave}',
        db_connection='ADDRESS',
        gbd_round_id=gbd_round_id,
        decomp_step_id=decomp_step_id,
        pre_gbd_round_id=pre_gbd_round_id,
        pre_decomp_step_id=pre_decomp_step_id,
    )
    return tasks

