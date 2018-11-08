'''
Use Jobmon for CODEm Central Runs!
'''

import os
import sys
import logging
import pandas as pd
from datetime import datetime
import numpy as np
import sqlalchemy as sql
import math

from jobmon.workflow.task_dag import TaskDag
from jobmon.workflow.python_task import PythonTask
from jobmon.workflow.workflow import Workflow

sys.path.append("FILEPATH")
sys.path.append("FILEPATH")

import codem.db_connect as db_connect
from run_all_codem import set_rerun_models, set_new_covariates, \
    get_model_type, get_acause
from best_query import main as get_best_models


def rerun_models(model_list, gbd_round_id, db_connection,
                 description=None, new_mvid=False):
    '''
    Sets the models in the database + covariates and returns
    arguments to pass to the jobmon
    BashTask.

    :param model_list: list of model version IDs (list of int)
    :param gbd_round_id: (int) gbd_round_id
    :param db_connection: (str) name of database like
                                                    'modeling-cod-db'
    :param description: model description
    :return: returns a list of bash commands and a list of job names
    '''
    if new_mvid:
        print("Setting new models \n")
        model_list = set_rerun_models(model_list, gbd_round_id, db_connection,
                                      description)
        print("Setting new covariates \n")
        set_new_covariates(model_list, db_connection, gbd_round_id)

    name = 'cod_{model_version_id}_{type}_{acause}'

    arg_list = []
    name_list = []

    for model in model_list:
        print model
        acause = get_acause(model, db_connection)
        print acause
        model_type = get_model_type(model, db_connection)
        arg_list.append([model, db_connection, gbd_round_id])
        name_list.append(name.format(model_version_id=model,
                                     type=model_type, acause=acause))

    return arg_list, name_list


def init_jobmon(gbd_round_id, central_run, wave):
    '''
    Initiates a jobmon workflow for a given central run and wave.
    '''
    dag = TaskDag(name="gbd_round_{}_codem_run_{}_wave_{}".format(
        gbd_round_id, central_run, wave))
    return dag


def add_tasks(dag, model_list, slots, mem_free,
              gbd_round_id, db_connection, description=None, new_mvid=False):
    '''
    Adds task to a jobmon dag and set rerun models. One for each model, as a BashTask.
    '''
    arg_list, name_list = rerun_models(model_list,
                                       gbd_round_id, db_connection, description, new_mvid)

    tasks = [PythonTask(path_to_python_binary='FILEPATH',
                        name=nme, slots=slots, script='FILEPATH',
                        env_variables={'OMP_NUM_THREADS': 1,
                                       'MKL_NUM_THREADS': 1},
                        args=arg,
                        mem_free=mem_free, max_attempts=3) for arg, nme in zip(arg_list, name_list)]
    dag.add_tasks(tasks)


if __name__ == '__main__':

    wave_path = "FILEPATH"
    added_dr = None

    central_run, wave, gbd_round_id, slots = sys.argv[1:5]

    wave = int(wave)
    gbd_round_id = int(gbd_round_id)
    slots = int(slots)

    # initiate the jobmon dag
    dag = init_jobmon(gbd_round_id, central_run, wave)

    wave_file = pd.read_csv(wave_path)[['cause_id', 'age_start', 'age_end', 'wave', 'total_seconds', 'model_version_1',
                                                                            'model_version_2', 'model_version_3', 'model_version_4']]
    wave_file = wave_file.loc[wave_file.wave == wave]
    wave_file = wave_file.sort_values('total_seconds', ascending=False)
    for index, row in wave_file.iterrows():
        if not math.isnan(row['model_version_1']):
            for i in [1, 2, 3, 4]:
                model = int(row['model_version_{}'.format(i)])
                description = 'central codem run {}, wave {}, re-run model version {}'.format(
                    central_run, wave, model)
                add_tasks(dag, [model], slots, slots / 2, gbd_round_id,
                          'DATABASE', description, new_mvid=True)
        else:
            glb = get_best_models(model_version_type_id=1, cause_ids=[row['cause_id']],
                                  age_range=[row['age_start'], row['age_end']], gbd_round_id=gbd_round_id)['feeders'].tolist()
            for model in glb:
                print model
                description = 'central codem run {}, wave {}, global models'.format(
                    central_run, wave)
                add_tasks(dag, [model], slots, slots / 2, gbd_round_id,
                          'DATABASE', description, new_mvid=True)
            dr = get_best_models(model_version_type_id=2, cause_ids=[row['cause_id']],
                                 age_range=[row['age_start'], row['age_end']], gbd_round_id=gbd_round_id)['feeders'].tolist()
            for model in dr:
                print model
                description = 'central codem run {}, wave {}, datarich models'.format(
                    central_run, wave)
                add_tasks(dag, [model], slots, slots / 2, gbd_round_id,
                          'DATABASE', description, new_mvid=True)

    date = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    name = 'codem_central_run_{}_{}_{}'.format(central_run, wave, date)
    errors = 'FILEPATH'.format(central_run, wave)
    output = 'FILEPATH'.format(central_run, wave)

    if not os.path.exists(errors):
        os.makedirs(errors)
    if not os.path.exists(output):
        os.makedirs(output)

    wf = Workflow(dag, name, project='proj_codem',
                  stderr=errors,
                  stdout=output)
    wf.run()
