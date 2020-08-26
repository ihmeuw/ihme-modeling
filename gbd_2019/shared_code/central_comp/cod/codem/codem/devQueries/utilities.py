import sqlalchemy as sql
import pandas as pd
import subprocess
import json
from tqdm import tqdm

from codem.reference.db_connect import query

import logging
logger = logging.getLogger(__name__)


def list_check(f):
    return f if type(f) is list else [f]


def get_server(db_name):
    '''
    read the codem credentials from a file on the cluster and combine with the
    mysql database to connect to

    :param db_name: string
        either 'ADDRESS' or 'ADDRESS'

    :return: string
        server with username and password to connect to
    '''

    filepath = 'FILEPATH'
    with open(filepath, 'r') as infile:
        creds = 'USER:' + json.load(infile)['password']
    return 'ADDRESS'


def get_model_status(df, db):
    '''
    given a dataframe including model_version_ids, return the same dataframe
    with a column for status
    '''

    call = '''
        SELECT status
        FROM cod.model_version
        WHERE model_version_id = {mvid}
    '''
    engine = sql.create_engine(db)

    df['status'] = 0
    # for each model version id, calculate the runtime and store it in the df
    for index, row in df.iterrows():
        model_version_id = row['model_version']
        status = pd.read_sql_query(call.format(mvid=model_version_id), engine).ix[0, 'status']
        df.set_value(index, 'status', status)

    return df


def get_best_children(cause_id, sex_id, best, db_name='ADDRESS'):
    '''
    returns the children of the model marked best for a given acause
    and sex_id

    :param cause_id: int
        cause to get models for
    :param sex_id: int
        either 1 (male) or 2 (female)
    '''
    db = get_server(db_name)
    call = '''SELECT child_id
        FROM cod.model_version_relation
        WHERE parent_id IN (
            SELECT max(model_version_id)
            FROM cod.model_version
            WHERE is_best = {best}
            AND gbd_round_id = 4
            AND cause_id = {cid}
            AND sex_id = {sid})
        '''.format(cid=cause_id, sid=sex_id, best = best)
    engine = sql.create_engine(db)
    mvids = pd.read_sql_query(call, engine).child_id.values.tolist()
    return mvids


def age_id_to_name(age_id, age_df=None, db_name='ADDRESS'):
    '''
    returns the age_group_alternative_name corresponding to the input age_id

    :param age_id: int
        the age_group_id to convert
    :param age_df: pandas dataframe
        a dataframe specifying the relationship between age_group_id and
        age_group_alternative_name; if 'None', it will be queried from
        sql
    :param db: string
        server to connect to (ADDRESS or ADDRESS)

    :return: string
        the age_group_alternative_name corresponding to the input age_id
    '''

    # get age_df from shared.cause if it wasn't passed in as an input
    if age_df is None:
        call = '''
            SELECT
                age_group_id, age_group_alternative_name
            FROM
                shared.age_group
            '''
        db = get_server(db_name)
        engine = sql.create_engine(db)
        age_df = pd.read_sql_query(call, engine)
    else:
        pass

    return age_df[age_df.age_group_id == age_id]. \
        age_group_alternative_name.iloc[0]


def sex_id_to_name(sex_id):
    '''
    returns 'Male', 'Female', or 'Both' for sex_id's 1, 2, and 3 respectively

    :param sex_id: int
        either 1, 2, or 3

    :return: string
        'Male', 'Female', or 'Both', depending on the input
    '''

    if sex_id == 1:
        return 'Male'
    elif sex_id == 2:
        return 'Female'
    else:  # sex_id == 3
        return 'Both'


def get_central_run_models(central_run, wave, gbd_round_id, decomp_step_id, db_connection, status=None):
    """
    Gets the models run during the central run, looking by description.
    Used to see if models are still pending or finished/failed.
    :param central_run:
    :param wave:
    :param gbd_round_id:
    :param decomp_step_id: (int)
    :param status: list of int
    :param db_connection: (str)
    :return:
    """
    call = '''
        SELECT model_version_id, status, description
        FROM cod.model_version
        WHERE gbd_round_id = {gbd}
        AND description like "central codem run {run}, wave {wave}%%"
        AND decomp_step_id = {decomp_step_id}
        '''.format(gbd=gbd_round_id,
                   run=central_run,
                   wave=wave,
                   decomp_step_id=decomp_step_id)
    if status is not None:
        call = call + ' AND status IN ({})'.format(', '.join([str(x) for x in status]))
    mvids = query(call, db_connection)
    return mvids


def get_mods_info(models, db_connection):
    """
    gets the cause_id, cause_name, modeler, inserted_by, model_version_type,
    and sex_id for each model in 'models' from the database 'db'

    :param models: list of ints
    :param db_connection: string
    """
    call = '''
        SELECT
            cmv.cause_id,
            sc.cause_name,
            sc.acause,
            cmv.model_version_id,
            cmv.sex_id,
            cmv.model_version_type_id,
            cmv.age_start,
            cmv.age_end,
            cmv.description,
            cmv.status,
            c.model_version_log_entry,
            c.date_inserted
        FROM
            cod.model_version cmv
                INNER JOIN
            (SELECT
                cml.model_version_log_entry,
                    cml.model_version_id,
                    cml.date_inserted
            FROM
                cod.model_version_log cml
            JOIN (SELECT
                model_version_id, MAX(date_inserted) ins
            FROM
                cod.model_version_log
            GROUP BY model_version_id) l ON l.model_version_id = cml.model_version_id
                AND l.ins = cml.date_inserted) c ON c.model_version_id = cmv.model_version_id
                INNER JOIN
            shared.cause sc ON sc.cause_id = cmv.cause_id
            WHERE cmv.model_version_id IN (
            {}
            )
           '''.format(', '.join([str(x) for x in models]))

    df = query(call, db_connection)
    return df


def get_pending_model_status(models, user='USERNAME'):
    """
    determines whether a model is in the queue or running

    :param models: data frame with model_version_id as a column
        model_version_ids to search for
    :param user: string
        if it's a central run, put the user as the central runner (i.e. USERNAME)
        if it's a whole bunch of individual users running models from CodViz, return the database user

    :return: pandas dataframe
        original model_version_id, plus a column 'cluster_status' indicating
        'running', 'queue', or 'dead'
    """
    call = "qstat -u {} | grep cod_{} | awk {{'print $5'}}"
    for index, row in tqdm(models.iterrows()):
        model = row['model_version_id']
        process = subprocess.Popen(call.format(user, model), shell=True,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = process.communicate()
        out = out.decode('utf-8')
        models.set_value(index, 'cluster_status', out)

    return models


def get_central_run_model_status(central_run, wave, user='USERNAME', db_connection='ADDRESS',
                                 gbd_round_id=6, decomp_step_id=2, status=None):
    models = get_central_run_models(central_run=central_run, wave=wave, db_connection=db_connection,
                                    gbd_round_id=gbd_round_id, status=status, decomp_step_id=decomp_step_id)['model_version_id'].tolist()
    if len(models) > 0:
        info = get_mods_info(models, db_connection=db_connection)
        status = get_pending_model_status(models=info, user=user)
    else:
        status = pd.DataFrame()
    return status


def get_failed_models(central_run, waves, user='USERNAME', db_connection='ADDRESS', gbd_round_id=6, decomp_step_id=2):
    dfs = []
    for wave in waves:
        logger.info(f"Getting failed models for central run {central_run} and wave {wave} and gbd round ID {gbd_round_id}")
        models = get_central_run_model_status(central_run=central_run, wave=wave, user=user, db_connection=db_connection,
                                              gbd_round_id=gbd_round_id, status=[0], decomp_step_id=decomp_step_id)
        dfs.append(models)
    all_models = pd.concat(dfs)
    all_models['cluster_status'] = all_models['cluster_status'].apply(lambda x: x.rstrip())
    return all_models.loc[~all_models.cluster_status.isin(['r', 'qw'])]


def get_cause_sex_pairs(db):
    '''
    returns a dataframe of all cause-sex pairs with a completed model
    run in the gbd round specified

    :param db: string
        server to get info from, includes creds
    :return: pandas dataframe
        dataframe with the columns cause_id and sex_id
    '''

    engine = sql.create_engine(db)
    call = '''
        SELECT DISTINCT
            cause_id, sex_id
        FROM
            cod.model_version
        WHERE
            status = 1
        '''
    df = pd.read_sql_query(call, engine)
    return df


def get_best_date(df, db):
    '''
    gets the date where the best model was marked "best" for a dataframe of
    cause-sex pairs

    :param df: pandas dataframe
        dataframe containing a column of cause_id's and a column of sex_id's
    :param db: string
        server to get info from, includes creds

    :return: pandas dataframe
        original df plus a column 'best_start', which indicates when the best
        model for each cause-sex pair was marked best
    '''

    engine = sql.create_engine(db)
    call = '''
        SELECT
            best_start
        FROM
            cod.model_version
        WHERE
            is_best = 1
        AND
            cause_id = {cause}
        AND
            sex_id = {sex}
        '''

    # get the best_start for each cause_sex pair in the df
    for index, row in df.iterrows():
        cause = row['cause_id']
        sex = row['sex_id']
        best = pd.read_sql_query(call.format(cause=cause, sex=sex), engine). \
            best_start.iloc[0]
        df.set_value(index, 'best_start', best)

    return df


def get_most_recent(df, db):
    '''
    gets the data where the most recent model was run for a datwithaframe of
    cause-sex pairs

    :param df: pandas dataframe
        dataframe containing a column of cause_id's and a column of sex_id's
    :param db: string
        server to get info from, includes creds

    :return: pandas dataframe
        original df plus a column 'most_recent', which indicates when most
        recent model was run for each cause_sex pair
    '''

    engine = sql.create_engine(db)
    call = '''
        SELECT
            max(date_inserted) as most_recent
        FROM
            cod.model_version
        WHERE
            cause_id = {cause}
        AND
            sex_id = {sex}
        '''

    # get the most recent model for each cause-sex pair in the df
    for index, row in df.iterrows():
        cause = row['cause_id']
        sex = row['sex_id']
        last = pd.read_sql_query(call.format(cause=cause, sex=sex), engine). \
            most_recent.iloc[0]
        df.set_value(index, 'most_recent', last)

    return df


def get_most_recent_completed(df, db, model_version_type_id):
    '''
    gets the data where the most recent model was completed for a cause-age-sex-model_version_type combination
    :param df:
    :param db:
    :return:
    '''

    engine = sql.create_engine(db)
    call = '''
        SELECT max(model_version_id) as model_version_id
        FROM cod.model_version
        WHERE cause_id = {cause}
        AND sex_id = {sex}
        AND age_start = {age_start}
        AND age_end = {age_end}
        AND model_version_type_id = {mvtid}
        AND status = 1
        AND gbd_round_id = 5
        ORDER BY model_version_id desc
        '''

    df['model_version_id_latest'] = 0
    for index, row in df.iterrows():
        cause = int(row['cause_id'])
        sex = int(row['sex_id'])
        age_start = int(row['age_start'])
        age_end = int(row['age_end'])
        last = pd.read_sql_query(call.format(cause=cause, sex=sex, age_start=age_start,
                                             age_end=age_end, mvtid=model_version_type_id), engine).model_version_id.iloc[0]
        df.set_value(index, 'model_version_id_latest', last)

    return df


def check_if_hybrids_bested(df, db):
    '''
    Checks if there is a hybrid marked best for each of the cause/sex/age pairs in the df and marks with
    marked best or not
    :param df:
    :param db:
    :param model_version_type_id:
    :return:
    '''

    engine = sql.create_engine(db)
    call = '''
        SELECT is_best
        FROM cod.model_version
        WHERE cause_id = {cause}
        AND sex_id = {sex}
        AND age_start = {age_start}
        AND age_end = {age_end}
        AND gbd_round_id = 5
        '''

    df['has_best'] = 0
    for index, row in df.iterrows():
        cause = int(row['cause_id'])
        sex = int(row['sex_id'])
        age_start = int(row['age_start'])
        age_end = int(row['age_end'])
        best = pd.read_sql_query(call.format(cause=cause, sex=sex, age_start=age_start,
                                             age_end=age_end), engine).is_best
        if 1 in best.tolist():
            has_best = 1
        else:
            has_best = 0

        df.set_value(index, 'has_best', has_best)

    return df


def get_modeler(df, db):
    '''
    Gets the modeler and adds as a new column
    :param df:
    :param db:
    :return:
    '''
    engine = sql.create_engine(db)
    call = '''
        SELECT username
        FROM cod.modeler
        WHERE cause_id = {cause}
        AND gbd_round_id = 5
        '''

    df['modeler'] = ''
    for index, row in df.iterrows():
        cause = int(row['cause_id'])
        try:
            modeler = pd.read_sql_query(call.format(cause=cause), engine).username.iloc[0]
        except IndexError:
            modeler = 'unknown'

        df.set_value(index, 'modeler', modeler)

    return df


def kill_models(list_of_models):
    '''
    kills the models provided

    :param list_of_models: list of ints
        models to kill. must be under the username of the person calling this
        function
    '''

    call = 'qdel cod_{}*'
    for model in list_of_models:
        subprocess.call(call.format(model), shell=True)
    return





