import json
import logging
import subprocess

import pandas as pd
import sqlalchemy as sql
from tqdm import tqdm

import gbd.constants as gbd
from db_tools import ezfuncs, query_tools

from codem.reference.db_connect import query

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
        creds = 'USER:' + json.load(infile)['PASSWORD']
    return 'ADDRESS'


def age_id_to_name(age_id, age_df=None, db_name='ADDRESS'):
    '''
    returns the age_group_name_short corresponding to the input age_id

    :param age_id: int
        the age_group_id to convert
    :param age_df: pandas dataframe
        a dataframe specifying the relationship between age_group_id and
        age_group_name_short; if 'None', it will be queried from
        sql
    :param db: string
        server to connect to (ADDRESS or ADDRESS)

    :return: string
        the age_group_name_short corresponding to the input age_id
    '''

    # get age_df from shared.cause if it wasn't passed in as an input
    if age_df is None:
        call = '''
            SELECT
                age_group_id, age_group_name_short
            FROM
                shared.age_group
            '''
        db = get_server(db_name)
        engine = sql.create_engine(db)
        age_df = pd.read_sql_query(call, engine)
    else:
        pass

    return age_df[age_df.age_group_id == age_id]. \
        age_group_name_short.iloc[0]


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


def get_pending_model_status(models, user):
    """
    determines whether a model is in the queue or running

    :param models: data frame with model_version_id as a column
        model_version_ids to search for
    :param user: string
        if it's a central run, put the user as the central runner
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
        models.at[index, 'cluster_status'] = out

    return models


def get_central_run_model_status(central_run, wave, user,
                                 db_connection='ADDRESS',
                                 gbd_round_id=gbd.GBD_ROUND_ID,
                                 decomp_step_id=None,
                                 status=None):
    models = get_central_run_models(
        central_run=central_run, wave=wave, db_connection=db_connection,
        gbd_round_id=gbd_round_id, status=status,
        decomp_step_id=decomp_step_id
    )['model_version_id'].tolist()
    if len(models) > 0:
        info = get_mods_info(models, db_connection=db_connection)
        status = get_pending_model_status(models=info, user=user)
    else:
        status = pd.DataFrame()
    return status


def get_failed_models(central_run, waves, user, db_connection='ADDRESS', gbd_round_id=gbd.GBD_ROUND_ID, decomp_step_id=None):
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
        df.at[index, 'best_start'] = best

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
        df.at[index, 'most_recent'] = last

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
        df.at[index, 'model_version_id_latest'] = last

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

        df.at[index, 'has_best'] = has_best

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


def count_partition(db, table, version_id, conn_def):
    """
    Counts the partitions to see if the partition list contains the versionID specified.

    :db (str): database, e.g. 'cod'
    :table (str): table in database, e.g. 'submodel'
    :version_id (int): versionID that partition is based off of, for example for cod.submodel
                      it is based off of model_version_id
    :conn_def (str): connection definition to use, codem or codem-test

    :return (int): 0 or 1
    """
    call = '''
           SELECT count(*)
           FROM information_schema.partitions
           WHERE table_schema = \'{db}\'
           AND table_name = \'{t}\'
           AND partition_method = \'RANGE\'
           AND cast(partition_description as unsigned int) > {vid}
           AND cast(substr(partition_name, instr(partition_name, \'p\')+1, instr(partition_name, \'_\')-2) as unsigned int) <= {vid}
           AND cast(substr(partition_name, instr(partition_name, \'_\')+1, length(partition_name))       as unsigned int) >= {vid}
           '''.format(db=db, t=table, vid=version_id)

    count = ezfuncs.query(call, conn_def).iloc[0][0]
    return count


def increase_partitions(conn_def, db, table):
    """
    Increases the partitions in a table. This problem has been encountered with the cod.submodel table
    in GBD 2016 and GBD 2017. This has only been used for the cod.submodel table.

    :conn_def (str): connection definition, either codem or codem-test
    :db (str): database, e.g. 'cod'
    :table (str): table in database, e.g. 'submodel'

    :return: None
    """
    get_partition = '''
                    SELECT MAX(CAST(PARTITION_DESCRIPTION AS UNSIGNED)) AS max
                    FROM information_schema.partitions
                    WHERE TABLE_SCHEMA = '{db}'
                    AND TABLE_NAME = '{t}'
                    '''

    add_partition = '''
                    ALTER TABLE {db}.{t} ADD PARTITION (PARTITION {string} VALUES LESS THAN ({less_than}));
                    '''

    maximum = query(get_partition.format(db=db, t=table), conn_def)['max'][0]

    start = maximum
    end = maximum + 1000 - 1

    string = "p{}_{}".format(start, end)
    less_than = end + 1

    call = add_partition.format(db=db, t=table, string=string,
                                less_than=less_than).rsplit('\n')
    call = " ".join(call)
    session = ezfuncs.get_session(conn_def)
    query_tools.exec_query(call, session=session, close=True)

