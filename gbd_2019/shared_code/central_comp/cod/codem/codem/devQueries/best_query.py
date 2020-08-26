import sqlalchemy as sql
import pandas as pd
import logging

from codem.devQueries.utilities import get_server, age_id_to_name, list_check

logger = logging.getLogger(__name__)


def get_cause_ids(acauses, db_connection='ADDRESS'):
    """
    Gets the cause IDs for acause lists.
    :return: list of acauses (str)
    """
    logger.info("Getting cause IDs.")
    db = get_server(db_connection)
    engine = sql.create_engine(db)

    acauses = list_check(acauses)

    call = '''
        SELECT cause_id FROM shared.cause
        WHERE acause IN ("{}")'''.format('", "'.join([str(x) for x in acauses]))
    df = pd.read_sql_query(call, engine)
    return df['cause_id'].tolist()


def get_best_models(cause_ids=None, age_range=None, sex_id=None, gbd_round_id=5, decomp_step_id=0,
                    db_connection='ADDRESS'):
    """
    Gets the best models and global feeder
    :param cause_ids: list of cause_ids if want to get for certain causes
    :param age_range: list of age_start_id, age_end_id if want to get for certain ages
    :param sex_id: sex ID to call
    :param decomp_step_id: decomposition analysis step
    :param gbd_round_id: gbd round ID (5 default)
    :param db_connection: database
    :return:
    """
    logger.info("Getting best models.")
    db = get_server(db_connection)
    engine = sql.create_engine(db)
    decomp_step_ids = list(range(decomp_step_id + 1))
    call = '''
        SELECT 
            MAX(CAST(mv.model_version_id AS UNSIGNED)) AS model_version_id,
            mv.sex_id,
            mv.cause_id,
            sc.cause_name,
            mv.age_start,
            mv.age_end,
            mv.date_inserted
        FROM
            cod.model_version mv
                INNER JOIN
            shared.cause sc ON mv.cause_id = sc.cause_id
        WHERE
            gbd_round_id = {gbd} AND decomp_step_id IN ({dc})
            '''
    if cause_ids is not None:
        call = call + ' AND mv.cause_id IN ({})'.format(', '.join([str(x) for x in list_check(cause_ids)]))
    if age_range is not None:
        call = call + ' AND mv.age_start = {} AND mv.age_end = {}'.format(int(age_range[0]),
                                                                          int(age_range[1]))
    if sex_id is not None:
        call = call + ' AND mv.sex_id = {}'.format(sex_id)
    call = call + ''' AND ((model_version_type_id = 3
                AND is_best = 1)
                OR (mv.cause_id IN (SELECT 
                    cause_id
                FROM
                    cod.model_version
                WHERE
                    model_version_type_id = 3
                        AND gbd_round_id = {gbd}
                        AND decomp_step_id IN ({dc})
                        AND status = 1)
                AND model_version_type_id = 3)
                AND mv.cause_id NOT IN (SELECT 
                    cause_id
                FROM
                    cod.model_version
                WHERE
                    model_version_type_id = 3
                        AND is_best = 1
                        AND gbd_round_id = {gbd}
                        AND decomp_step_id IN ({dc})
                        AND status = 1)
                    AND status = 1)
        GROUP BY mv.sex_id , mv.cause_id , sc.cause_name , mv.age_start , mv.age_end
        '''
    call = call.format(gbd=gbd_round_id, dc=', '.join([str(x) for x in decomp_step_ids]))
    best = pd.read_sql_query(call, engine)
    return best


def get_feeders(cause_ids=None, age_range=None, sex_id=None, model_version_type_id=[1, 2],
                gbd_round_id=5, decomp_step_id=0, db_connection='ADDRESS'):

    """
    Using the best model versions,
    gets the global feeder for that model version id.
    :return:
    """
    logger.info("Getting feeder models.")
    best = get_best_models(cause_ids, age_range, sex_id, gbd_round_id,
                           decomp_step_id=decomp_step_id, db_connection=db_connection)
    call = '''
        SELECT mvr.child_id as child_id, mv.sex_id, mv.cause_id, sc.cause_name,
            mv.age_start, mv.age_end, mv.model_version_type_id, sc.acause
        FROM cod.model_version_relation mvr
        INNER JOIN cod.model_version mv
        ON mvr.child_id = mv.model_version_id
        INNER JOIN shared.cause sc
        ON mv.cause_id = sc.cause_id
        WHERE mvr.parent_id IN ({parents})
        AND mv.model_version_type_id IN ({types})
        '''.format(parents=', '.join([str(x) for x in best.model_version_id.unique().tolist()]),
                   types=', '.join([str(x) for x in list_check(model_version_type_id)]))

    db = get_server(db_connection)
    engine = sql.create_engine(db)
    feeders = pd.read_sql_query(call, engine)
    
    df = best.merge(feeders, on=['sex_id', 'cause_id', 'cause_name', 'age_start',
                                 'age_end'])
    df = df[['model_version_id', 'child_id', 'sex_id', 'cause_id', 'cause_name', 'acause',
             'age_start', 'age_end', 'model_version_type_id', 'date_inserted']]

    df['age_start_name'] = df.age_start.map(age_id_to_name)
    df['age_end_name'] = df.age_end.map(age_id_to_name)
    
    return df

