import os
import sys
from utilities import get_server, age_id_to_name
import sqlalchemy as sql
import pandas as pd


def get_best_models(db, engine, cause_ids=None, age_range=None, gbd_round_id=5):
    '''
    Gets the best models and global feeder
    :param db:
    :param engine:
    :param cause_ids: list of cause_ids if want to get for certain causes
    :param age_range: list of age_start_id, age_end_id if want to get for certain ages
    :param best_start: start date for best models
    :param best_end: end date for best models
    :return:
    '''

    best_call = '''
        SELECT mv.model_version_id, mv.sex_id, mv.cause_id, sc.cause_name,
            mv.age_start, mv.age_end, mv.date_inserted
        FROM cod.model_version mv
        INNER JOIN shared.cause sc
        ON mv.cause_id = sc.cause_id
        WHERE is_best = 1
        AND model_version_type_id = 3
        AND gbd_round_id = {gbd}
        '''

    best_call = best_call.format(gbd=gbd_round_id)

    if cause_ids is not None:
        cause_ids = ', '.join([str(x) for x in cause_ids])
        best_call = best_call + ' AND mv.cause_id IN ({})'.format(cause_ids)
    if age_range is not None:
        best_call = best_call + ' AND mv.age_start = {} AND mv.age_end = {}'.format(int(age_range[0]),
                                                                                    int(age_range[1]))
    print best_call

    best = pd.read_sql_query(best_call, engine)

    return best


def get_feeders(best, db, engine, model_version_type_id):
    '''
    Using the best model versions,
    gets the global feeder for that model version id.
    :param best: df of best models
    :param db:
    :param engine:
    :param model_version_type_id: int
        1: global feeder; 2: data rich feeder
    :return:
    '''
    feeder_call = '''
        SELECT mvr.child_id as feeders, mv.sex_id, mv.cause_id, sc.cause_name,
            mv.age_start, mv.age_end, mv.model_version_type_id
        FROM cod.model_version_relation mvr
        INNER JOIN cod.model_version mv
        ON mvr.child_id = mv.model_version_id
        INNER JOIN shared.cause sc
        ON mv.cause_id = sc.cause_id
        WHERE mvr.parent_id IN ({})
        AND mv.model_version_type_id = {}
        '''
    best_ids = best.model_version_id.unique().tolist()

    feeder_call = feeder_call.format(best_ids, model_version_type_id)
    for symbol in ['[', ']']:
        feeder_call = feeder_call.replace(symbol, '')

    feeders = pd.read_sql_query(feeder_call, engine)

    df = best.merge(feeders, on=['sex_id', 'cause_id', 'cause_name', 'age_start',
                                 'age_end'])
    df = df[['model_version_id', 'feeders', 'sex_id', 'cause_id', 'cause_name',
             'age_start', 'age_end', 'model_version_type_id', 'date_inserted']]

    df.age_start = df.age_start.map(age_id_to_name)
    df.age_end = df.age_end.map(age_id_to_name)

    return df


def main(model_version_type_id, cause_ids, age_range, gbd_round_id):

    db = get_server('DATABASE')
    engine = sql.create_engine(db)
    best_models = get_best_models(
        db, engine, cause_ids, age_range, gbd_round_id)
    best_and_feeders = get_feeders(
        best_models, db, engine, model_version_type_id=model_version_type_id)

    return best_and_feeders
