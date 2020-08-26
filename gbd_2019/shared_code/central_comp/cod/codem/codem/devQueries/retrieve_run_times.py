from datetime import datetime
import numpy as np
import logging

from codem.reference.db_connect import query
from codem.devQueries.best_query import get_feeders

logger = logging.getLogger(__name__)


def get_model_runtime(gbd_round_id, db_connection='ADDRESS', covariates=False):
    logger.info("Retrieving models for best models in gbd round {}".format(gbd_round_id))
    df = get_feeders(model_version_type_id=1, gbd_round_id=gbd_round_id,
                     db_connection=db_connection)
    df.drop(['model_version_id', 'sex_id'], inplace=True, axis=1)
    df.rename(columns={'child_id': 'model_version_id'}, inplace=True)
    runtime = get_runtime(df, db_connection, covariates)
    runtime.sort_values(['cause_name', 'age_start', 'age_end', 'total_seconds'],
                        inplace=True,
                        ascending=False)
    runtime.drop_duplicates(['cause_id', 'age_start', 'age_end'], inplace=True)
    runtime.sort_values('total_seconds', inplace=True, ascending=False)
    runtime['wave'] = 0
    runtime['global_male'] = np.NaN
    runtime['global_female'] = np.NaN
    runtime['developed_male'] = np.NaN
    runtime['developed_female'] = np.NaN
    return runtime


def days_hours_minutes(td):
    """
    Converts time delta to days, hours minutes.
    """
    dhm = (td.days, td.seconds // 3600, (td.seconds // 60) % 60)
    return '{0}, {1}, {2}'.format(dhm[0], dhm[1], dhm[2])


def get_runtime(df, db_connection='ADDRESS', covariates=False):
    """
    Given a dataframe including model_version_ids, return the same dataframe
    with columns including the runtime associated with each model (both in
    seconds and as days/hours/minutes

    Select covariates=True to just get the runtime of covariate selection
    """
    logger.info("Calculating runtime.")
    call = '''
        SELECT
            min(date_inserted) as start,
            max(date_inserted) as end
        FROM cod.model_version_log
        WHERE model_version_id = {mvid}
    '''
    if covariates:
        call += ''' AND model_version_log_entry IN
        ('Running covariate selection started.',
        'Running KO process started.')'''
    else:
        pass

    df['total_seconds'] = 0
    df['days_hrs_mins'] = ''
    # for each model version id, calculate the runtime and store it in the df
    for index, row in df.iterrows():
        model_version_id = row['model_version_id']
        times = query(call.format(mvid=model_version_id), db_connection)
        start_time = datetime.strptime(str(times.ix[0, 'start']), '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(str(times.ix[0, 'end']), '%Y-%m-%d %H:%M:%S')
        delta = end_time - start_time
        df.set_value(index, 'total_seconds', delta.total_seconds())
        df.set_value(index, 'days_hrs_mins', days_hours_minutes(delta))
    
    return df

